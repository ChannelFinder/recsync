
#include <epicsVersion.h>
#include <epicsString.h>

#include <dbStaticLib.h>
#include <dbAccess.h>

#define epicsExportSharedSymbols

#include "caster.h"

static const char* envs[] =
{
    /* from envPaths */
    "EPICS_BASE",
    "TOP",
    "ARCH",
    "IOC",

    /* CA related */
    "EPICS_CA_ADDR_LIST",
    "EPICS_CA_AUTO_ADDR_LIST",
    "EPICS_CA_MAX_ARRAY_BYTES",

    /* Common */
    "PWD",
    "EPICS_HOST_ARCH",
    "IOCNAME",

    /* iocStats */
    "ENGINEER",
    "LOCATION",

    NULL
};

static int pushEnv(caster_t *caster)
{
    size_t i;
    int ret = 0;

    ret = casterSendInfo(caster, 0, "EPICS_VERSION", EPICS_VERSION_STRING);
    if(ret)
        ERRRET(ret, caster, "Failed to send epics version");

    for(i=0; !ret && envs[i]; i++) {
        const char *val = getenv(envs[i]);
        if(val && val[0]!='\0')
            ret = casterSendInfo(caster, 0, envs[i], val);
        if(ret)
            casterMsg(caster, "Error sending env %s", envs[i]);
    }

    return ret;
}

static int pushRecord(caster_t *caster, DBENTRY *pent)
{
    dbCommon *prec = pent->precnode->precord;
    ssize_t rid;
    int ret = 0;
    long status;

    rid = casterSendRecord(caster, prec->rdes->name, prec->name);
    if(rid<=0)
        return rid;

    for(status=dbFirstInfo(pent); !ret && !status;
        status=dbNextInfo(pent))
    {
        const char *name = dbGetInfoName(pent),
                   *val  = dbGetInfoString(pent);

        if(val && val[0]!='\0')
            ret = casterSendInfo(caster, rid, name, val);
    }

    return ret;
}

int casterPushPDB(void *junk, caster_t *caster)
{
    DBENTRY ent;
    int ret;
    long rtstat, rstat;

    ret = pushEnv(caster);
    if(ret)
        return ret;

    dbInitEntry(pdbbase, &ent);

    for(rtstat=dbFirstRecordType(&ent); !rtstat;
        rtstat=dbNextRecordType(&ent))
    {
        for(rstat=dbFirstRecord(&ent); !rstat;
            rstat=dbNextRecord(&ent))
        {
            ret = pushRecord(caster, &ent);
            if(ret)
                goto done;
        }
    }

done:
    dbFinishEntry(&ent);
    return ret;
}
