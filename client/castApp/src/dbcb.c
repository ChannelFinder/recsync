
#include <epicsVersion.h>
#include <epicsString.h>
#include <envDefs.h>

#include <dbStaticLib.h>
#include <dbAccess.h>

#define epicsExportSharedSymbols

#include "caster.h"

static const char* default_envs[] =
{
    /* automatic (if unset) */
    "HOSTNAME",

    /* from envPaths */
    "EPICS_BASE",
    "TOP",
    "ARCH",
    "IOC",

    /* CA related */
    "EPICS_CA_ADDR_LIST",
    "EPICS_CA_AUTO_ADDR_LIST",
    "EPICS_CA_MAX_ARRAY_BYTES",
    "RSRV_SERVER_PORT",

    /* PVA related */
    "PVAS_SERVER_PORT",

    /* Common */
    "PWD",
    "EPICS_HOST_ARCH",
    "IOCNAME",
    "HOSTNAME",

    /* iocStats */
    "ENGINEER",
    "LOCATION",

    NULL
};

static int pushEnv(caster_t *caster)
{
    size_t i;
    int ret = 0;

    if(!getenv("HOSTNAME")) {
        const size_t blen = 256;
        char *buf = calloc(1,blen);
        if(buf && gethostname(buf, blen)==0) {
            buf[blen-1] = '\0'; /* paranoia */
            epicsEnvSet("HOSTNAME", buf);
        }
        free(buf);
    }

    ret = casterSendInfo(caster, 0, "EPICS_VERSION", EPICS_VERSION_STRING);
    if(ret)
        ERRRET(ret, caster, "Failed to send epics version");

    for(i=0; !ret && default_envs[i]; i++) {
        const char *val = getenv(default_envs[i]);
        if(val && val[0]!='\0')
            ret = casterSendInfo(caster, 0, default_envs[i], val);
        if(ret)
            casterMsg(caster, "Error sending env %s", default_envs[i]);
    }

    if(caster->extra_envs) {
        for (i = 0; !ret && caster->extra_envs[i]; i++) {
            const char *val = getenv(caster->extra_envs[i]);
            if (val && val[0] != '\0')
                ret = casterSendInfo(caster, 0, caster->extra_envs[i], val);
            if (ret)
                casterMsg(caster, "Error sending env %s", caster->extra_envs[i]);
        }
    }
    return ret;
}

static int pushRecord(caster_t *caster, DBENTRY *pent)
{
    dbCommon *prec = pent->precnode->precord;
    ssize_t rid;
    int ret = 0;
    long status;

    if(dbIsAlias(pent))
        return 0;

    rid = casterSendRecord(caster, prec->rdes->name, prec->name);
    if(rid<=0)
        return rid;

    if(pent->precnode->flags & DBRN_FLAGS_HASALIAS) {
        DBENTRY subent;

        dbCopyEntryContents(pent, &subent);

        for(status = dbFirstRecord(&subent); !ret && !status;
            status = dbNextRecord(&subent))
        {
            if(dbIsAlias(&subent) &&
               subent.precnode->precord == prec)
            {
                ret = casterSendAlias(caster, rid, subent.precnode->recordname);
            }
        }

        dbFinishEntry(&subent);
    }

    for(status=dbFirstInfo(pent); !ret && !status;
        status=dbNextInfo(pent))
    {
        const char *name = dbGetInfoName(pent),
                   *val  = dbGetInfoString(pent);

        if(val && val[0]!='\0')
            ret = casterSendInfo(caster, rid, name, val);
    }

    /* send desc as INFO tag */
    const char *name = "recordDesc";
    const char *val= prec->desc;
    if(val && val[0]!='\0')
        ret = casterSendInfo(caster, rid, name, val);

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
