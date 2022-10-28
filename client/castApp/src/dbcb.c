
#include <string.h>
#include <errlog.h>
#include <ctype.h>
#include <epicsVersion.h>
#include <epicsString.h>
#include <iocsh.h>
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

char **extra_envs = NULL;
int num_extra_envs = 0;

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

    for (i = 0; !ret && extra_envs[i]; i++) {
        const char *val = getenv(extra_envs[i]);
        if (val && val[0] != '\0')
            ret = casterSendInfo(caster, 0, extra_envs[i], val);
        if (ret)
            casterMsg(caster, "Error sending env %s", extra_envs[i]);
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

/*
  Example call: addReccasterEnvVar("SECTOR")
  If this environment variable is set, it will be sent in addition to the envs array
*/
static void addReccasterEnvVar(const char* envList)
{
  if(envList == NULL) {
    errlogSevPrintf(errlogMajor, "envList is NULL for %s\n", __func__);
    return;
  }
  if(envList[0] == '\0') {
    errlogSevPrintf(errlogMinor, "envList is empty for %s\n", __func__);
    return;
  }

  extra_envs = realloc(extra_envs, sizeof(char *) * (++num_extra_envs + 1));
  if (extra_envs == NULL) {
      errlogSevPrintf(errlogMajor, "Error in memory allocation of extra_envs from %s", __func__);
      return;
  }

  char *newvar = (char *)calloc(strlen(envList)+1, sizeof(char));
  strncpy(newvar, envList, sizeof(envList)+1);
  extra_envs[num_extra_envs - 1] = newvar;

  extra_envs[num_extra_envs] = NULL;
}

static const iocshArg initArg0 = { "environmentVar", iocshArgString };
static const iocshArg * const initArgs[] = { &initArg0 };
static const iocshFuncDef initFuncDef = { "addReccasterEnvVar", 1, initArgs };
static void initCallFunc(const iocshArgBuf *args)
{
    addReccasterEnvVar(args[0].sval);
}
void addReccasterEnvVarRegistrar(void)
{
    iocshRegister(&initFuncDef,initCallFunc);
}

#include <epicsExport.h>
epicsExportRegistrar(addReccasterEnvVarRegistrar);
