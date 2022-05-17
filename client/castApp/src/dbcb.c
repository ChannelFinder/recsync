
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

static const char* envs[] =
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

/* String comma separated list from setReccasterEnvironmentVars */
static char * customEnvsDefinition = NULL;

static int pushCustomEnv(caster_t *caster)
{
    size_t i;
    int ret = 0;
    int numEnvs = 0;
    char * saveptr;
    char ** customEnvs = NULL;

    char * token = epicsStrtok_r(customEnvsDefinition, ",", &saveptr);

    while(token) {
      customEnvs = realloc(customEnvs, sizeof (char*) * ++numEnvs);
      if (customEnvs == NULL)
          ERRRET(1, caster, "Error in memory allocation of custom environment vars from setReccasterEnvironmentVars");

      /* skip whitespace before and after environment variable name */
      while (*token && (isspace((int) *token))) ++token;
      char * end = token + strlen(token) - 1;
      while(end > token && isspace((int) *end)) end--;
      end[1] = '\0';

      customEnvs[numEnvs-1] = token;
      token = epicsStrtok_r(NULL, ",", &saveptr);
    }

    /* add NULL as last element */
    customEnvs = realloc (customEnvs, sizeof (char*) * (numEnvs+1));
    if (customEnvs == NULL)
        ERRRET(1, caster, "Error in memory allocation of last NULL element in custom environment vars from setReccasterEnvironmentVars");

    customEnvs[numEnvs] = 0;

    if(customEnvs) {
      for(i=0; !ret && customEnvs[i]; i++) {
          const char *val = getenv(customEnvs[i]);
          if(val && val[0]!='\0')
              ret = casterSendInfo(caster, 0, customEnvs[i], val);
          if(ret)
              casterMsg(caster, "Error sending env %s", customEnvs[i]);
      }
      free(customEnvs);
    }
    return ret;
}

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

    for(i=0; !ret && envs[i]; i++) {
        const char *val = getenv(envs[i]);
        if(val && val[0]!='\0')
            ret = casterSendInfo(caster, 0, envs[i], val);
        if(ret)
            casterMsg(caster, "Error sending env %s", envs[i]);
    }

    if(!ret && customEnvsDefinition) {
        ret = pushCustomEnv(caster);
        free(customEnvsDefinition);
        customEnvsDefinition = NULL;
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
  Example call: setReccasterEnvironmentVars("SECTOR,BUILDING,CONTACT")
  If these environment variables are set, they will be sent in addition to the envs array
*/
static void setReccasterEnvironmentVars(const char* envList)
{
  if(envList == NULL) {
    errlogSevPrintf(errlogMajor, "envList is NULL for %s\n", __func__);
    return;
  }
  if(envList[0] == '\0') {
    errlogSevPrintf(errlogMinor, "envList is empty for %s\n", __func__);
    return;
  }

  const size_t slen = strlen(envList) + 1;

  if(customEnvsDefinition != NULL)
    free(customEnvsDefinition);

  customEnvsDefinition = (char *)calloc(slen, sizeof(char));
  if (customEnvsDefinition == NULL) {
    errlogSevPrintf(errlogMajor, "Error in memory allocation for envList in %s\n", __func__);
    return;
  }

  strncpy(customEnvsDefinition, envList, slen);
}

static const iocshArg initArg0 = { "environmentVars", iocshArgString };
static const iocshArg * const initArgs[] = { &initArg0 };
static const iocshFuncDef initFuncDef = { "setReccasterEnvironmentVars", 1, initArgs };
static void initCallFunc(const iocshArgBuf *args)
{
    setReccasterEnvironmentVars(args[0].sval);
}
void setReccasterEnvironmentVarsRegistrar(void)
{
    iocshRegister(&initFuncDef,initCallFunc);
}

#include <epicsExport.h>
epicsExportRegistrar(setReccasterEnvironmentVarsRegistrar);
