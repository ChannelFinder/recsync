
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#include <initHooks.h>
#include <epicsExit.h>
#include <epicsMutex.h>
#include <epicsAssert.h>
#include <iocsh.h>
#include <errlog.h>

#include <devSup.h>
#include <dbScan.h>
#include <stringinRecord.h>
#include <mbbiRecord.h>

#define epicsExportSharedSymbols

#include "caster.h"

static caster_t thecaster;

typedef struct {
    epicsMutexId lock;
    IOSCANPVT scan;
    casterState laststate;
    int intraccept;
    char lastmsg[MAX_STRING_SIZE];
} dpriv;

static dpriv thepriv;

static
void dsetshowmsg(void* arg, struct _caster_t* self)
{
    epicsMutexMustLock(thepriv.lock);
    thepriv.laststate = self->current;
    strcpy(thepriv.lastmsg, self->lastmsg);
    epicsMutexUnlock(thepriv.lock);
    if(thepriv.intraccept)
        scanIoRequest(thepriv.scan);
}

static void castexit(void *raw)
{
    casterShutdown(&thecaster);
}

static void casthook(initHookState state)
{
    if(state==initHookAfterInterruptAccept)
        thepriv.intraccept = 1;

    if(state!=initHookAfterIocRunning)
        return;

    casterInit(&thecaster);

    thecaster.getrecords = &casterPushPDB;
    thecaster.onmsg = &dsetshowmsg;

    if(casterStart(&thecaster)) {
        printf("reccaster failed to start...\n");
        return;
    }

    epicsAtExit(&castexit, NULL);
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
  printf("envList - %s\n", envList);

  thecaster.extra_envs = realloc(thecaster.extra_envs, sizeof(char *) * (++thecaster.num_extra_envs + 1));
  if (thecaster.extra_envs == NULL) {
      errlogSevPrintf(errlogMajor, "Error in memory allocation of extra_envs from %s", __func__);
      return;
  }

  char *newvar = (char *)calloc(strlen(envList)+1, sizeof(char));
  strncpy(newvar, envList, sizeof(envList)+1);
  thecaster.extra_envs[thecaster.num_extra_envs - 1] = newvar;

  thecaster.extra_envs[thecaster.num_extra_envs] = NULL;
}

static const iocshArg addReccasterEnvVarArg0 = { "environmentVar", iocshArgString };
static const iocshArg * const addReccasterEnvVarArgs[] = { &addReccasterEnvVarArg0 };
static const iocshFuncDef addReccasterEnvVarFuncDef = { "addReccasterEnvVar", 1, addReccasterEnvVarArgs };
static void addReccasterEnvVarCallFunc(const iocshArgBuf *args)
{
    addReccasterEnvVar(args[0].sval);
}

static void reccasterRegistrar(void)
{
    initHookRegister(&casthook);
    thepriv.lock = epicsMutexMustCreate();
    scanIoInit(&thepriv.scan);
    thepriv.laststate=casterStateInit;
    strcpy(thepriv.lastmsg, "Initializing");
    iocshRegister(&addReccasterEnvVarFuncDef,addReccasterEnvVarCallFunc);
}

static long init_record(void* prec)
{
    return 0;
}

static long get_ioint_info(int dir, void *prec, IOSCANPVT *scan)
{
    *scan = thepriv.scan;
    return 0;
}

static long read_mbbi(mbbiRecord *prec)
{
    epicsMutexMustLock(thepriv.lock);
    prec->val = thepriv.laststate;
    prec->udf = 0;
    epicsMutexUnlock(thepriv.lock);
    return 2;
}

static long read_stringin(stringinRecord *prec)
{
    epicsMutexMustLock(thepriv.lock);
    strncpy(prec->val, thepriv.lastmsg, sizeof(prec->val));
    prec->val[sizeof(prec->val)-1] = '\0';
    epicsMutexUnlock(thepriv.lock);
    return 0;
}

typedef struct {
    dset common;
    DEVSUPFUN read;
} dset5;

#define DSETCOMMON {5, NULL, NULL, (DEVSUPFUN)&init_record, (DEVSUPFUN)&get_ioint_info}

static dset5 devCasterMBBIState = {
    DSETCOMMON,
    (DEVSUPFUN)&read_mbbi
};

static dset5 devCasterSIMsg = {
    DSETCOMMON,
    (DEVSUPFUN)&read_stringin
};

#include <epicsExport.h>

epicsExportAddress(double,reccastTimeout);
epicsExportAddress(double,reccastMaxHoldoff);

epicsExportAddress(dset, devCasterMBBIState);
epicsExportAddress(dset, devCasterSIMsg);

epicsExportRegistrar(reccasterRegistrar);
