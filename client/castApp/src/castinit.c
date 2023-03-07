
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


    thecaster.getrecords = &casterPushPDB;
    thecaster.onmsg = &dsetshowmsg;

    if(casterStart(&thecaster)) {
        printf("reccaster failed to start...\n");
        return;
    }

    epicsAtExit(&castexit, NULL);
}

/*
    Example call: addReccasterEnvVars("SECTOR") or addReccasterEnvVars("SECTOR", "BUILDING")
    Appends the given env variables to the extra_envs list to be sent in addition to the default_envs array
*/
static void addReccasterEnvVars(caster_t* self, int argc, char **argv)
{
    int i, j;
    int ret = 0;
    if(argc < 2) {
        errlogSevPrintf(errlogMinor, "At least one argument expected for addReccasterEnvVars\n");
        return;
    }
    /* sanitize input - check for dups and empty args. First arg in argv is function name so skip that */
    int argCount = argc - 1;
    epicsMutexMustLock(self->lock);
    for(i = 1; i < argc; i++) {
        if(argv[i] == NULL) {
            argCount--;
            errlogSevPrintf(errlogMinor, "Arg is NULL for addReccasterEnvVars\n");
            continue;
        }
        else if(argv[i][0] == '\0') {
            argCount--;
            errlogSevPrintf(errlogMinor, "Arg is empty for addReccasterEnvVars\n");
            argv[i] = NULL;
            continue;
        }
        /* check if dup in self->extra_envs. doesn't check if arg is in default_envs right now */
        for(j = 0; j < self->num_extra_envs; j++) {
            if(strcmp(argv[i], self->extra_envs[j]) == 0) {
              argCount--;
              errlogSevPrintf(errlogMinor, "Env var %s is already in extra_envs list\n", argv[i]);
              argv[i] = NULL;
              break;
            }
        }
    }
    epicsMutexUnlock(self->lock);

    char ** tmp_new_envs;
    tmp_new_envs = calloc(argCount, sizeof(*tmp_new_envs));
    if(tmp_new_envs == NULL) {
        errlogSevPrintf(errlogMajor, "Error in memory allocation of tmp_new_envs from addReccasterEnvVars\n");
        return;
    }
    for(i = 1, j = 0; i < argc; i++) {
        /* check for bad args which are set to NULL above */
        if(!argv[i])
            continue;
        if((tmp_new_envs[j] = strdup(argv[i])) == NULL) {
            ret = 1;
        }
        j++;
    }
    if(!ret) {
        char ** new_envs;
        epicsMutexMustLock(self->lock);
        if(self->shutdown) {
            /* shutdown in progress, silent no-op */
        }
        else if(self->current != casterStateInit) {
            /* Attempt to add after iocInit(), when we may be connected.
               To fully support, would need to force reconnect or resend w/ updated envs list. */
            errlogSevPrintf(errlogMinor, "addReccasterEnvVars called after iocInit() when reccaster might already be connected. Not supported\n");
            ret = 2;
        }
        else if (!(new_envs = realloc(self->extra_envs, sizeof(* new_envs) * (self->num_extra_envs + argCount)))) {
            errlogSevPrintf(errlogMajor, "Error in memory re-allocation of new_envs for self->extra_envs from addReccasterEnvVars\n");
            ret = 1;
        }
        else {
            /* from this point, nothing can fail */
            self->extra_envs = new_envs;
            for(i=0; i<argCount; i++) {
                new_envs[self->num_extra_envs + i] = tmp_new_envs[i];
                tmp_new_envs[i] = NULL; /* prevent early free below */
            }
            self->num_extra_envs += argCount;
        }
        epicsMutexUnlock(self->lock);
    }
    /* cleanup tmp_new_envs[] on success or failure */
    for(i = 0; i < argCount; i++) {
        free(tmp_new_envs[i]);
    }
    free(tmp_new_envs);
    if(ret) {
        errlogSevPrintf(errlogMajor, "Error in addReccasterEnvVars - reccaster might not send the extra env vars specified\n");
    }

}

static const iocshArg addReccasterEnvVarsArg0 = { "environmentVar", iocshArgArgv };
static const iocshArg * const addReccasterEnvVarsArgs[] = { &addReccasterEnvVarsArg0 };
static const iocshFuncDef addReccasterEnvVarsFuncDef = { "addReccasterEnvVars", 1, addReccasterEnvVarsArgs };
static void addReccasterEnvVarsCallFunc(const iocshArgBuf *args)
{
    addReccasterEnvVars(&thecaster, args[0].aval.ac, args[0].aval.av);
}

static void reccasterRegistrar(void)
{
    initHookRegister(&casthook);
    casterInit(&thecaster);
    thepriv.lock = epicsMutexMustCreate();
    scanIoInit(&thepriv.scan);
    thepriv.laststate=casterStateInit;
    strcpy(thepriv.lastmsg, "Initializing");
    iocshRegister(&addReccasterEnvVarsFuncDef,addReccasterEnvVarsCallFunc);
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
