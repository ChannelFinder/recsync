
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
void addReccasterEnvVars(caster_t* self, int argc, char **argv)
{
    size_t i, j;
    int ret = 0;

    argv++; argc--; /* skip function arg */
    if(argc < 1) {
        errlogSevPrintf(errlogMinor, "At least one argument expected for addReccasterEnvVars\n");
        return;
    }

    epicsMutexMustLock(self->lock);
    if(self->shutdown) {
        /* shutdown in progress, silent no-op */
        epicsMutexUnlock(self->lock);
        return;
    }
    else if(self->current != casterStateInit) {
        /* Attempt to add after iocInit(), when we may be connected.
           To fully support, would need to force reconnect or resend w/ updated envs list. */
        errlogSevPrintf(errlogMinor, "addReccasterEnvVars called after iocInit() when reccaster might already be connected. Not supported\n");
        epicsMutexUnlock(self->lock);
        return;
    }
    int new_extra_envs_size = self->num_extra_envs + argc;
    int num_new_extra_envs = self->num_extra_envs;

    char **new_extra_envs = calloc(new_extra_envs_size, sizeof(*new_extra_envs));
    if(new_extra_envs == NULL) {
        errlogSevPrintf(errlogMajor, "Error in memory allocation of new_extra_envs from addReccasterEnvVars\n");
        epicsMutexUnlock(self->lock);
        return;
    }
    /* copy self->extra_envs into new_extra_envs with room for new envs */
    for(i=0; i < self->num_extra_envs; i++) {
        if((new_extra_envs[i] = strdup(self->extra_envs[i])) == NULL) {
            errlogSevPrintf(errlogMinor, "strdup error for copying %s to new_extra_envs[%zu] from addReccasterEnvVars\n", self->extra_envs[i], i);
            ret = 1;
            break;
        }
    }
    int found_dup;
    /* sanitize input - check for dups and empty args */
    if(!ret) {
        for(i=0; i < argc; i++) {
            if(argv[i] == NULL) {
                errlogSevPrintf(errlogMinor, "Arg is NULL for addReccasterEnvVars\n");
                continue;
            }
            else if(argv[i][0] == '\0') {
                errlogSevPrintf(errlogMinor, "Arg is empty for addReccasterEnvVars\n");
                continue;
            }
            found_dup = 0;
            /* check if dup in self->default_envs */
            for(j = 0; default_envs[j]; j++) {
                if(strcmp(argv[i], default_envs[j]) == 0) {
                    found_dup = 1;
                    errlogSevPrintf(errlogMinor, "Env var %s is already in env list sent by reccaster by default\n", argv[i]);
                    break;
                }
            }
            if(found_dup) {
                continue;
            }
            /* check if dup in self->extra_envs */
            for(j = 0; j < num_new_extra_envs; j++) {
                if(new_extra_envs[j] == NULL) {
                    continue;
                }
                if(strcmp(argv[i], new_extra_envs[j]) == 0) {
                    found_dup = 1;
                    errlogSevPrintf(errlogMinor, "Env var %s is already in extra_envs list\n", argv[i]);
                    break;
                }
            }
            if(found_dup) {
                continue;
            }
            if((new_extra_envs[num_new_extra_envs] = strdup(argv[i])) == NULL) {
                errlogSevPrintf(errlogMinor, "strdup error for copying %s to new_extra_envs[%d] from addReccasterEnvVars\n", argv[i], num_new_extra_envs);
                ret = 1;
                break;
            }
            /* this is a valid arg and we have added the new env var to our array, increment new_extra_envs count */
            num_new_extra_envs++;
        }
    }
    /* if we have no allocation issues and have at least one new env var that is valid, add to self->extra_envs */
    if(!ret && num_new_extra_envs > self->num_extra_envs) {
        /* from this point, nothing can fail */
        char ** tmp;
        tmp = self->extra_envs; /* swap pointers so we can clean up new_extra_envs on success/failure */
        self->extra_envs = new_extra_envs;
        new_extra_envs = tmp;

        new_extra_envs_size = self->num_extra_envs; /* with swap of pointers also swap size */
        self->num_extra_envs = num_new_extra_envs;
    }
    /* cleanup new_extra_envs[] on success or failure */
    for(i = 0; i < new_extra_envs_size; i++) {
        free(new_extra_envs[i]);
    }
    free(new_extra_envs);
    epicsMutexUnlock(self->lock);

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
