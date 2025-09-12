
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

    int found_dup;
    /* sanitize input - check for dups and empty args */
    for(i=0; i < argc; i++) {
        if(argv[i][0] == '\0') {
            errlogSevPrintf(errlogMinor, "Arg is empty for addReccasterEnvVars\n");
            continue;
        }
        found_dup = 0;
        /* check if dup in self->default_envs */
        for(j = 0; default_envs[j]; j++) {
            if(strcmp(argv[i], default_envs[j]) == 0) {
                found_dup = 1;
                break;
            }
        }
        if(found_dup) {
            errlogSevPrintf(errlogMinor, "Env var %s is already in env list sent by reccaster by default\n", argv[i]);
            continue;
        }
        /* check if dup in self->extra_envs */
        ELLNODE *cur = ellFirst(&self->extra_envs);
        while (cur != NULL) {
            string_list_t *temp = (string_list_t *)cur;
            if (strcmp(argv[i], temp->item_str) == 0) {
                found_dup = 1;
                break;
            }
            cur = ellNext(cur);
        }
        if(found_dup) {
            errlogSevPrintf(errlogMinor, "Env var %s is already in extra_envs list\n", argv[i]);
            continue;
        }
        string_list_t *new_list = malloc(sizeof(string_list_t));
        if (new_list == NULL) {
            errlogSevPrintf(errlogMajor, "Error in addReccasterEnvVars - malloc error for creating linked list node");
            break;
        }
        new_list->item_str = strdup(argv[i]);
        if (new_list->item_str == NULL) {
            errlogSevPrintf(errlogMajor, "Error in addReccasterEnvVars - strdup error for copying %s to new->item_str from addReccasterEnvVars\n", argv[i]);
            free(new_list);  /* frees if strdup fails */
            break;
        }
        ellAdd(&self->extra_envs, &new_list->node);
    }
    epicsMutexUnlock(self->lock);
}

static const iocshArg addReccasterEnvVarsArg0 = { "environmentVar", iocshArgArgv };
static const iocshArg * const addReccasterEnvVarsArgs[] = { &addReccasterEnvVarsArg0 };
static const iocshFuncDef addReccasterEnvVarsFuncDef = {
    "addReccasterEnvVars",
    1,
    addReccasterEnvVarsArgs,
#ifdef IOCSHFUNCDEF_HAS_USAGE
    "Reccaster has a default list of environment variables it sends on IOC startup.\n"
    "This function will append extra variables to that default list.\n"
    "Must be called before iocInit\n"
    "Example: addReccasterEnvVars 'SECTOR' 'BUILDING'\n"
#endif
};
static void addReccasterEnvVarsCallFunc(const iocshArgBuf *args)
{
    addReccasterEnvVars(&thecaster, args[0].aval.ac, args[0].aval.av);
}

void addReccasterExcludePattern(caster_t* self, int argc, char **argv) {
    size_t i;
    argv++; argc--; /* skip function arg */
    if (argc < 1) {
        errlogSevPrintf(errlogMinor, "At least one argument expected for addReccasterExcludePattern\n");
        return;
    }
    epicsMutexMustLock(self->lock);
    if (self->shutdown) {
        /* shutdown in progress, silent no-op */
        epicsMutexUnlock(self->lock);
        return;
    }
    /* error if called after iocInit() */
    if (self->current != casterStateInit) {
        errlogSevPrintf(errlogMinor, "addReccasterExcludePattern called after iocInit() when reccaster might already be connected. Not supported\n");
        epicsMutexUnlock(self->lock);
        return;
    }

    for (i = 0; i < argc; i++) {
        if (argv[i][0] == '\0') {
            errlogSevPrintf(errlogMinor, "Arg is empty for addReccasterExcludePattern\n");
            continue;
        }
        /* check duplicates */
        int dup = 0;
        ELLNODE *cur = ellFirst(&self->exclude_patterns);
        while (cur != NULL) {
            string_list_t *temp = (string_list_t *)cur;
            if (strcmp(argv[i], temp->item_str) == 0) {
                dup = 1;
                break;
            }
            cur = ellNext(cur);
        }
        if (dup) {
            errlogSevPrintf(errlogMinor, "Duplicate pattern %s in addReccasterExcludePattern\n", argv[i]);
            continue;
        }
        string_list_t *new_list = malloc(sizeof(string_list_t));
        if (new_list == NULL) {
            errlogSevPrintf(errlogMajor, "Error in addReccasterExcludePattern - malloc error for creating linked list node");
            break;
        }
        new_list->item_str = strdup(argv[i]);
        if (new_list->item_str == NULL) {
            errlogSevPrintf(errlogMajor, "Error in addReccasterExcludePattern - strdup error for copying %s to new->item_str from addReccasterExcludePattern\n", argv[i]);
            free(new_list);  /* frees if strdup fails */
            break;
        }
        ellAdd(&self->exclude_patterns, &new_list->node);
    }
    epicsMutexUnlock(self->lock);
}

static const iocshArg addReccasterExcludePatternArg0 = { "excludePattern", iocshArgArgv };
static const iocshArg * const addReccasterExcludePatternArgs[] = { &addReccasterExcludePatternArg0 };
static const iocshFuncDef addReccasterExcludePatternFuncDef = {
    "addReccasterExcludePattern",
    1,
    addReccasterExcludePatternArgs
};

static void addReccasterExcludePatternCallFunc(const iocshArgBuf *args) {
    addReccasterExcludePattern(&thecaster, args[0].aval.ac, args[0].aval.av);
}

static void reccasterRegistrar(void)
{
    osiSockAttach();
    initHookRegister(&casthook);
    casterInit(&thecaster);
    thepriv.lock = epicsMutexMustCreate();
    scanIoInit(&thepriv.scan);
    thepriv.laststate=casterStateInit;
    strcpy(thepriv.lastmsg, "Initializing");
    iocshRegister(&addReccasterEnvVarsFuncDef,addReccasterEnvVarsCallFunc);
    iocshRegister(&addReccasterExcludePatternFuncDef,addReccasterExcludePatternCallFunc);
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
