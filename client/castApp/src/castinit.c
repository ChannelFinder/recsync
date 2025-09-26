
#include <osiSock.h>

#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#include <initHooks.h>
#include <epicsExit.h>
#include <epicsMutex.h>
#include <epicsAssert.h>
#include <cantProceed.h>
#include <dbDefs.h>
#include <iocsh.h>
#include <errlog.h>

#include <epicsStdio.h>
#include <drvSup.h>
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
    osiSockAddr lastserv;
    int intraccept;
    char lastmsg[MAX_STRING_SIZE];
} dpriv;

static dpriv thepriv;

static
void dsetshowmsg(void* arg, struct _caster_t* self)
{
    epicsMutexMustLock(thepriv.lock);
    thepriv.laststate = self->current;
    memcpy(&thepriv.lastserv, &self->nameserv, sizeof(self->nameserv));
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
    int dup;
    ELLNODE *cur;
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
        const size_t arg_len = strlen(argv[i]) + 1;
        if (argv[i][0] == '\0') {
            errlogSevPrintf(errlogMinor, "Arg is empty for addReccasterExcludePattern\n");
            continue;
        }
        /* check duplicates */
        dup = 0;
        for(cur = ellFirst(&self->exclude_patterns); cur; cur = ellNext(cur)) {
            const string_list_t *ppattern = CONTAINER(cur, string_list_t, node);
            if (strcmp(argv[i], ppattern->item_str) == 0) {
                dup = 1;
                break;
            }
        }
        if (dup) {
            errlogSevPrintf(errlogMinor, "Duplicate pattern %s in addReccasterExcludePattern\n", argv[i]);
            continue;
        }
        string_list_t *new_node = mallocMustSucceed(sizeof(string_list_t) + arg_len, "addReccasterExcludePattern");
        new_node->item_str = (char *)(new_node + 1);
        memcpy(new_node->item_str, argv[i], arg_len);

        ellAdd(&self->exclude_patterns, &new_node->node);
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

static long drv_report(int lvl)
{
    casterState laststate;
    const char * lastname = "UNKNOWN";
    char lastmsg[MAX_STRING_SIZE] = "";
    osiSockAddr lastserv;

    epicsMutexMustLock(thepriv.lock);
    switch(laststate = thepriv.laststate) {
#define CASE(NAME) case casterState ## NAME : lastname = #NAME ; break
    CASE(Init);
    CASE(Listen);
    CASE(Connect);
    CASE(Upload);
    CASE(Done);
#undef CASE
    }
    memcpy(lastmsg, thepriv.lastmsg, sizeof(thepriv.lastmsg));
    lastmsg[sizeof(lastmsg)-1] = '\0';
    memcpy(&lastserv, &thepriv.lastserv, sizeof(thepriv.lastserv));
    epicsMutexUnlock(thepriv.lock);

    printf(" State: %s\n", lastname);
    printf(" Msg: %s\n", lastmsg);

    // reuse char buffer
    ipAddrToDottedIP(&lastserv.ia, lastmsg, sizeof(lastmsg));
    lastmsg[sizeof(lastmsg)-1] = '\0';

    switch(laststate) {
    case casterStateConnect:
    case casterStateUpload:
    case casterStateDone:
        printf(" Server: %s\n", lastname);
        break;
    default:
        break;
    }

    return 0;
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

static drvet drvCaster = {
    2,
    (DRVSUPFUN)drv_report,
    NULL,
};

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

epicsExportAddress(drvet, drvCaster);

epicsExportRegistrar(reccasterRegistrar);
