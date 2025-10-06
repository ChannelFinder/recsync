
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

/* Helper function to add items from iocsh calls to internal linked lists
 * funcName is the name of the IOC shell function being called (for error messages)
 * itemDesc is string to describe what is being added (for error messages)
 * defaultArray is an optional arg for a default list to also check for duplicates
 */
static void addToReccasterLinkedList(caster_t* self, int argc, char **argv, ELLLIST* list, const char* funcName, const char* itemDesc, const char** defaultArray)
{
    size_t i, j;
    int dup;
    ELLNODE *cur;
    argv++; argc--; /* skip function arg */
    if(argc < 1) {
        errlogSevPrintf(errlogMinor, "At least one argument expected for %s\n", funcName);
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
           To fully support, would need to force reconnect or resend w/ updated list. */
        errlogSevPrintf(errlogMinor, "%s called after iocInit() when reccaster might already be connected. Not supported\n", funcName);
        epicsMutexUnlock(self->lock);
        return;
    }

    /* sanitize input - check for dups and empty args */
    for (i = 0; i < argc; i++) {
        const size_t arg_len = strlen(argv[i]) + 1;
        if(argv[i][0] == '\0') {
            errlogSevPrintf(errlogMinor, "Arg is empty for %s\n", funcName);
            continue;
        }
        dup = 0;
        /* if the defaultArray arg is used, check if this is a duplicate */
        if (defaultArray) {
            for(j = 0; defaultArray[j]; j++) {
                if(strcmp(argv[i], defaultArray[j]) == 0) {
                    dup = 1;
                    break;
                }
            }
            if(dup) {
                errlogSevPrintf(errlogMinor, "Item %s is already in list sent by reccaster by default\n", argv[i]);
                continue;
            }
        }
        /* check if dup in existing linked list */
        for(cur = ellFirst(list); cur; cur = ellNext(cur)) {
            string_list_t *pitem = CONTAINER(cur, string_list_t, node);
            if (strcmp(argv[i], pitem->item_str) == 0) {
                dup = 1;
                break;
            }
        }
        if(dup) {
            errlogSevPrintf(errlogMinor, "%s %s already in list for %s\n", itemDesc, argv[i], funcName);
            continue;
        }
        string_list_t *new_node = mallocMustSucceed(sizeof(string_list_t) + arg_len, funcName);
        new_node->item_str = (char *)(new_node + 1);
        memcpy(new_node->item_str, argv[i], arg_len);

        ellAdd(list, &new_node->node);
    }
    epicsMutexUnlock(self->lock);
}

/* Example call: addReccasterEnvVars("SECTOR") or addReccasterEnvVars("SECTOR", "BUILDING")
 * Appends the given env variables to the extra_envs list to be sent in addition to the default_envs array
 */
void addReccasterEnvVars(caster_t* self, int argc, char **argv)
{
    addToReccasterLinkedList(self, argc, argv, &self->extra_envs, "addReccasterEnvVars", "Environment variable", (const char**)default_envs);
}

/* Example call: addReccasterExcludePattern("TEST:*") or addReccasterExcludePattern("TEST:*", "*_")
 * Appends the given patterns to the exclude_patterns list so those PVs and their meta-data are not sent
 */
void addReccasterExcludePattern(caster_t* self, int argc, char **argv)
{
    addToReccasterLinkedList(self, argc, argv, &self->exclude_patterns, "addReccasterExcludePattern", "Exclude pattern", NULL);
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

static const iocshArg addReccasterExcludePatternArg0 = { "excludePattern", iocshArgArgv };
static const iocshArg * const addReccasterExcludePatternArgs[] = { &addReccasterExcludePatternArg0 };
static const iocshFuncDef addReccasterExcludePatternFuncDef = {
    "addReccasterExcludePattern",
    1,
    addReccasterExcludePatternArgs,
#ifdef IOCSHFUNCDEF_HAS_USAGE
    "By default, reccaster will send all PVs on IOC startup.\n"
    "This function allows you to exclude PVs by specifying patterns to exclude.\n"
    "Must be called before iocInit\n"
    "Example: addReccasterExcludePattern 'TEST:*' '*_'\n"
#endif
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
