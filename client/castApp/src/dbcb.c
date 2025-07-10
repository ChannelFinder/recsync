
#include <epicsVersion.h>
#include <epicsString.h>
#include <envDefs.h>

#include <dbStaticLib.h>
#include <dbAccess.h>

#define epicsExportSharedSymbols

#include "caster.h"
#include <string.h>

const char* default_envs[] =
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

    /* exclude naming pattern */
    "RECCASTER_EXCLUDE",

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

    epicsMutexMustLock(caster->lock);
    for (i = 0; !ret && i < caster->num_extra_envs; i++) {
        const char *val = getenv(caster->extra_envs[i]);
        if (val && val[0] != '\0')
            ret = casterSendInfo(caster, 0, caster->extra_envs[i], val);
        if (ret)
            casterMsg(caster, "Error sending env %s", caster->extra_envs[i]);
    }
    epicsMutexUnlock(caster->lock);

    return ret;
}

/* TEMPORARY PLACE FOR THESE FUNCTIONS */

int count(char *string, char in) {
    int count = 0;
    for (int i = 0; i < strlen(string); i++) {
        if (string[i] == in) count++;
    }
    return count;
}

char **split(char *pat, int size) {
    /* variable declaration */
    const char *delim = ",";
    // int size = count(pat, *delim) + 1;
    char *pat_cpy;
    char *token;
    char **listOfStrings;

    if(!(listOfStrings = (char**)malloc(size))) {
        perror("malloc");
        exit(EXIT_FAILURE);
    }

    if(!(pat_cpy  = (char*)malloc(strlen(pat) + 1))) {
        perror("malloc");
        exit(EXIT_FAILURE);
    }
    strcpy(pat_cpy, pat);
    
    token = strtok(pat_cpy, delim);
    int i = 0;
    while (token) {
        if(!(listOfStrings[i] = (char*)malloc(strlen(token) + 1))) {
            perror("malloc");
            exit(EXIT_FAILURE);
        }
        strcpy(listOfStrings[i], token);
        i++;
        token = strtok(NULL, delim);
    }
    free(pat_cpy); // null check this too
    return listOfStrings;
}

/* TEMPORARY PLACE FOR THESE FUNCTIONS */

static int pushRecord(caster_t *caster, DBENTRY *pent)
{
    dbCommon *prec = pent->precnode->precord;
    ssize_t rid;
    int ret = 0;
    long status;

    if(dbIsAlias(pent))
        return 0;
    
    char **patterns = split(getenv("RECCASTER_EXCLUDE"), 3);
    for (int i = 0; i < 3; i++) { // this is bad code...
        if(epicsStrGlobMatch(prec->name, patterns[i]))
            return 0;
    }
    
    for(int i = 0; i < 3; i++) {
        free(patterns[i]);
    }
    free(patterns);

    // if(epicsStrGlobMatch(prec->name, getenv("RECCASTER_EXCLUDE"))) {
    //     fprintf(stderr, "%s\n", prec->name);
    //     return 0;
    // }

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
