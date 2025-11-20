#include <string.h>

#include <epicsUnitTest.h>
#include <testMain.h>

#include "caster.h"

void* epicsRtemsFSImage;

static void testLog(void* arg, struct _caster_t* self)
{
    testDiag("ERR %s", self->lastmsg);
}

static void testAddExcludePatternX(void)
{
    int i = 0;
    caster_t caster;
    casterInit(&caster);
    caster.onmsg = &testLog;

    int argc;
    char *argvlist[5];
    argvlist[0] = "addReccasterExcludePattern";

    char *expectedPatterns[] =
    {
        "*_",
        "*__",
        "*:Intrnl:*",
        "*_internal",
        "*exclude_me"
    };
    int expectedNumPatterns = 0;

    testDiag("Testing addReccasterExcludePattern with one good env");
    argvlist[1] = "*_";
    argc = 2;
    testOk1(caster.exclude_patterns.count==expectedNumPatterns);
    addReccasterExcludePattern(&caster, argc, argvlist);
    expectedNumPatterns++;
    testOk1(caster.exclude_patterns.count==expectedNumPatterns);
    ELLNODE *cur;
    cur = ellFirst(&caster.exclude_patterns);
    while (cur != NULL) {
        string_list_t *temp = (string_list_t *)cur;
        testOk1(strcmp(temp->item_str, expectedPatterns[i]) == 0);
        i++;
        cur = ellNext(cur);
    }

    testDiag("Testing addReccasterExcludePattern with two more patterns");
    argvlist[1] = "*__";
    argvlist[2] = "*:Intrnl:*";
    argc = 3;
    i = 0;
    testOk1(caster.exclude_patterns.count==expectedNumPatterns);
    addReccasterExcludePattern(&caster, argc, argvlist);
    expectedNumPatterns += 2;
    testOk1(caster.exclude_patterns.count==expectedNumPatterns);
    cur = ellFirst(&caster.exclude_patterns);
    while (cur != NULL) {
        string_list_t *temp = (string_list_t *)cur;
        testOk1(strcmp(temp->item_str, expectedPatterns[i]) == 0);
        i++;
        cur = ellNext(cur);
    }

    testDiag("Testing addReccasterExcludePattern with a duplicate pattern");
    argvlist[1] = "*_";
    argc = 2;
    i = 0;
    testOk1(caster.exclude_patterns.count==expectedNumPatterns);
    addReccasterExcludePattern(&caster, argc, argvlist);
    testOk1(caster.exclude_patterns.count==expectedNumPatterns);
    cur = ellFirst(&caster.exclude_patterns);
    while (cur != NULL) {
        string_list_t *temp = (string_list_t *)cur;
        testOk1(strcmp(temp->item_str, expectedPatterns[i]) == 0);
        i++;
        cur = ellNext(cur);
    }

    testDiag("Testing addReccasterExcludePattern with a new and a duplicate");
    argvlist[1] = "*_internal";
    argvlist[2] = "*__";
    argc = 3;
    i = 0;
    testOk1(caster.exclude_patterns.count==expectedNumPatterns);
    addReccasterExcludePattern(&caster, argc, argvlist);
    expectedNumPatterns++;
    testOk1(caster.exclude_patterns.count==expectedNumPatterns);
    cur = ellFirst(&caster.exclude_patterns);
    while (cur != NULL) {
        string_list_t *temp = (string_list_t *)cur;
        testOk1(strcmp(temp->item_str, expectedPatterns[i]) == 0);
        i++;
        cur = ellNext(cur);
    }

    testDiag("Testing addReccasterExcludePattern with two of the same pattern");
    argvlist[1] = "*exclude_me";
    argvlist[2] = "*exclude_me";
    argc = 3;
    i = 0;
    testOk1(caster.exclude_patterns.count==expectedNumPatterns);
    addReccasterExcludePattern(&caster, argc, argvlist);
    expectedNumPatterns++;
    testOk1(caster.exclude_patterns.count==expectedNumPatterns);
    cur = ellFirst(&caster.exclude_patterns);
    while (cur != NULL) {
        string_list_t *temp = (string_list_t *)cur;
        testOk1(strcmp(temp->item_str, expectedPatterns[i]) == 0);
        i++;
        cur = ellNext(cur);
    }

    testDiag("Testing addReccasterExcludePattern with duplicates in argv and exclude pattern list");
    argvlist[1] = "*__";
    argvlist[2] = "*__";
    argc = 3;
    i = 0;
    testOk1(caster.exclude_patterns.count==expectedNumPatterns);
    addReccasterExcludePattern(&caster, argc, argvlist);
    testOk1(caster.exclude_patterns.count==expectedNumPatterns);
    cur = ellFirst(&caster.exclude_patterns);
    while (cur != NULL) {
        string_list_t *temp = (string_list_t *)cur;
        testOk1(strcmp(temp->item_str, expectedPatterns[i]) == 0);
        i++;
        cur = ellNext(cur);
    }

    epicsEventSignal(caster.shutdownEvent);
    casterShutdown(&caster);
}

static void testAddExcludePatternBadInput()
{
    caster_t caster;
    casterInit(&caster);
    caster.onmsg = &testLog;

    int argc;
    char *argvlist[2];
    argvlist[0] = "addReccasterExcludePattern";

    testDiag("Testing addReccasterExcludePattern with no arguments");
    argc = 1;
    testOk1(caster.exclude_patterns.count==0);
    addReccasterExcludePattern(&caster, argc, argvlist);
    testOk1(caster.exclude_patterns.count==0);

    testDiag("Testing addReccasterExcludePattern with empty string argument");
    argvlist[1] = "";
    argc = 2;
    testOk1(caster.exclude_patterns.count==0);
    addReccasterExcludePattern(&caster, argc, argvlist);
    testOk1(caster.exclude_patterns.count==0);

    epicsEventSignal(caster.shutdownEvent);
    casterShutdown(&caster);
}

MAIN(testAddExcludePattern)
{
    testPlan(37);
    osiSockAttach();
    testAddExcludePatternX();
    testAddExcludePatternBadInput();
    osiSockRelease();
    return testDone();
}
