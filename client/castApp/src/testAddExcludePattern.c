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
    int i;
    caster_t caster;
    casterInit(&caster);
    caster.onmsg = &testLog;

    int argc;
    char *argvlist[3];
    argvlist[0] = "addReccasterExcludePattern";

    char *expectedPatterns[] =
    {
        "*_",
        "*__",
        "*:Intrnl:*"
    };
    int expectedNumPatterns = 0;

    testDiag("Testing addReccasterExcludePattern with one good env");
    argvlist[1] = "*_";
    argc = 2;
    testOk1(caster.num_exclude_patterns==expectedNumPatterns);
    addReccasterExcludePattern(&caster, argc, argvlist);
    expectedNumPatterns++;
    testOk1(caster.num_exclude_patterns==expectedNumPatterns);
    for(i=0; i < expectedNumPatterns; i++) {
        testOk1(strcmp(caster.exclude_patterns[i], expectedPatterns[i]) == 0);
    }

    testDiag("Testing addReccasterExcludePattern with two more patterns");
    argvlist[1] = "*__";
    argvlist[2] = "*:Intrnl:*";
    argc = 3;
    testOk1(caster.num_exclude_patterns==expectedNumPatterns);
    addReccasterExcludePattern(&caster, argc, argvlist);
    expectedNumPatterns += 2;
    testOk1(caster.num_exclude_patterns==expectedNumPatterns);
    for(i=0; i < expectedNumPatterns; i++) {
        testOk1(strcmp(caster.exclude_patterns[i], expectedPatterns[i]) == 0);
    }

    // ^ 8 tests

    testDiag("Testing addReccasterExcludePattern with a duplicate pattern");
    argvlist[1] = "*_";
    argc = 2;
    testOk1(caster.num_exclude_patterns==expectedNumPatterns);
    addReccasterExcludePattern(&caster, argc, argvlist);
    testOk1(caster.num_exclude_patterns==expectedNumPatterns);
    for(i=0; i < expectedNumPatterns; i++) {
        testOk1(strcmp(caster.exclude_patterns[i], expectedPatterns[i]) == 0);
    }

    epicsEventSignal(caster.shutdownEvent);
    casterShutdown(&caster);
}

MAIN(testAddExcludePattern)
{
    testPlan(13);
    osiSockAttach();
    testAddExcludePatternX();
    osiSockRelease();
    return testDone();
}
