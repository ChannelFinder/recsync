#include <dbStaticLib.h>
#include <dbUnitTest.h>
#include <testMain.h>
#include <errlog.h>
#include <dbAccess.h>
#include <string.h>
#include <epicsThread.h>
#include <epicsMutex.h>

#include "caster.h"

static void testAddEnvVars(void)
{
    int i;
    caster_t caster;
    casterInit(&caster);

    int argc;
    char *argvlist[6];
    argvlist[0] = "addReccasterEnvVars";

    char *expectedExtraEnvs[] = 
    { 
        "SECTOR",
        "BUILDING",
        "CONTACT",
        "DEVICE",
        "Field",
        "FAMILY"
    };
    int expectedNumExtraEnvs = 0;

    testDiag("Testing addReccasterEnvVars with one good env");
    argvlist[1] = "SECTOR";
    argc = 2;
    testOk1(caster.num_extra_envs==expectedNumExtraEnvs);
    addReccasterEnvVars(&caster, argc, argvlist);
    expectedNumExtraEnvs++;
    testOk1(caster.num_extra_envs==expectedNumExtraEnvs);
    for(i=0; i < expectedNumExtraEnvs; i++) {
        testOk1(strcmp(caster.extra_envs[i], expectedExtraEnvs[i]) == 0);
    }

    testDiag("Testing addReccasterEnvVars with two more good envs");
    argvlist[1] = "BUILDING";
    argvlist[2] = "CONTACT";
    argc = 3;
    testOk1(caster.num_extra_envs==expectedNumExtraEnvs);
    addReccasterEnvVars(&caster, argc, argvlist);
    expectedNumExtraEnvs += 2;
    testOk1(caster.num_extra_envs==expectedNumExtraEnvs);
    for(i=0; i < expectedNumExtraEnvs; i++) {
        testOk1(strcmp(caster.extra_envs[i], expectedExtraEnvs[i]) == 0);
    }

    testDiag("Testing addReccasterEnvVars with duplicate env");
    argvlist[1] = "SECTOR";
    argc = 2;
    testOk1(caster.num_extra_envs==expectedNumExtraEnvs);
    addReccasterEnvVars(&caster, argc, argvlist);
    testOk1(caster.num_extra_envs==expectedNumExtraEnvs);
    for(i=0; i < expectedNumExtraEnvs; i++) {
        testOk1(strcmp(caster.extra_envs[i], expectedExtraEnvs[i]) == 0);
    }

    testDiag("Testing addReccasterEnvVars with one dup and one good env");
    argvlist[1] = "CONTACT";
    argvlist[2] = "DEVICE";
    argc = 3;
    testOk1(caster.num_extra_envs==expectedNumExtraEnvs);
    addReccasterEnvVars(&caster, argc, argvlist);
    expectedNumExtraEnvs++;
    testOk1(caster.num_extra_envs==expectedNumExtraEnvs);
    for(i=0; i < expectedNumExtraEnvs; i++) {
        testOk1(strcmp(caster.extra_envs[i], expectedExtraEnvs[i]) == 0);
    }

    testDiag("Testing addReccasterEnvVars with NULL argument and then a good env");
    argvlist[1] = NULL;
    argvlist[2] = "Field";
    argc = 3;
    testOk1(caster.num_extra_envs==expectedNumExtraEnvs);
    addReccasterEnvVars(&caster, argc, argvlist);
    expectedNumExtraEnvs++;
    testOk1(caster.num_extra_envs==expectedNumExtraEnvs);
    for(i=0; i < expectedNumExtraEnvs; i++) {
        testOk1(strcmp(caster.extra_envs[i], expectedExtraEnvs[i]) == 0);
    }

    testDiag("Testing addReccasterEnvVars with a good env and a dup of that env");
    argvlist[1] = NULL;
    argvlist[2] = "FAMILY";
    argvlist[3] = "FAMILY";
    argc = 4;
    testOk1(caster.num_extra_envs==expectedNumExtraEnvs);
    addReccasterEnvVars(&caster, argc, argvlist);
    expectedNumExtraEnvs++;
    testOk1(caster.num_extra_envs==expectedNumExtraEnvs);
    for(i=0; i < expectedNumExtraEnvs; i++) {
        testOk1(strcmp(caster.extra_envs[i], expectedExtraEnvs[i]) == 0);
    }

    testDiag("Testing addReccasterEnvVars with a env vars from default list");
    argvlist[1] = "EPICS_BASE";
    argvlist[2] = "EPICS_CA_MAX_ARRAY_BYTES";
    argvlist[3] = "PVAS_SERVER_PORT";
    argvlist[4] = "RSRV_SERVER_PORT";
    argvlist[5] = "ENGINEER";
    argc = 6;
    testOk1(caster.num_extra_envs==expectedNumExtraEnvs);
    addReccasterEnvVars(&caster, argc, argvlist);
    testOk1(caster.num_extra_envs==expectedNumExtraEnvs);
    for(i=0; i < expectedNumExtraEnvs; i++) {
        testOk1(strcmp(caster.extra_envs[i], expectedExtraEnvs[i]) == 0);
    }

    epicsEventId sd;
    sd = caster.shutdownEvent;
    epicsEventSignal(sd);

    casterShutdown(&caster);
}

static void testAddEnvVarsBadInput(void)
{
    caster_t caster;
    casterInit(&caster);

    int argc;
    char *argvlist[2];
    argvlist[0] = "addReccasterEnvVars";

    testDiag("Testing addReccasterEnvVars with no arguments");
    argc = 1;
    testOk1(caster.num_extra_envs==0);
    addReccasterEnvVars(&caster, argc, argvlist);
    testOk1(caster.num_extra_envs==0);

    testDiag("Testing addReccasterEnvVars with empty string argument");
    argvlist[1] = "";
    argc = 2;
    testOk1(caster.num_extra_envs==0);
    addReccasterEnvVars(&caster, argc, argvlist);
    testOk1(caster.num_extra_envs==0);

    testDiag("Testing addReccasterEnvVars with NULL argument");
    argvlist[1] = NULL;
    argc = 2;
    testOk1(caster.num_extra_envs==0);
    addReccasterEnvVars(&caster, argc, argvlist);
    testOk1(caster.num_extra_envs==0);

    epicsEventId sd;
    sd = caster.shutdownEvent;
    epicsEventSignal(sd);

    casterShutdown(&caster);
}

MAIN(testAddEnvVars)
{
    testPlan(48);
    testAddEnvVars();
    testAddEnvVarsBadInput();
    return testDone();
}
