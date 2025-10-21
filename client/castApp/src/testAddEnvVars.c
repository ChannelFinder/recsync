#include <string.h>

#include <epicsUnitTest.h>
#include <testMain.h>

#include "caster.h"

void* epicsRtemsFSImage;

static void testLog(void* arg, struct _caster_t* self)
{
    testDiag("ERR %s", self->lastmsg);
}

static void testAddEnvVarsX(void)
{
    int i = 0;
    caster_t caster;
    casterInit(&caster);
    caster.onmsg = &testLog;

    int argc;
    char *argvlist[7];
    argvlist[0] = "addReccasterEnvVars";

    char *expectedExtraEnvs[] =
    {
        "SECTOR",
        "BUILDING",
        "CONTACT",
        "DEVICE",
        "FAMILY"
    };
    size_t defaultEnvCount = 0;
    while (default_envs[defaultEnvCount]) {
        defaultEnvCount++;
    }
    int expectedNumExtraEnvs = defaultEnvCount;

    testDiag("Testing addReccasterEnvVars with one good env");
    argvlist[1] = "SECTOR";
    argvlist[2] = NULL;
    argc = 2;
    i = 0;
    testOk1(caster.envs.count==expectedNumExtraEnvs);
    addReccasterEnvVars(&caster, argc, argvlist);
    expectedNumExtraEnvs++;
    testOk1(caster.envs.count==expectedNumExtraEnvs);
    ELLNODE *cur;
    cur = ellFirst(&caster.envs);
    while (cur != NULL) {
        string_list_t *temp = (string_list_t *)cur;
        if (i < defaultEnvCount) {
            testOk1(strcmp(temp->item_str, default_envs[i]) == 0);
        }
        else {
            testOk1(strcmp(temp->item_str, expectedExtraEnvs[i - defaultEnvCount]) == 0);
        }
        i++;
        cur = ellNext(cur);
    }

    testDiag("Testing addReccasterEnvVars with two more good envs");
    argvlist[1] = "BUILDING";
    argvlist[2] = "CONTACT";
    argvlist[3] = NULL;
    argc = 3;
    i = 0;
    testOk1(caster.envs.count==expectedNumExtraEnvs);
    addReccasterEnvVars(&caster, argc, argvlist);
    expectedNumExtraEnvs += 2;
    testOk1(caster.envs.count==expectedNumExtraEnvs);
    cur = ellFirst(&caster.envs);
    while (cur != NULL) {
        string_list_t *temp = (string_list_t *)cur;
        if (i < defaultEnvCount) {
            testOk1(strcmp(temp->item_str, default_envs[i]) == 0);
        }
        else {
            testOk1(strcmp(temp->item_str, expectedExtraEnvs[i - defaultEnvCount]) == 0);
        }
        i++;
        cur = ellNext(cur);
    }

    testDiag("Testing addReccasterEnvVars with duplicate env");
    argvlist[1] = "SECTOR";
    argvlist[2] = NULL;
    argc = 2;
    i = 0;
    testOk1(caster.envs.count==expectedNumExtraEnvs);
    addReccasterEnvVars(&caster, argc, argvlist);
    testOk1(caster.envs.count==expectedNumExtraEnvs);
    cur = ellFirst(&caster.envs);
    while (cur != NULL) {
        string_list_t *temp = (string_list_t *)cur;
        if (i < defaultEnvCount) {
            testOk1(strcmp(temp->item_str, default_envs[i]) == 0);
        }
        else {
            testOk1(strcmp(temp->item_str, expectedExtraEnvs[i - defaultEnvCount]) == 0);
        }
        i++;
        cur = ellNext(cur);
    }

    testDiag("Testing addReccasterEnvVars with one dup and one good env");
    argvlist[1] = "CONTACT";
    argvlist[2] = "DEVICE";
    argvlist[3] = NULL;
    argc = 3;
    i = 0;
    testOk1(caster.envs.count==expectedNumExtraEnvs);
    addReccasterEnvVars(&caster, argc, argvlist);
    expectedNumExtraEnvs++;
    testOk1(caster.envs.count==expectedNumExtraEnvs);
    cur = ellFirst(&caster.envs);
    while (cur != NULL) {
        string_list_t *temp = (string_list_t *)cur;
        if (i < defaultEnvCount) {
            testOk1(strcmp(temp->item_str, default_envs[i]) == 0);
        }
        else {
            testOk1(strcmp(temp->item_str, expectedExtraEnvs[i - defaultEnvCount]) == 0);
        }
        i++;
        cur = ellNext(cur);
    }

    testDiag("Testing addReccasterEnvVars with a good env and a dup of that env");
    argvlist[1] = "FAMILY";
    argvlist[2] = "FAMILY";
    argvlist[3] = NULL;
    argc = 3;
    i = 0;
    testOk1(caster.envs.count==expectedNumExtraEnvs);
    addReccasterEnvVars(&caster, argc, argvlist);
    expectedNumExtraEnvs++;
    testOk1(caster.envs.count==expectedNumExtraEnvs);
    cur = ellFirst(&caster.envs);
    while (cur != NULL) {
        string_list_t *temp = (string_list_t *)cur;
        if (i < defaultEnvCount) {
            testOk1(strcmp(temp->item_str, default_envs[i]) == 0);
        }
        else {
            testOk1(strcmp(temp->item_str, expectedExtraEnvs[i - defaultEnvCount]) == 0);
        }
        i++;
        cur = ellNext(cur);
    }

    testDiag("Testing addReccasterEnvVars with a env vars from default list");
    argvlist[1] = "EPICS_BASE";
    argvlist[2] = "EPICS_CA_MAX_ARRAY_BYTES";
    argvlist[3] = "PVAS_SERVER_PORT";
    argvlist[4] = "RSRV_SERVER_PORT";
    argvlist[5] = "ENGINEER";
    argvlist[6] = NULL;
    argc = 6;
    i = 0;
    testOk1(caster.envs.count==expectedNumExtraEnvs);
    addReccasterEnvVars(&caster, argc, argvlist);
    testOk1(caster.envs.count==expectedNumExtraEnvs); /* these are all defaults so the count should not change */
    cur = ellFirst(&caster.envs);
    while (cur != NULL) {
        string_list_t *temp = (string_list_t *)cur;
        if (i < defaultEnvCount) {
            testOk1(strcmp(temp->item_str, default_envs[i]) == 0);
        }
        else {
            testOk1(strcmp(temp->item_str, expectedExtraEnvs[i - defaultEnvCount]) == 0);
        }
        i++;
        cur = ellNext(cur);
    }

    epicsEventSignal(caster.shutdownEvent);
    casterShutdown(&caster);
}

static void testAddEnvVarsBadInput(void)
{
    caster_t caster;
    casterInit(&caster);
    caster.onmsg = &testLog;

    size_t defaultEnvCount = 0;
    while (default_envs[defaultEnvCount]) {
        defaultEnvCount++;
    }

    int argc;
    char *argvlist[3];
    argvlist[0] = "addReccasterEnvVars";
    argvlist[1] = NULL;

    testDiag("Testing addReccasterEnvVars with no arguments");
    argc = 1;
    testOk1(caster.envs.count==defaultEnvCount);
    addReccasterEnvVars(&caster, argc, argvlist);
    testOk1(caster.envs.count==defaultEnvCount);

    testDiag("Testing addReccasterEnvVars with empty string argument");
    argvlist[1] = "";
    argvlist[2] = NULL;
    argc = 2;
    testOk1(caster.envs.count==defaultEnvCount);
    addReccasterEnvVars(&caster, argc, argvlist);
    testOk1(caster.envs.count==defaultEnvCount);

    epicsEventSignal(caster.shutdownEvent);
    casterShutdown(&caster);
}

MAIN(testAddEnvVars)
{
    /* 90 is defaultEnvCount * 6 for each test loop to check default env list */
    testPlan(37 + 90);
    osiSockAttach();
    testAddEnvVarsX();
    testAddEnvVarsBadInput();
    osiSockRelease();
    return testDone();
}
