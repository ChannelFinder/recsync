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
    char *argvlist[6];
    argvlist[0] = "addReccasterEnvVars";

    char *expectedExtraEnvs[] =
    {
        "SECTOR",
        "BUILDING",
        "CONTACT",
        "DEVICE",
        "FAMILY"
    };
    int expectedNumExtraEnvs = 0;

    testDiag("Testing addReccasterEnvVars with one good env");
    argvlist[1] = "SECTOR";
    argc = 2;
    i = 0;
    testOk1(caster.extra_envs.count==expectedNumExtraEnvs);
    addReccasterEnvVars(&caster, argc, argvlist);
    expectedNumExtraEnvs++;
    testOk1(caster.extra_envs.count==expectedNumExtraEnvs);
    ELLNODE *cur;
    cur = ellFirst(&caster.extra_envs);
    while (cur != NULL) {
        string_list_t *temp = (string_list_t *)cur;
        testOk1(strcmp(temp->item_str, expectedExtraEnvs[i]) == 0);
        i++;
        cur = ellNext(cur);
    }

    testDiag("Testing addReccasterEnvVars with two more good envs");
    argvlist[1] = "BUILDING";
    argvlist[2] = "CONTACT";
    argc = 3;
    i = 0;
    testOk1(caster.extra_envs.count==expectedNumExtraEnvs);
    addReccasterEnvVars(&caster, argc, argvlist);
    expectedNumExtraEnvs += 2;
    testOk1(caster.extra_envs.count==expectedNumExtraEnvs);
    cur = ellFirst(&caster.extra_envs);
    while (cur != NULL) {
        string_list_t *temp = (string_list_t *)cur;
        testOk1(strcmp(temp->item_str, expectedExtraEnvs[i]) == 0);
        i++;
        cur = ellNext(cur);
    }

    testDiag("Testing addReccasterEnvVars with duplicate env");
    argvlist[1] = "SECTOR";
    argc = 2;
    i = 0;
    testOk1(caster.extra_envs.count==expectedNumExtraEnvs);
    addReccasterEnvVars(&caster, argc, argvlist);
    testOk1(caster.extra_envs.count==expectedNumExtraEnvs);
    cur = ellFirst(&caster.extra_envs);
    while (cur != NULL) {
        string_list_t *temp = (string_list_t *)cur;
        testOk1(strcmp(temp->item_str, expectedExtraEnvs[i]) == 0);
        i++;
        cur = ellNext(cur);
    }

    testDiag("Testing addReccasterEnvVars with one dup and one good env");
    argvlist[1] = "CONTACT";
    argvlist[2] = "DEVICE";
    argc = 3;
    i = 0;
    testOk1(caster.extra_envs.count==expectedNumExtraEnvs);
    addReccasterEnvVars(&caster, argc, argvlist);
    expectedNumExtraEnvs++;
    testOk1(caster.extra_envs.count==expectedNumExtraEnvs);
    cur = ellFirst(&caster.extra_envs);
    while (cur != NULL) {
        string_list_t *temp = (string_list_t *)cur;
        testOk1(strcmp(temp->item_str, expectedExtraEnvs[i]) == 0);
        i++;
        cur = ellNext(cur);
    }

    testDiag("Testing addReccasterEnvVars with a good env and a dup of that env");
    argvlist[1] = "FAMILY";
    argvlist[2] = "FAMILY";
    argc = 3;
    i = 0;
    testOk1(caster.extra_envs.count==expectedNumExtraEnvs);
    addReccasterEnvVars(&caster, argc, argvlist);
    expectedNumExtraEnvs++;
    testOk1(caster.extra_envs.count==expectedNumExtraEnvs);
    cur = ellFirst(&caster.extra_envs);
    while (cur != NULL) {
        string_list_t *temp = (string_list_t *)cur;
        testOk1(strcmp(temp->item_str, expectedExtraEnvs[i]) == 0);
        i++;
        cur = ellNext(cur);
    }

    testDiag("Testing addReccasterEnvVars with a env vars from default list");
    argvlist[1] = "EPICS_BASE";
    argvlist[2] = "EPICS_CA_MAX_ARRAY_BYTES";
    argvlist[3] = "PVAS_SERVER_PORT";
    argvlist[4] = "RSRV_SERVER_PORT";
    argvlist[5] = "ENGINEER";
    argc = 6;
    i = 0;
    testOk1(caster.extra_envs.count==expectedNumExtraEnvs);
    addReccasterEnvVars(&caster, argc, argvlist);
    testOk1(caster.extra_envs.count==expectedNumExtraEnvs);
    cur = ellFirst(&caster.extra_envs);
    while (cur != NULL) {
        string_list_t *temp = (string_list_t *)cur;
        testOk1(strcmp(temp->item_str, expectedExtraEnvs[i]) == 0);
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

    int argc;
    char *argvlist[2];
    argvlist[0] = "addReccasterEnvVars";

    testDiag("Testing addReccasterEnvVars with no arguments");
    argc = 1;
    testOk1(caster.extra_envs.count==0);
    addReccasterEnvVars(&caster, argc, argvlist);
    testOk1(caster.extra_envs.count==0);

    testDiag("Testing addReccasterEnvVars with empty string argument");
    argvlist[1] = "";
    argc = 2;
    testOk1(caster.extra_envs.count==0);
    addReccasterEnvVars(&caster, argc, argvlist);
    testOk1(caster.extra_envs.count==0);

    epicsEventSignal(caster.shutdownEvent);
    casterShutdown(&caster);
}

MAIN(testAddEnvVars)
{
    testPlan(37);
    osiSockAttach();
    testAddEnvVarsX();
    testAddEnvVarsBadInput();
    osiSockRelease();
    return testDone();
}
