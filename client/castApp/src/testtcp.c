
#include <string.h>

#include <epicsThread.h>
#include <epicsMutex.h>

#include <epicsUnitTest.h>
#include <testMain.h>

#include "caster.h"
#include "sockhelpers.h"
static int result;
static size_t cycles;
static epicsEventId cycled[2];
static epicsMutexId lock;

static void tester(void *raw)
{
    caster_t *self = raw;
    epicsEventId sd;

    testDiag("TCP tester starts");

    epicsMutexMustLock(lock);

    while(!self->shutdown) {

        epicsMutexUnlock(lock);
        epicsEventMustWait(cycled[1]);
        epicsMutexMustLock(lock);

        result = doCasterTCPPhase(self);
        cycles++;

        testDiag("TCP tester cycle ends %d", result);

        epicsEventSignal(cycled[0]);
    }

    testDiag("TCP tester stops");
    sd = self->shutdownEvent;
    epicsMutexUnlock(lock);

    epicsEventSignal(sd);
}

static int getrecords(void *arg, caster_t *self)
{
    testOk1(arg==self);
    epicsEventSignal(cycled[0]);
    return 0;
}

static void testTCP(void)
{
    caster_t caster;
    SOCKET listener;
    shSocket sock;
    osiSockAddr dest, client;
    osiSocklen_t slen;
    epicsUInt16 msgid;
    union casterTCPBody buf;
    ssize_t ret;

    testDiag("Test TCP client");

    shSocketInit(&sock);

    lock = epicsMutexMustCreate();
    cycled[0] = epicsEventMustCreate(epicsEventEmpty);
    cycled[1] = epicsEventMustCreate(epicsEventEmpty);

    listener = epicsSocketCreate(AF_INET, SOCK_STREAM, 0);
    if(listener==INVALID_SOCKET) {
        testAbort("Failed to create socket");
        return;
    }

    casterInit(&caster);

    memset(&dest, 0, sizeof(dest));
    dest.ia.sin_family = AF_INET;
    dest.ia.sin_addr.s_addr = htonl(INADDR_LOOPBACK);

    if(bind(listener, &dest.sa, sizeof(dest))) {
        testAbort("Failed to bind socket");
        return;
    }

    slen = sizeof(dest);
    if(getsockname(listener, &dest.sa, &slen) || slen<sizeof(dest.ia)) {
        testAbort("Failed to find myself");
        return;
    }


    epicsMutexLock(caster.lock);
    memcpy(&caster.nameserv, &dest, sizeof(dest));
    caster.servkey = 0x12345678;
    caster.haveserv = 1;
    caster.arg = &caster;
    caster.getrecords = &getrecords;
    epicsMutexUnlock(caster.lock);

    if(listen(listener, 2)) {
        testAbort("Squirrel!!");
        return;
    }

    epicsThreadMustCreate("tcptester",
                          epicsThreadPriorityMedium,
                          epicsThreadGetStackSize(epicsThreadStackSmall),
                          &tester, &caster);

    epicsEventSignal(cycled[1]);

    slen = sizeof(client);
    sock.sd = epicsSocketAccept(listener, &client.sa, &slen);
    if(sock.sd==INVALID_SOCKET) {
        testAbort("Client did not connect");
        return;
    }
    testPass("Client connects");

    epicsSocketDestroy(listener);

    testDiag("Wait for client greeting");

    ret = casterRecvPMsg(&sock, &msgid, &buf.bytes, sizeof(buf.bytes), 0);

    testDiag("client greeting %d", (int)ret);
    testOk1(ret==sizeof(buf.c_greet));

    testOk1(msgid==0x0001);
    testOk1(buf.c_greet.version==0);
    testOk1(buf.c_greet.type==0);
    testOk1(buf.c_greet.serverKey=htonl(0x12345678));

    memset(&buf.bytes, 0, sizeof(buf.bytes));

    buf.s_greet.version = 1; /* client should assume protocol will be min(0,1) */

    testDiag("Send server greeting");

    testOk1(casterSendPHead(&sock, 0x8001, sizeof(buf.s_greet))==1);

    testOk1(shSendAll(&sock, &buf.bytes, sizeof(buf.s_greet), 0)==1);

    epicsEventMustWait(cycled[0]);
    testPass("getrecords callback invoked");

    ret = casterRecvPMsg(&sock, &msgid, &buf.bytes, sizeof(buf.bytes), 0);
    testDiag("client done %d", (int)ret);
    testOk1(ret==4);
    testOk1(msgid==0x0005);

    /* ping the client once */
    buf.ping.nonce = htonl(0x10203040);

    testOk1(casterSendPHead(&sock, 0x8002, sizeof(buf.ping))==1);

    testOk1(shSendAll(&sock, &buf.bytes, sizeof(buf.ping), 0)==1);

    memset(&buf, 0, sizeof(buf));

    testOk1(casterRecvPMsg(&sock, &msgid, &buf.bytes, sizeof(buf.bytes), 0)==sizeof(buf.ping));

    testOk1(msgid==0x0002);
    testOk1(buf.ping.nonce==htonl(0x10203040));

    caster.shutdown = 1;

    testDiag("shutdown");
    epicsSocketDestroy(sock.sd);

    epicsEventMustWait(cycled[0]);

    testPass("cycle ends");

    casterShutdown(&caster);

    epicsEventDestroy(cycled[0]);
    epicsEventDestroy(cycled[1]);
    epicsMutexDestroy(lock);

    testPass("done");
}

static void testCB(void)
{
    SOCKET sd[2];
    shSocket sock[2];
    union casterTCPBody buf;
    epicsUInt16 msgid;
    epicsUInt32 blen;
    caster_t caster;
    char cbuf[10];

    testDiag("Test client callback operations");

    if(shSocketPair(sd)) {
        testAbort("Failed to make sockets");
        return;
    }

    caster.csock = &sock[0];
    caster.nextRecID = 42;

    shSocketInit(&sock[0]);
    shSocketInit(&sock[1]);
    sock[0].sd = sd[0];
    sock[1].sd = sd[1];

    testOk1(casterSendRecord(&caster, "hello", "world", "desc")==42);

    testOk1(caster.nextRecID==43);

    testOk1(casterRecvPHead(&sock[1], &msgid, &blen, 0)==1);

    testOk1(msgid==0x0003);
    testOk1(blen==sizeof(buf.c_add)+5+5);

    testOk1(shRecvExact(&sock[1], &buf.bytes, sizeof(buf.c_add), 0)==sizeof(buf.c_add));

    testOk1(buf.c_add.rid==htonl(42));
    testOk1(buf.c_add.rtype==0);
    testOk1(buf.c_add.rtlen==5);
    testOk1(buf.c_add.rnlen==htons(5));

    testOk1(shRecvExact(&sock[1], &cbuf, 10, 0)==10);

    testOk1(memcmp("helloworld", cbuf, 10)==0);

    testOk1(casterSendInfo(&caster, 42, "one", "two")==0);

    testOk1(caster.nextRecID==43);

    testOk1(casterRecvPHead(&sock[1], &msgid, &blen, 0)==1);

    testOk1(msgid==0x0006);
    testOk1(blen==sizeof(buf.c_info)+3+3);

    testOk1(shRecvExact(&sock[1], &buf.bytes, sizeof(buf.c_info), 0)==sizeof(buf.c_info));

    testOk1(buf.c_info.rid==htonl(42));
    testOk1(buf.c_info.klen==3);
    testOk1(buf.c_info.vlen==htons(3));

    testOk1(shRecvExact(&sock[1], &cbuf, 6, 0)==6);

    testOk1(memcmp("onetwo", cbuf, 6)==0);

    epicsSocketDestroy(sd[0]);
    epicsSocketDestroy(sd[1]);
}

MAIN(testtcp)
{
    testPlan(42);
    osiSockAttach();
    testTCP();
    testCB();
    osiSockRelease();
    return testDone();
}
