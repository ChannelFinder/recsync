
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

void* epicsRtemsFSImage;

static void testerhook(caster_t *self, caster_h state)
{
    if(state!=casterUDPSetup)
        return;
    epicsMutexUnlock(lock);

    epicsEventSignal(cycled[0]);

    epicsEventMustWait(cycled[1]);
    epicsMutexMustLock(lock);
}

static void tester(void *raw)
{
    caster_t *self = raw;
    epicsEventId sd;

    testDiag("UDP tester starts");

    epicsMutexMustLock(lock);

    while(!self->shutdown) {

        epicsMutexUnlock(lock);
        epicsEventMustWait(cycled[1]);
        epicsMutexMustLock(lock);

        result = doCasterUDPPhase(self);
        cycles++;

        epicsEventSignal(cycled[0]);
    }

    testDiag("UDP tester stops");
    sd = self->shutdownEvent;
    epicsMutexUnlock(lock);

    epicsEventSignal(sd);
}

static void testUDP(void)
{
    caster_t caster;
    shSocket sender;
    osiSockAddr dest;
    union casterUDP buf;

    shSocketInit(&sender);

    sender.sd = shCreateSocket(AF_INET, SOCK_DGRAM, 0);
    if(sender.sd==INVALID_SOCKET) {
        testAbort("Failed to create socket");
        return;
    }

    lock = epicsMutexMustCreate();
    cycled[0] = epicsEventMustCreate(epicsEventEmpty);
    cycled[1] = epicsEventMustCreate(epicsEventEmpty);

    casterInit(&caster);

    caster.udpport = 0; /* test with random port */
    caster.testhook = &testerhook;

    epicsThreadMustCreate("udptester",
                          epicsThreadPriorityMedium,
                          epicsThreadGetStackSize(epicsThreadStackSmall),
                          &tester, &caster);

    epicsEventSignal(cycled[1]);

    /* wait for tester thread to setup socket */
    epicsEventMustWait(cycled[0]);

    epicsMutexMustLock(lock);

    testOk1(caster.udpport!=0);

    testDiag("UDP test with port %d", caster.udpport);

    memset(&dest, 0, sizeof(dest));
    dest.ia.sin_family = AF_INET;
    dest.ia.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    dest.ia.sin_port = htons(caster.udpport);

    epicsMutexUnlock(lock);

    /* allow tester thread to begin recv() */
    epicsEventSignal(cycled[1]);

    testDiag("Test announcement directly from server");

    memset(&buf, 0, sizeof(buf));
    buf.m_msg.pid = htons(RECAST_MAGIC);
    buf.m_msg.serverIP = htonl(0xffffffff);
    buf.m_msg.serverPort = htons(0x1020);
    buf.m_msg.serverKey = htonl(0x12345678);

    testOk1(0==shSendTo(&sender, &buf.m_bytes, 0x10, 0, &dest));

    /* wait for tester thread to completer recv() and end cycle */
    epicsEventMustWait(cycled[0]);

    epicsMutexMustLock(lock);
    testOk1(cycles==1);
    testOk1(result==0);
    testOk1(caster.haveserv==1);
    testOk1(caster.nameserv.ia.sin_family==AF_INET);
    testOk1(caster.nameserv.ia.sin_addr.s_addr==htonl(INADDR_LOOPBACK));
    testOk1(caster.nameserv.ia.sin_port==htons(0x1020));
    testOk1(caster.servkey==0x12345678);
    epicsMutexUnlock(lock);

    testDiag("Test proxied announcement");

    /* start next cycle */
    epicsEventSignal(cycled[1]);

    /* wait for tester thread to setup socket */
    epicsEventMustWait(cycled[0]);

    epicsMutexMustLock(lock);

    dest.ia.sin_port = htons(caster.udpport);

    epicsMutexUnlock(lock);

    buf.m_msg.serverIP = htonl(0x50607080);

    /* allow tester thread to begin recv() */
    epicsEventSignal(cycled[1]);

    testOk1(0==shSendTo(&sender, &buf.m_bytes, 0x10, 0, &dest));

    /* wait for tester thread to completer recv() and end cycle */
    epicsEventMustWait(cycled[0]);

    epicsMutexMustLock(lock);
    testOk1(cycles==2);
    testOk1(result==0);
    testOk1(caster.haveserv==1);
    testOk1(caster.nameserv.ia.sin_family==AF_INET);
    testOk1(caster.nameserv.ia.sin_addr.s_addr==htonl(0x50607080));
    testOk1(caster.nameserv.ia.sin_port==htons(0x1020));
    epicsMutexUnlock(lock);

    /* begin shutdown cycle */
    epicsEventSignal(cycled[1]);
    epicsEventMustWait(cycled[0]);
    epicsEventSignal(cycled[1]);


    casterShutdown(&caster);

    epicsEventDestroy(cycled[0]);
    epicsEventDestroy(cycled[1]);
    epicsMutexDestroy(lock);
}

MAIN(testudp)
{
    testPlan(16);
    osiSockAttach();;
    testUDP();
    osiSockRelease();
    return testDone();
}
