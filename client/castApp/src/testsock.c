
#include <string.h>

#include <epicsUnitTest.h>
#include <testMain.h>

#include "sockhelpers.h"

void* epicsRtemsFSImage;

static void testUDP(void)
{
    shSocket sock[2];
    osiSockAddr addr0, addr1;
    osiSocklen_t alen;
    static const char testmsg[] = "Hello world";
    char buf[20];

    shSocketInit(&sock[0]);
    shSocketInit(&sock[1]);
    shSetTimeout(&sock[0], 0.1);
    shSetTimeout(&sock[1], 0.1);

    sock[0].sd = shCreateSocket(AF_INET, SOCK_DGRAM, 0);
    sock[1].sd = shCreateSocket(AF_INET, SOCK_DGRAM, 0);

    testOk1(sock[0].sd!=INVALID_SOCKET);
    testOk1(sock[1].sd!=INVALID_SOCKET);

    memset(&addr0, 0, sizeof(addr0));

    /* bind to loopback on a random port */
    addr0.ia.sin_family = AF_INET;
    addr0.ia.sin_addr.s_addr = htonl(INADDR_LOOPBACK);

    testOk1(0==bind(sock[0].sd, &addr0.sa, sizeof(addr0)));

    alen = sizeof(addr0);
    testOk1(0==getsockname(sock[0].sd, &addr0.sa, &alen));
    testOk1(alen>=sizeof(addr0.ia));

    testDiag("sd[0] bound to %d", ntohs(addr0.ia.sin_port));

    testDiag("Recv timeout");

    testOk1(-1==shWaitFor(&sock[0], SH_CANRX, 0));
    testOk1(SOCKERRNO==SOCK_ETIMEDOUT);
    SOCKERRNOSET(0);

    testOk1(-1==shRecvIgnore(&sock[0], 4, 0));
    testOk1(SOCKERRNO==SOCK_ETIMEDOUT);

    testDiag("Test send");

    testOk1(0==shWaitFor(&sock[1], SH_CANTX, 0));

    testOk1(0==shSendTo(&sock[1], testmsg, sizeof(testmsg), 0, &addr0));
    testDiag("error: %s", strerror(SOCKERRNO));

    testOk1(0==shWaitFor(&sock[0], SH_CANRX, 0));

    testOk1(sizeof(testmsg)==shRecvFrom(&sock[0], buf, sizeof(buf), 0, &addr1));

    testOk1(memcmp(buf, testmsg, sizeof(testmsg))==0);

    epicsSocketDestroy(sock[0].sd);
    epicsSocketDestroy(sock[1].sd);
}

static void testWakeup(void)
{
    shSocket sock;
    SOCKET wakeup[2];
    epicsUInt32 junk = 0;
    int ret;

    shSocketInit(&sock);

    sock.sd = shCreateSocket(AF_INET, SOCK_DGRAM, 0);
    testOk1(sock.sd!=INVALID_SOCKET);

    ret=socketpair_compat(AF_INET, SOCK_STREAM, 0, wakeup);
    testOk(ret==0, "socketpair_compat() -> %d == 0 (%d)", ret, SOCKERRNO);

    sock.wakeup = wakeup[1];

    shSetTimeout(&sock, 100.0); /* something noticable */

    testOk1(sizeof(junk)==send(wakeup[0], (char*)&junk, sizeof(junk), 0));

    SOCKERRNOSET(0);
    ret = shWaitFor(&sock, SH_CANTX, 0);
    testOk(ret==-1 && SOCKERRNO==SOCK_ETIMEDOUT,
           "shWaitFor(&sock, SH_CANTX, 0)==%d (%d) ==-1 (SOCK_ETIMEDOUT)", ret, (int)SOCKERRNO);

    epicsSocketDestroy(sock.sd);
    epicsSocketDestroy(wakeup[0]);
    epicsSocketDestroy(wakeup[1]);
}

MAIN(testsock)
{
    testPlan(18);
    osiSockAttach();;
    testUDP();
    testWakeup();
    osiSockRelease();
    return testDone();
}
