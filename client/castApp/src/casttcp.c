
#include <string.h>

#include <errlog.h>

#define epicsExportSharedSymbols

#include "caster.h"

int doCasterTCPPhase(caster_t *self)
{
    int ret = -1;
    shSocket sock;
    union casterTCPBody buf;
    epicsUInt16 msgid;
    ssize_t blen;

    memset(&buf, 0, sizeof(buf));

    shSocketInit(&sock);
    sock.wakeup = self->wakeup[1];
    shSetTimeout(&sock, self->timeout);

    if(!self->haveserv)
        return -1;

    sock.sd = shCreateSocket(AF_INET, SOCK_STREAM, 0);
    if(!sock.sd)
        ERRRET(-1, self, "Failed to create socket");

    if(shConnect(&sock, &self->nameserv))
        ERRTO(done, self, "caster failed to connect");

    if(self->testhook)
        (*self->testhook)(self, casterTCPSetup);

    /* handshake phase.  Send greeting and wait for same */
    buf.c_greet.serverKey = htonl(self->servkey);

    if(casterSendPHead(&sock, 0x0001, sizeof(buf.c_greet))!=1)
        ERRTO(done, self, "Failed to send greeting header");

    if(shSendAll(&sock, &buf.bytes, sizeof(buf.c_greet), 0)!=1)
        ERRTO(done, self, "Failed to send greeting body");

    blen = casterRecvPMsg(&sock, &msgid, &buf.bytes, sizeof(buf.bytes), 0);
    if(blen<0)
        ERRTO(done, self, "Missing greeting header");

    if(msgid!=0x8001 || blen<sizeof(buf.s_greet))
        ERRTO(done, self, "First message not a greeting %04x %u", msgid, (unsigned)blen);

    self->csock = &sock; /* set callback socket */

    self->current = casterStateUpload;
    casterMsg(self, "Connected");

    /* record upload phase. */
    if((*self->getrecords)(self->arg, self))
        ERRTO(done, self, "Error during record upload");

    buf.ping.nonce = 0;

    if(casterSendPHead(&sock, 0x0005, 4)!=1)
        ERRTO(done, self, "Failed to send all done");

    if(shSendAll(&sock, &buf.bytes, sizeof(buf.ping), 0)!=1)
        ERRTO(done, self, "Failed to send all done body");

    self->current = casterStateDone;
    casterMsg(self, "Synchronized");

    /* longer timeout while we wait for periodic pings */
    shSetTimeout(&sock, self->timeout*4.0);

    while(!self->shutdown) {
        blen = casterRecvPMsg(&sock, &msgid, &buf.bytes, sizeof(buf.bytes), 0);
        if(blen==0)
            break; /* normal end of connection */
        else if(blen<0 && SOCKERRNO==SOCK_ETIMEDOUT)
            ERRTO(done, self, "RecCaster server timeout");
        else if(blen<0)
            ERRTO(done, self, "Missing ping header");

        if(msgid!=0x8002)
            continue;

        if(blen<sizeof(buf.ping))
            ERRTO(done, self, "Not a ping request header");

        if(casterSendPHead(&sock, 0x0002, sizeof(buf.ping))!=1)
            ERRTO(done, self, "Failed to send pong header");

        if(shSendAll(&sock, &buf.bytes, sizeof(buf.ping), 0)!=1)
            ERRTO(done, self, "Failed to send pong body");
    }

    ret = 0;
done:
    self->csock = NULL;
    epicsSocketDestroy(sock.sd);
    return ret;
}
