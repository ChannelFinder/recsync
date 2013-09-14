
#include <errlog.h>

#define epicsExportSharedSymbols

#include "caster.h"

/* Decode server announcement.
 *
 *    | 0 | 1 | 2 | 3 |
 * 00 | ID    | 0 | X |
 * 04 |   SERV ADDR   |
 * 08 | PORT  | X | X |
 * 0C |   SERV KEY    |
 * 10 |   ignore...
 */
static void haveCandidate(caster_t *self,
                          casterAnnounce *buf,
                          osiSockAddr* peer)
{
    if(ntohs(buf->pid) != RECAST_MAGIC)
        return; /* not a recast announcement */

    if(ntohs(buf->version)!=0)
        return; /* reserved for later expansion */

    self->nameserv.ia.sin_family = AF_INET;
    if(ntohl(buf->serverIP)==0xffffffff) {
        /* direct from the source */
        self->nameserv.ia.sin_addr = peer->ia.sin_addr;
    } else {
        /* proxied */
        self->nameserv.ia.sin_addr.s_addr = buf->serverIP;
    }
    self->nameserv.ia.sin_port = buf->serverPort;

    /* ignore TOU16(buf,0xA) */

    self->servkey = ntohl(buf->serverKey);

    self->haveserv = 1;
}

int doCasterUDPPhase(caster_t *self)
{
    shSocket sock; /* UDP listener */
    osiSockAddr me, peer;
    osiSocklen_t peerlen = sizeof(peer);
    int ret = -1;

    self->haveserv = 0;

    shSocketInit(&sock);

    sock.sd = epicsSocketCreate(AF_INET, SOCK_DGRAM, 0);
    if(sock.sd==INVALID_SOCKET) {
        casterMsg(self,"failed to create udp socket.");
        return -1; /* try again */
    }
    sock.wakeup = self->wakeup[1];

    epicsSocketEnableAddressUseForDatagramFanout(sock.sd);

    me.ia.sin_family = AF_INET;
    me.ia.sin_addr.s_addr = htonl(INADDR_ANY);
    me.ia.sin_port = htons(self->udpport);

    if(bind(sock.sd, &me.sa, sizeof(me)))
        ERRTO(done, self,"failed to bind udp socket.");

    if(self->udpport==0) {
        osiSocklen_t slen = sizeof(me);
        if(getsockname(sock.sd, &me.sa, &slen))
            ERRTO(done, self,"reccaster failed to find udp name\n");
        self->udpport = ntohs(me.ia.sin_port);
    }

    if(self->testhook)
        (*self->testhook)(self, casterUDPSetup);

    while(!self->haveserv && !self->shutdown) {
        union casterUDP buf;
        ssize_t ret;

        if(shWaitFor(&sock, SH_CANRX, MSG_NOTIMO)) {
            if(SOCKERRNO==SOCK_ETIMEDOUT)
                continue;
            goto done;
        }

        ret = recvfrom(sock.sd, buf.m_bytes, sizeof(buf.m_bytes), 0,
                       &peer.sa, &peerlen);
        if(ret<0) {
            if(SOCKERRNO==SOCK_EWOULDBLOCK)
                continue;
            casterMsg(self, "recaster UDP recv error %d\n", (int)SOCKERRNO);
            goto done;
        } else if(peerlen<sizeof(peer.ia) || ret<sizeof(buf.m_msg)) {
            goto done;
        }
        haveCandidate(self, &buf.m_msg, &peer);
    }

    ret = 0;
done:
    epicsSocketDestroy(sock.sd);
    return ret;
}
