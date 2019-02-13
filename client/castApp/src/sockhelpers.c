#include <string.h>
#include <stdio.h>

#include <epicsTypes.h>

#define epicsExportSharedSymbols

#include "sockhelpers.h"

void shSocketInit(shSocket *s)
{
    s->sd = s->wakeup = INVALID_SOCKET;
    s->timeout.tv_sec = s->timeout.tv_usec = 0;
}

void shSetTimeout(shSocket *s, double val)
{
    struct timeval tv = {0,0};

    if(val<0.0 || val>=0x7fffffff)
        return; /* ignore invalid */

    else if(val>0.0) {
        tv.tv_sec = (epicsUInt32)val;
        tv.tv_usec = (epicsUInt32)(1e6*(val-(double)tv.tv_sec));
    }

    s->timeout = tv;
}

SOCKET shCreateSocket(int domain, int type, int protocol)
{
    SOCKET sd = epicsSocketCreate(domain, type, protocol);
    int ret;
    osiSockIoctl_t flag;

    if(sd==INVALID_SOCKET)
        return sd;

    /* set non-blocking IO */
    flag = 1;
    ret = socket_ioctl(sd, FIONBIO, &flag);

    if(ret) {
        epicsSocketDestroy(sd);
        sd = INVALID_SOCKET;
    }

    return sd;
}

int socketpair_compat(int af, int st, int p, SOCKET sd[2])
{
    SOCKET listener;
    int ret = -1;
    osiSockAddr ep[2];
    osiSocklen_t slen = sizeof(ep[0]);

    if(st!=SOCK_STREAM) {
        SOCKERRNOSET(SOCK_EINVAL);
        return -1;
    }

    listener = epicsSocketCreate(AF_INET, SOCK_STREAM, 0);
    sd[0] = INVALID_SOCKET;
    sd[1] = shCreateSocket(AF_INET, SOCK_STREAM, 0);

    if(listener==INVALID_SOCKET || sd[1]==INVALID_SOCKET) {
        SOCKERRNOSET(SOCK_EMFILE);
        goto fail;
    }

    memset(ep, 0, sizeof(ep));
    ep[0].ia.sin_family = AF_INET;
    ep[0].ia.sin_addr.s_addr = htonl(INADDR_LOOPBACK);

    if(bind(listener, &ep[0].sa, sizeof(ep[0])))
        goto fail;

    if(getsockname(listener, &ep[0].sa, &slen))
        goto fail;

    if(listen(listener, 2))
        goto fail;

    /* we can't possibly succeed immediately */
    if(connect(sd[1], &ep[0].sa, sizeof(ep[0]))!=-1)
        goto fail;

    if(SOCKERRNO!=SOCK_EINPROGRESS)
        goto fail;

    while(1) {
        int err;
        shSocket atemp;
        SOCKET temp;
        osiSocklen_t olen = sizeof(err);

        slen = sizeof(ep[1]);
        temp = epicsSocketAccept(listener, &ep[1].sa, &slen);

        if(temp==INVALID_SOCKET) {
            if(SOCKERRNO==SOCK_EINTR)
                continue;
            goto fail;
        }

        shSocketInit(&atemp);
        atemp.sd = sd[1];

        if(shWaitFor(&atemp, SH_CANTX, 0)) {
            /* someone raced us and won... */
            epicsSocketDestroy(temp);
            continue;
        }

        if(getsockopt(sd[1], SOL_SOCKET, SO_ERROR, (char*)&err, &olen))
            goto fail;

        if(err) {
            SOCKERRNOSET(err);
            goto fail;
        }

        sd[0] = temp;
        break;
    }

    {
        /* restore blocking IO */
        osiSockIoctl_t flag = 0;
        if(socket_ioctl(sd[1], FIONBIO, &flag))
            goto fail;
    }

    epicsSocketDestroy(listener);

    return 0;
fail:
    if(listener!=INVALID_SOCKET)
        epicsSocketDestroy(sd[0]);
    if(listener!=INVALID_SOCKET)
        epicsSocketDestroy(sd[0]);
    if(sd[1]!=INVALID_SOCKET)
        epicsSocketDestroy(sd[1]);
    return ret;
}

int shSocketPair(SOCKET sd[2])
{
#if __unix__
    return socketpair(AF_LOCAL, SOCK_STREAM, 0, sd);
#else
    return socketpair_compat(AF_INET, SOCK_STREAM, 0, sd);
#endif
}

int shWaitFor(shSocket *s, int op, int flags)
{
    struct timeval timo = s->timeout, *ptimo=NULL;
    int ret, maxid = s->sd+1;
    fd_set wset, rset;

    if(!(flags&MSG_NOTIMO) && (timo.tv_sec || timo.tv_usec) )
        ptimo = &timo;

    FD_ZERO(&wset);
    FD_ZERO(&rset);
    if(s->wakeup!=INVALID_SOCKET)
        FD_SET(s->wakeup, &rset);

    if(s->wakeup>s->sd)
        maxid = s->wakeup+1;

    switch(op) {
    case 0: break;
    case SH_CANTX: FD_SET(s->sd, &wset); break;
    case SH_CANRX: FD_SET(s->sd, &rset); break;
    default:
        SOCKERRNOSET(SOCK_EINVAL);
        return -1;
    }

    do {
        ret = select(maxid, &rset, &wset, NULL, ptimo);
    } while(ret==-1 && SOCKERRNO==SOCK_EINTR);

    if(ret<0) {
            return ret;
    } else if(ret==0 || (s->wakeup!=INVALID_SOCKET && FD_ISSET(s->wakeup, &rset))) {
        SOCKERRNOSET(SOCK_ETIMEDOUT);
        return -1;
    } else { /* ret>0 && !FD_ISSET(wakeup, &rset) */
        return 0; /* socket ready */
    }
}

int shConnect(shSocket *s, const osiSockAddr *peer)
{
    int ret;

    do {
        ret = connect(s->sd, &peer->sa, sizeof(peer->sa));
    } while(ret==-1 && SOCKERRNO==SOCK_EINTR);

    if(ret<0 && SOCKERRNO==SOCK_EINPROGRESS) {

        ret = shWaitFor(s, SH_CANTX, 0);

        if(ret == 0) { /* operation complete */
            int err;
            osiSocklen_t elen = sizeof(err);
            ret = getsockopt(s->sd, SOL_SOCKET, SO_ERROR, (char*)&err, &elen);
            if(ret==0 && err) {
                SOCKERRNOSET(err);
                ret = -1;
            }

        }
    }

    return ret;
}

ssize_t shRecvExact(shSocket *s, void *buf, size_t len, int flags)
{
    char *cbuf=buf;
    ssize_t ret;
    size_t sofar = 0;

    while(sofar<len) {
        if(shWaitFor(s, SH_CANRX, flags))
            return -1;

        ret = recv(s->sd, cbuf+sofar, len-sofar, 0);
        if(ret<0 && SOCKERRNO==SOCK_EINTR)
            continue;
        else if(ret<=0)
            return ret;

        sofar += ret;
    }
    return sofar;
}

ssize_t shRecvIgnore(shSocket *s, size_t len, int flags)
{
    ssize_t ret;
    size_t sofar = 0;
    char buf[40];

    while(sofar<len) {
        if(shWaitFor(s, SH_CANRX, flags))
            return -1;

        ret = recv(s->sd, buf, sizeof(buf), 0);
        if(ret<=0 && SOCKERRNO==SOCK_EINTR)
            continue;
        else if(ret<=0)
            return ret;

        sofar += ret;
    }
    return sofar;
}

ssize_t shRecvFrom(shSocket* s, void *buf, size_t len, int flags,
                   osiSockAddr *peer)
{
    ssize_t ret;
    osiSocklen_t slen = sizeof(*peer);

    if(shWaitFor(s, SH_CANRX, flags))
        return -1;

    do {
        ret = recvfrom(s->sd, buf, len, 0, &peer->sa, &slen);
    } while(ret==-1 && SOCKERRNO==SOCK_EINTR);

    if(ret<0) {
        if(SOCKERRNO==SOCK_EWOULDBLOCK) {
            /* we already waited and were told that some data was available,
             * so treat this as a timeout
             */
            SOCKERRNOSET(SOCK_ETIMEDOUT);
        }
    } else if(slen<sizeof(peer->ia)) {
        SOCKERRNOSET(SOCK_EADDRINUSE); /* something strange happened... */
        ret = -1;
    }
    return ret;
}

int shSendTo(shSocket* s, const void *buf, size_t len, int flags,
             const osiSockAddr* peer)
{
    ssize_t ret;

    if(shWaitFor(s, SH_CANTX, flags))
        return -1;

    do {
        ret = sendto(s->sd, buf, len, 0, &peer->sa, sizeof(*peer));
    } while(ret==-1 && SOCKERRNO==SOCK_EINTR);

    return ret!=len;
}

int shSendAll(shSocket* s, const void *buf, size_t len, int flags)
{
    ssize_t ret;
    const char *cbuf = buf;
    size_t sofar = 0;

    while(sofar<len) {

        if(shWaitFor(s, SH_CANTX, flags))
            return -1;

        ret = send(s->sd, cbuf+sofar, len-sofar, MSG_NOSIGNAL);
        if(ret<=0 && SOCKERRNO==SOCK_EINTR)
            continue;
        else if(ret<=0)
            return ret;

        sofar += ret;
    }

    return 1;
}
