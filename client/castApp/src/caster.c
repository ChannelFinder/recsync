
#include <stdlib.h>
#include <stdarg.h>
#include <string.h>

#include <errlog.h>
#include <cantProceed.h>
#include <epicsAssert.h>
#include <epicsThread.h>
#include <epicsStdio.h>

#define epicsExportSharedSymbols

#include "caster.h"

#ifdef STATIC_ASSERT
STATIC_ASSERT(sizeof(casterAnnounce)==0x10);
STATIC_ASSERT(sizeof(casterHeader)==8);
#else
#  warning Base version does not provide STATIC_ASSERT.  Skipping checks.
#endif

epicsShareDef double reccastTimeout = 20.0;
epicsShareDef double reccastMaxHoldoff = 10.0;

static void casterThread(void* junk)
{
    caster_t *self=junk;

    casterMsg(self, "Starting");

    while(1) {
        int shutdown;
        epicsMutexMustLock(self->lock);
        shutdown = self->shutdown;
        epicsMutexUnlock(self->lock);

        if(shutdown)
            break;

        self->timeout = reccastTimeout;

        if(self->errors>10)
            self->errors=10;
        if(self->errors)
            epicsThreadSleep(self->errors*5.0);

        self->errors++; /* be pessemistic */

        self->current = casterStateListen;
        casterMsg(self, "Searching");


        if(doCasterUDPPhase(self))
            continue;

        casterMsg(self, "Found server");

        /* random holdoff */
        {
            int result;
            shSocket junk;
            double holdoff = reccastMaxHoldoff*rand()/RAND_MAX;

            if(holdoff>2.0)
                casterMsg(self, "connect hold-off %f", holdoff);

            shSocketInit(&junk);
            shSetTimeout(&junk, holdoff);
            junk.wakeup = self->wakeup[1];

            result = shWaitFor(&junk, 0, 0);
            if(result!=-1 && SOCKERRNO!=SOCK_ETIMEDOUT) {
                casterMsg(self, "holdoff error");
                continue;
            }
        }

        self->current = casterStateConnect;
        if(self->haveserv) {
            char abuf[80];
            sockAddrToA(&self->nameserv.sa, abuf, sizeof(abuf));
            casterMsg(self, "Connecting to: %s", abuf);
        }

        if(!doCasterTCPPhase(self))
            self->errors = 0;

        self->current = casterStateListen;
        if(self->haveserv) {
            char abuf[80];
            sockAddrToA(&self->nameserv.sa, abuf, sizeof(abuf));
            casterMsg(self, "Lost server: %s", abuf);
        }
    }

    casterMsg(self, "Stopping");

    epicsEventSignal(self->shutdownEvent);
}

static
void casterShowMsgDefault(void* arg, struct _caster_t* self)
{
    errlogMessage(self->lastmsg);
    errlogMessage("\n");
}

void casterInit(caster_t *self)
{
    memset(self, 0, sizeof(*self));
    self->udpport = RECAST_PORT;
    self->shutdownEvent = epicsEventMustCreate(epicsEventEmpty);
    self->lock = epicsMutexMustCreate();
    self->nextRecID = 1;
    self->onmsg = &casterShowMsgDefault;
    self->current = casterStateInit;
    self->timeout = reccastTimeout;

    if(shSocketPair(self->wakeup))
        cantProceed("casterInit failed to create shutdown socket");
}

void casterShutdown(caster_t *self)
{
    epicsUInt32 junk = htonl(0xdeadbeef);

    epicsMutexMustLock(self->lock);
    self->shutdown = 1;
    epicsMutexUnlock(self->lock);

    if(sizeof(junk)!=send(self->wakeup[0], (char*)&junk, sizeof(junk), 0))
        cantProceed("casterShutdown notification failed");

    epicsEventMustWait(self->shutdownEvent);

    epicsEventDestroy(self->shutdownEvent);
    self->shutdownEvent = NULL;
    epicsSocketDestroy(self->wakeup[0]);
    epicsSocketDestroy(self->wakeup[1]);

    epicsMutexDestroy(self->lock);
}

int casterStart(caster_t *self)
{
    epicsThreadId id;
    id = epicsThreadCreate("reccaster",
                           epicsThreadPriorityMedium,
                           epicsThreadGetStackSize(epicsThreadStackSmall),
                           &casterThread, self);
    return !id;
}


void casterMsg(caster_t *self, const char* msg, ...)
{
    int ret;
    va_list args;

    va_start(args, msg);
    ret = epicsVsnprintf(self->lastmsg, sizeof(self->lastmsg), msg, args);
    va_end(args);

    if(ret<0) {
        errlogMessage("casterMsg failed\n");
        return;
    }

    self->lastmsg[sizeof(self->lastmsg)-1] = '\0';

    (*self->onmsg)(self->arg, self);
}

static
ssize_t casterSendRA(caster_t* self, epicsUInt8 type, size_t rid, const char* rtype, const char* rname)
{
    union casterTCPBody buf;
    epicsUInt32 blen = sizeof(buf.c_add);
    size_t lt=rtype ? strlen(rtype) : 0, ln=strlen(rname);

    buf.c_add.rid = htonl(rid);
    buf.c_add.rtype = type;
    buf.c_add.rtlen = lt;
    buf.c_add.rnlen = htons(ln);

    blen += lt + ln;

    if(casterSendPHead(self->csock, 0x0003, blen)!=1)
        return -1;

    if(shSendAll(self->csock, &buf.bytes, sizeof(buf.c_add), 0)!=1)
        return -1;

    if(rtype && shSendAll(self->csock, rtype, lt, 0)!=1)
        return -1;

    if(shSendAll(self->csock, rname, ln, 0)!=1)
        return -1;

    return 0;
}

ssize_t casterSendRecord(caster_t* self, const char* rtype, const char* rname)
{
    size_t rid;

    if(self->nextRecID<0)
        return -1;

    rid = self->nextRecID++;

    if(casterSendRA(self, 0, rid, rtype, rname))
        return -1;
    return rid;
}

ssize_t casterSendAlias(caster_t* self, size_t rid, const char* rname)
{
    return casterSendRA(self, 1, rid, NULL, rname);
}

int casterSendInfo(caster_t *self, ssize_t rid, const char* name, const char* val)
{
    union casterTCPBody buf;
    epicsUInt32 blen = sizeof(buf.c_info);
    size_t ln=strlen(name), lv=strlen(val);

    if(rid<0)
        return -1;

    buf.c_info.rid = htonl(rid);
    buf.c_info.klen = ln;
    buf.c_info.reserved = 0;
    buf.c_info.vlen = htons(lv);

    blen += ln + lv;

    if(casterSendPHead(self->csock, 0x0006, blen)!=1)
        return -1;

    if(shSendAll(self->csock, &buf.bytes, sizeof(buf.c_info), 0)!=1)
        return -1;

    if(shSendAll(self->csock, name, ln, 0)!=1)
        return -1;

    if(shSendAll(self->csock, val, lv, 0)!=1)
        return -1;

    return 0;
}

int casterRecvPHead(shSocket *s, epicsUInt16* id, epicsUInt32* len, int flags)
{
    int ret;
    union casterTCPHead hbuf;

    ret = shRecvExact(s, &hbuf.m_bytes, sizeof(hbuf.m_bytes), flags);
    if(ret!=sizeof(hbuf.m_bytes))
        return ret;

    if(ntohs(hbuf.m_msg.pid) != RECAST_MAGIC)
        return -1;

    *id = ntohs(hbuf.m_msg.msgid);

    *len = ntohl(hbuf.m_msg.bodylen);

    return 1;
}

ssize_t casterRecvPMsg(shSocket* s, epicsUInt16* id,
                       void *buf, size_t len, int flags)
{
    union casterTCPHead hbuf;
    ssize_t ret, blen, rlen;

    ret = shRecvExact(s, &hbuf.m_bytes, sizeof(hbuf.m_bytes), flags);
    if(ret!=sizeof(hbuf.m_bytes))
        return ret;

    if(ntohs(hbuf.m_msg.pid) != RECAST_MAGIC)
        return -1;

    *id = ntohs(hbuf.m_msg.msgid);

    blen = ntohl(hbuf.m_msg.bodylen);
    if(blen<0)
        return -1; /* overflow */

    ret = shRecvExact(s, buf, blen>len ?len :blen, flags);
    if(ret<=0)
        return -1;
    rlen = ret;

    if(blen>len) {
        ret = shRecvIgnore(s, blen-len, flags);
        if(ret<=0)
            return -1;
    }

    return rlen;
}

int casterSendPHead(shSocket* s, epicsUInt16 id, epicsInt32 blen)
{
    union casterTCPHead buf;

    buf.m_msg.pid = htons(RECAST_MAGIC);
    buf.m_msg.msgid = htons(id);
    buf.m_msg.bodylen = htonl(blen);

    return shSendAll(s, buf.m_bytes, sizeof(buf.m_msg), 0);
}
