#ifndef CASTER_H
#define CASTER_H

#if defined(_MSC_VER)
    #include <BaseTsd.h>
    #include <WinSock2.h>
    typedef SSIZE_T ssize_t;
#endif

#include <epicsTypes.h>
#include <osiSock.h>
#include <epicsEvent.h>
#include <epicsMutex.h>
#include <compilerDependencies.h>
#include <ellLib.h>

#include "sockhelpers.h"

#define RECAST_PORT 5049

epicsShareExtern double reccastTimeout;
epicsShareExtern double reccastMaxHoldoff;

extern const char* default_envs[];
extern const size_t default_envs_count;

typedef enum {
  casterUDPSetup,
  casterTCPSetup,
} caster_h;

typedef enum {
    casterStateInit,
    casterStateListen,
    casterStateConnect,
    casterStateUpload,
    casterStateDone,
} casterState;

typedef struct {
    ELLNODE node;
    char *item_str;
} string_list_t;

typedef struct _caster_t {
    double timeout;

    unsigned short udpport;

    ssize_t nextRecID;
    shSocket *csock;

    /* doCaster*Phase() listens on [1] */
    SOCKET wakeup[2];

    int errors;
    epicsEventId shutdownEvent;

    int haveserv;
    osiSockAddr nameserv;
    epicsUInt32 servkey;

    /* overwritable hook functions */
    void (*testhook)(struct _caster_t*, caster_h);

    void *arg;
    int (*getrecords)(void*, struct _caster_t*);
    void (*onmsg)(void*, struct _caster_t*);

    casterState current;

    /* lock for message and shutdown flags */
    epicsMutexId lock;

    int shutdown;
    char lastmsg[MAX_STRING_SIZE];

    ELLLIST envs;
    ELLLIST exclude_patterns;

} caster_t;

epicsShareFunc
void casterMsg(caster_t *self, const char* msg, ...) EPICS_PRINTF_STYLE(2,3);

#define ERRTO(TAG, CASTER, ...) do{ casterMsg(CASTER, __VA_ARGS__); goto TAG; }while(0)

#define ERRRET(VAL, CASTER, ...) do{ casterMsg(CASTER, __VA_ARGS__); return (VAL); }while(0)

epicsShareFunc
void casterInit(caster_t *self);
epicsShareFunc
void casterShutdown(caster_t *self);
epicsShareFunc
int casterStart(caster_t *self);

epicsShareFunc
ssize_t casterSendRecord(caster_t* c, const char* rtype, const char* rname);
epicsShareFunc
ssize_t casterSendAlias(caster_t* c, size_t rid, const char* rname);
epicsShareFunc
int casterSendInfo(caster_t *c, ssize_t rid, const char* name, const char* val);

/* push process database information */
epicsShareFunc
int casterPushPDB(void *junk, caster_t *caster);

epicsShareFunc
void addToReccasterLinkedList(caster_t* self, size_t itemCount, const char **items, ELLLIST* reccastList, const char* funcName, const char* itemDesc);

epicsShareFunc
void addReccasterEnvVars(caster_t* self, int argc, char **argv);

epicsShareFunc
void addReccasterExcludePattern(caster_t* self, int argc, char **argv);

/* internal */

epicsShareFunc
int doCasterUDPPhase(caster_t *self);
epicsShareFunc
int doCasterTCPPhase(caster_t *self);

/* recv() a TCP protocol message.
 * Message id is stored in *id.
 * returns the number of body bytes stored in the buffer.
 * If a message w/ zero body length is recieved this
 * is reported as a disconnect.
 *
 * Unless MSG_PARTIAL is given, message bodies too long
 * to find in the provided buffer will trigger an error.
 */
epicsShareFunc
ssize_t casterRecvPMsg(shSocket* s, epicsUInt16* id,
                       void *buf, size_t len, int flags);

/* recv() a TCP protocol header.
 *
 * returns 1 on success, 0 when connection is lost, and -1 on error
 */
epicsShareFunc
int casterRecvPHead(shSocket *s, epicsUInt16* id, epicsUInt32* len, int flags);

epicsShareFunc
int casterSendPHead(shSocket* s, epicsUInt16 id, epicsInt32 blen);

#define RECAST_MAGIC 0x5243 /* 'RC' */

/* UDP server advertisement message
 *
 *    | 0 | 1 | 2 | 3 |
 * 00 | ID    | 0 | X |
 * 04 |   SERV ADDR   |
 * 08 | PORT  | X | X |
 * 0C |   SERV KEY    |
 * 10 |   ignore...
 */
typedef struct {
    epicsUInt16 pid; /* protocol ID RECAST_MAGIC */
    epicsUInt8 version; /* must be 0 */
    epicsUInt8 reserved0;
    epicsUInt32 serverIP;
    epicsUInt16 serverPort;
    epicsUInt16 reserved1;
    epicsUInt32 serverKey;
} casterAnnounce;

union casterUDP {
    casterAnnounce m_msg;
    char m_bytes[sizeof(casterAnnounce)];
};

/* TCP message header
 *
 *    | 0 | 1 | 2 | 3 |
 * 00 |  PID  | MSGID |
 * 04 |  body length  |
 * 08 | body bytes...
 */
typedef struct {
    epicsUInt16 pid; /* protocol ID RECAST_MAGIC */
    epicsUInt16 msgid;
    epicsUInt32 bodylen;
} casterHeader;

union casterTCPHead {
    casterHeader m_msg;
    char m_bytes[sizeof(casterHeader)];
};

/* server messages */
typedef struct {
    epicsUInt8 version;
} casterServerGreet; /* 0x8001 */

typedef struct {
    epicsUInt32 nonce;
} casterPing; /* 0x8002 and 0x0002 */

/* client messages */
typedef struct {
    epicsUInt8 version;
    epicsUInt8 type; /* 0 - static, 1 - dynamic, >127  - Site specific */
    epicsUInt8 reserved[2];
    epicsUInt32 serverKey;
} casterClientGreet; /* 0x0001 */

typedef struct {
    epicsUInt32 rid; /* record instance id */
    epicsUInt8 rtype; /* 0 - IOC Rec, 1 - IOC Alias */
    epicsUInt8 rtlen; /* # of bytes in record type name (0 for aliases) */
    epicsUInt16 rnlen; /* # of bytes in record instance name */
    /* record type and instance names follow */
} casterClientAddRec; /* 0x0003 */

typedef struct {
    epicsUInt32 rid; /* record instance id */
} casterClientDelRec; /* 0x0004 */

/* casterClientDone 0x0005 dummy 4 byte payload */

typedef struct {
    epicsUInt32 rid; /* record instance id */
    epicsUInt8 klen; /* # of bytes in record type name */
    epicsUInt8 reserved;
    epicsUInt16 vlen; /* # of bytes in record instance name */
    /* key and value strings follow */
} casterClientAddInfo; /* 0x0006 */

union casterTCPBody {
    casterServerGreet s_greet;
    casterPing ping;

    casterClientGreet c_greet;
    casterClientAddRec c_add;
    casterClientDelRec c_del;
    casterClientAddInfo c_info;

    char bytes[sizeof(casterClientAddRec)];
};


#endif // CASTER_H
