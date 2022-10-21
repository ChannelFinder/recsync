#ifndef SOCKHELPERS_H
#define SOCKHELPERS_H
/* blocking socket operations w/ timeouts and interruptions */
#include <epicsTypes.h>
#include <osiSock.h>

#if defined(_MSC_VER)
    #include <BaseTsd.h>
    #include <WinSock2.h>
    typedef SSIZE_T ssize_t;
#endif

#ifndef SOCKERRNOSET
# if defined(WIN32) || defined(WIN64)
#   define SOCKERRNOSET(E)  WSASetLastError(E)
# else
#   define SOCKERRNOSET(E) do{ SOCKERRNO = (E); }while(0)
# endif
#endif

#include <shareLib.h>

#ifndef MSG_NOSIGNAL
#  define MSG_NOSIGNAL 0
#endif

typedef struct {
    SOCKET sd; /* data socket */
    SOCKET wakeup; /* force timeout socket */
    struct timeval timeout; /* normal timeout */
} shSocket;

epicsShareFunc
void shSocketInit(shSocket *s);

epicsShareFunc
void shSetTimeout(shSocket* s, double val);

/* create a new non-blocking socket */
epicsShareFunc
SOCKET shCreateSocket(int domain, int type, int protocol);

/* create a pair of connected stream sockets */
epicsShareFunc
int shSocketPair(SOCKET sd[2]);

/* Connect w/ timeout */
epicsShareFunc
int shConnect(shSocket* s, const osiSockAddr *peer);

#define SH_CANTX 1
#define SH_CANRX 2

/* disable timeout for this call */
#define MSG_NOTIMO 0x4000

/* wait for socket sd event, or data available on
 * wakeup socket.
 * Wakeup socket ready is treated as a timeout
 * on socket sd.
 */
epicsShareFunc
int shWaitFor(shSocket* s, int op, int flags);

/* recv() exactly the requested number of bytes.
 * returns either 'len', 0 for disconnect, or an error -1.
 */
epicsShareFunc
ssize_t shRecvExact(shSocket* s, void *buf, size_t len, int flags);

/* pull 'len' bytes from the stream and discard them */
epicsShareFunc
ssize_t shRecvIgnore(shSocket* s, size_t len, int flags);

epicsShareFunc
ssize_t shRecvFrom(shSocket* s, void *buf, size_t len, int flags,
                   osiSockAddr *peer);

/* returns zero if all bytes sent. -1 on error, 1 on incomplete */
epicsShareFunc
int shSendTo(shSocket* s, const void *buf, size_t len, int flags,
             const osiSockAddr* peer);

/* returns 1 if all bytes sent, 0 when connection lost, and -1 on error */
epicsShareFunc
int shSendAll(shSocket* s, const void *buf, size_t len, int flags);

epicsShareFunc
int socketpair_compat(int af, int st, int p, SOCKET sd[2]);

#endif // SOCKHELPERS_H
