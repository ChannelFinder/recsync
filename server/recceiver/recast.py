from __future__ import annotations

import collections
import logging
import secrets
import struct
import sys
import time
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable

from twisted.internet import defer, protocol, reactor
from twisted.protocols import stateful

from .interfaces import CommitTransaction, SourceAddress

_log = logging.getLogger(__name__)

_M = 0x5243

_Head = struct.Struct(">HHI")
if _Head.size != 8:  # noqa: PLR2004
    msg = f"Expected _Head.size == 8, got {_Head.size}"
    raise RuntimeError(msg)

_ping = struct.Struct(">I")
if _ping.size != 4:  # noqa: PLR2004
    msg = f"Expected _ping.size == 4, got {_ping.size}"
    raise RuntimeError(msg)

_s_greet = struct.Struct(">B")
if _s_greet.size != 1:
    msg = f"Expected _s_greet.size == 1, got {_s_greet.size}"
    raise RuntimeError(msg)

_c_greet = struct.Struct(">BBxxI")
if _c_greet.size != 8:  # noqa: PLR2004
    msg = f"Expected _c_greet.size == 8, got {_c_greet.size}"
    raise RuntimeError(msg)

_c_info = struct.Struct(">IBxH")
if _c_info.size != 8:  # noqa: PLR2004
    msg = f"Expected _c_info.size == 8, got {_c_info.size}"
    raise RuntimeError(msg)

_c_rec = struct.Struct(">IBBH")
if _c_rec.size != 8:  # noqa: PLR2004
    msg = f"Expected _c_rec.size == 8, got {_c_rec.size}"
    raise RuntimeError(msg)


class CastReceiver(stateful.StatefulProtocol):
    timeout = 3.0
    version = 0

    def __init__(self, active: bool = True) -> None:  # noqa: FBT001, FBT002
        self.reactor = reactor

        self.sess: CollectionSession | None = None
        self.active = active
        self.uploadSize, self.uploadStart = 0, 0.0

        self.rxfn: dict[int, tuple[Callable, int]] = collections.defaultdict(self.dfact)

        self.rxfn[1] = (self.recvClientGreeting, _c_greet.size)
        self.rxfn[2] = (self.recvPong, _ping.size)
        self.rxfn[3] = (self.recvAddRec, _c_rec.size)
        self.rxfn[4] = (self.recvDelRec, _ping.size)
        self.rxfn[5] = (self.recvDone, -1)
        self.rxfn[6] = (self.recvInfo, _c_info.size)

    def writeMsg(self, msgid: int, body: bytes) -> None:
        head = _Head.pack(_M, msgid, len(body))
        msg = b"".join((head, body))
        self.transport.write(msg)

    def dataReceived(self, data: bytes) -> None:
        self.uploadSize += len(data)
        stateful.StatefulProtocol.dataReceived(self, data)

    def connectionMade(self) -> None:
        if self.active:
            # Full speed ahead
            self.phase = 1  # 1: send ping, 2: receive pong
            self.T = self.reactor.callLater(self.timeout, self.writePing)
            self.writeMsg(0x8001, _s_greet.pack(self.version))
            self.uploadStart = time.time()
        else:
            # apply brakes
            self.transport.pauseProducing()

    def connectionLost(self, reason: Any = protocol.connectionDone) -> None:  # noqa: ARG002, ANN401
        self.factory.isDone(self, self.active)
        if hasattr(self, "T") and self.T and self.T.active():
            self.T.cancel()
        if hasattr(self, "T"):
            del self.T
        if self.sess:
            self.sess.close()
        del self.sess

    def restartPingTimer(self) -> None:
        T, self.T = self.T, self.reactor.callLater(self.timeout, self.writePing)
        if T and T.active():
            T.cancel()

    def writePing(self) -> None:
        if self.phase == 2:  # noqa: PLR2004
            self.transport.loseConnection()
            _log.debug("pong missed: close connection")
        else:
            self.restartPingTimer()
            self.phase = 2
            self.nonce = secrets.randbelow(0x100000000)
            self.writeMsg(0x8002, _ping.pack(self.nonce))
            _log.debug(f"ping nonce: {self.nonce}")

    def getInitialState(self) -> tuple[Callable, int]:
        return (self.recvHeader, 8)

    def recvHeader(self, data: bytes) -> tuple[Callable, int] | None:
        self.restartPingTimer()
        magic, msgid, blen = _Head.unpack(data)
        if magic != _M:
            _log.error(f"Protocol error! Bad magic {magic}")
            self.transport.loseConnection()
            return None
        self.msgid = msgid
        fn, minlen = self.rxfn[self.msgid]
        if minlen >= 0 and blen < minlen:
            return (self.ignoreBody, blen)
        return (fn, blen)

    # 0x0001
    def recvClientGreeting(self, body: bytes) -> tuple[Callable, int] | None:
        cver, ctype, skey = _c_greet.unpack(body[: _c_greet.size])
        if ctype != 0:
            _log.error(f"I don't understand you! {ctype}")
            self.transport.loseConnection()
            return None
        self.version = min(self.version, cver)
        self.clientKey = skey
        self.sess = self.factory.addClient(self, self.transport.getPeer())
        return self.getInitialState()

    # 0x0002
    def recvPong(self, body: bytes) -> tuple[Callable, int]:
        (nonce,) = _ping.unpack(body[: _ping.size])
        if nonce != self.nonce:
            _log.error(f"pong nonce does not match! {nonce}!={self.nonce}")
            self.transport.loseConnection()
        else:
            _log.debug("pong nonce match")
            self.phase = 1
        return self.getInitialState()

    # 0x0006
    def recvInfo(self, body: bytes) -> tuple[Callable, int]:
        record_id, klen, vlen = _c_info.unpack(body[: _c_info.size])
        text = body[_c_info.size :].decode()
        if klen == 0 or klen + vlen < len(text):
            _log.error("Ignoring info update")
            return self.getInitialState()
        key = text[:klen]
        val = text[klen : klen + vlen]
        if self.sess:
            if record_id:
                self.sess.recInfo(record_id, key, val)
            else:
                self.sess.iocInfo(key, val)
        return self.getInitialState()

    # 0x0003
    def recvAddRec(self, body: bytes) -> tuple[Callable, int]:
        record_id, record_type, rtlen, rnlen = _c_rec.unpack(body[: _c_rec.size])
        text = body[_c_rec.size :].decode()
        if rnlen == 0 or rtlen + rnlen < len(text):
            _log.error("Ignoring record update")

        elif self.sess:
            if rtlen > 0 and record_type == 0:  # new record
                rectype = text[:rtlen]
                recname = text[rtlen : rtlen + rnlen]
                self.sess.addRecord(record_id, rectype, recname)

            elif record_type == 1:  # record alias
                recname = text[rtlen : rtlen + rnlen]
                self.sess.addAlias(record_id, recname)

        return self.getInitialState()

    # 0x0004
    def recvDelRec(self, body: bytes) -> tuple[Callable, int]:
        (record_id,) = _ping.unpack(body[: _ping.size])
        if self.sess:
            self.sess.delRecord(record_id)
        return self.getInitialState()

    # 0x0005
    def recvDone(self, body: bytes) -> tuple[Callable, int]:  # noqa: ARG002
        self.factory.isDone(self, self.active)
        if self.sess:
            self.sess.done()
        if self.phase == 1:
            self.writePing()

        elapsed_s = time.time() - self.uploadStart
        size_kb = self.uploadSize / 1024
        rate_kbs = size_kb / elapsed_s
        if self.sess:
            source_address = f"{self.sess.ep.host}:{self.sess.ep.port}"
            _log.info(
                f"Done message from {source_address}: uploaded {size_kb}kB in {elapsed_s}s ({rate_kbs}kB/s)",
            )

        return self.getInitialState()

    def ignoreBody(self, body: bytes) -> tuple[Callable, int]:  # noqa: ARG002
        return self.getInitialState()

    @classmethod
    def dfact(cls) -> tuple[Callable, int]:
        return (cls.ignoreBody, -1)


class Transaction:
    def __init__(self, ep: Any, transaction_id: int) -> None:  # noqa: ANN401
        self.connected = True
        self.initial = False
        self.source_address = ep
        self.srcid = transaction_id
        self.records_to_add: dict[str, tuple[str, str]] = {}
        self.client_infos: dict[str, str] = {}
        self.record_infos_to_add: dict[str, dict[str, str]] = {}
        self.aliases: dict[str, list[str]] = collections.defaultdict(list)
        self.records_to_delete: set[str] = set()

    def to_dataclass(self) -> CommitTransaction:
        return CommitTransaction(
            source_address=SourceAddress(host=self.source_address.host, port=self.source_address.port),
            srcid=self.srcid,
            client_infos=dict(self.client_infos),
            records_to_add=dict(self.records_to_add),
            records_to_delete=set(self.records_to_delete),
            record_infos_to_add=dict(self.record_infos_to_add),
            aliases=dict(self.aliases),
            initial=self.initial,
            connected=self.connected,
        )

    def show(self, fp: Any = sys.stdout) -> None:  # noqa: ARG002, ANN401
        _log.info(str(self))

    def __str__(self) -> str:
        source_address = f"{self.source_address.host}:{self.source_address.port}"
        init = self.initial
        conn = self.connected
        nenv = len(self.client_infos)
        nadd = len(self.records_to_add)
        ndel = len(self.records_to_delete)
        ninfo = len(self.record_infos_to_add)
        nalias = len(self.aliases)
        return f"Transaction(Src:{source_address}, Init:{init}, Conn:{conn}, Env:{nenv}, Rec:{nadd}, Alias:{nalias}, Info:{ninfo}, Del:{ndel})"

    def __repr__(self) -> str:
        return f"""Transaction(
            source_address={self.source_address},
            initial={self.initial},
            connected={self.connected},
            records_to_add={self.records_to_add},
            client_infos={self.client_infos},
            record_infos_to_add={self.record_infos_to_add},
            aliases={self.aliases},
            records_to_delete={self.records_to_delete})
            """


class CollectionSession:
    timeout = 5.0
    trlimit = 0
    factory: CastFactory

    def __init__(self, proto: CastReceiver, endpoint: Any) -> None:  # noqa: ANN401
        _log.info(f"Open session from {endpoint}")
        self.reactor = reactor
        self.proto, self.ep = proto, endpoint
        self.transaction = Transaction(self.ep, id(self))
        self.transaction.initial = True
        self.C = defer.succeed(None)
        self.T: float | None = None
        self.dirty = False

    def close(self) -> None:
        _log.info(f"Close session from {self.ep}")

        def suppressCancelled(err: defer.Failure) -> defer.Failure | None:
            if not err.check(defer.CancelledError):
                return err
            _log.debug("Suppress the expected CancelledError")
            return None

        self.C.addErrback(suppressCancelled).cancel()

        # Clear the current transaction and
        # commit an empty one for disconnect.
        self.transaction = Transaction(self.ep, id(self))
        self.transaction.connected = False
        self.dirty = True
        self.flush()

    def flush(self, connected: bool = True) -> None:  # noqa: FBT001, FBT002, ARG002
        _log.info(f"Flush session from {self.ep}")
        self.T = None
        if not self.dirty:
            return

        transaction, self.transaction = self.transaction, Transaction(self.ep, id(self))
        self.dirty = False

        def commit(_ignored: Any) -> defer.Deferred:  # noqa: ANN401
            commit_transaction = transaction.to_dataclass()
            _log.info(f"Commit: {commit_transaction}")
            return defer.maybeDeferred(self.factory.commit, commit_transaction)

        def abort(err: defer.Failure) -> None:
            if err.check(defer.CancelledError):
                _log.info(f"Commit cancelled: {transaction}")
                return
            _log.error(f"Commit failure: {err}")
            self.proto.transport.loseConnection()
            raise defer.CancelledError

        self.C.addCallback(commit).addErrback(abort)

    # Flushes must NOT occur at arbitrary points in the data stream
    # because that can result in a PV and its record info or aliases being split
    # between transactions. Only flush after Add or Del or Done message received.
    def flushSafely(self) -> None:
        if (self.T and time.time() >= self.T) or (
            self.trlimit
            and self.trlimit <= (len(self.transaction.records_to_add) + len(self.transaction.records_to_delete))
        ):
            self.flush()

    def markDirty(self) -> None:
        if not self.T:
            self.T = time.time() + self.timeout
        self.dirty = True

    def done(self) -> None:
        self.flush()

    def iocInfo(self, key: str, val: str) -> None:
        self.transaction.client_infos[key] = val
        self.markDirty()

    def addRecord(self, record_id: str, record_type: str, record_name: str) -> None:
        self.flushSafely()
        self.transaction.records_to_add[record_id] = (record_name, record_type)
        self.markDirty()

    def addAlias(self, record_id: str, record_name: str) -> None:
        self.transaction.aliases[record_id].append(record_name)
        self.markDirty()

    def delRecord(self, record_id: str) -> None:
        self.flushSafely()
        self.transaction.records_to_add.pop(record_id, None)
        self.transaction.records_to_delete.add(record_id)
        self.transaction.record_infos_to_add.pop(record_id, None)
        self.markDirty()

    def recInfo(self, record_id: str, key: str, val: str) -> None:
        try:
            client_infos = self.transaction.record_infos_to_add[record_id]
        except KeyError:
            client_infos = {}
            self.transaction.record_infos_to_add[record_id] = client_infos
        client_infos[key] = val
        self.markDirty()


class CastFactory(protocol.ServerFactory):
    protocol = CastReceiver
    session = CollectionSession

    maxActive = 3

    def __init__(self) -> None:
        # Flow control by limiting the number of concurrent
        # "active" connectons  Active means dumping lots of records.
        # connections become "inactive" by calling isDone()
        self.NActive = 0
        self.Wait: list[CastReceiver] = []

    def isDone(self, P: CastReceiver, active: bool) -> None:  # noqa: FBT001
        if not active:
            # connection closed before activation
            if P in self.Wait:
                self.Wait.remove(P)
        elif len(self.Wait) > 0:
            # Others are waiting
            P2 = self.Wait.pop(0)
            P2.active = True
            P2.transport.resumeProducing()
            P2.connectionMade()
        else:
            self.NActive -= 1

    def buildProtocol(self, addr: Any) -> CastReceiver:  # noqa: ARG002, ANN401
        active = self.NActive < self.maxActive
        P = self.protocol(active=active)
        P.factory = self
        if not active:
            self.Wait.append(P)
        return P

    def addClient(self, proto: CastReceiver, address: Any) -> CollectionSession:  # noqa: ANN401
        S = self.session(proto, address)
        S.factory = self
        return S

    # Note: this method replaced by RecService
    def commit(self, transaction: CommitTransaction) -> None:
        _log.info(f"Commit: {transaction}")
