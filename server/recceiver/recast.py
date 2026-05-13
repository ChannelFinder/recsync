# -*- coding: utf-8 -*-

import collections
import logging
import random
import time

from twisted.internet import defer, protocol
from twisted.protocols import stateful
from zope.interface import implementer

from .interfaces import ITransaction
from .protocol import messages

log = logging.getLogger(__name__)


class CastReceiver(stateful.StatefulProtocol):
    timeout = 3.0
    version = 0

    def __init__(self, active=True):
        from twisted.internet import reactor

        self.reactor = reactor

        self.sess, self.active = None, active
        self.uploadSize, self.uploadStart = 0, 0

        self.rxfn = collections.defaultdict(self.dfact)

        self.rxfn[messages.ClientGreeting.msg_id] = (self.recvClientGreeting, messages.ClientGreeting.payload.size)
        self.rxfn[messages.Pong.msg_id] = (self.recvPong, messages.Pong.payload.size)
        self.rxfn[messages.AddRecord.msg_id] = (self.recvAddRec, messages.AddRecord.payload.size)
        self.rxfn[messages.DelRecord.msg_id] = (self.recvDelRec, messages.DelRecord.payload.size)
        self.rxfn[messages.UploadDone.msg_id] = (self.recvDone, messages.UploadDone.payload.size)
        self.rxfn[messages.AddInfo.msg_id] = (self.recvInfo, messages.AddInfo.payload.size)

    def dataReceived(self, data):
        self.uploadSize += len(data)
        stateful.StatefulProtocol.dataReceived(self, data)

    def connectionMade(self):
        if self.active:
            # Full speed ahead
            self.phase = 1  # 1: send ping, 2: receive pong
            self._ping_timer = self.reactor.callLater(self.timeout, self.writePing)
            self.transport.write(messages.ServerGreeting(self.version).frame())
            self.uploadStart = time.time()
        else:
            # apply brakes
            self.transport.pauseProducing()

    def connectionLost(self, reason=protocol.connectionDone):
        self.factory.isDone(self, self.active)
        if self._ping_timer and self._ping_timer.active():
            self._ping_timer.cancel()
        del self._ping_timer
        if self.sess:
            self.sess.close()
        del self.sess

    def restartPingTimer(self):
        old, self._ping_timer = self._ping_timer, self.reactor.callLater(self.timeout, self.writePing)
        if old and old.active():
            old.cancel()

    def writePing(self):
        if self.phase == 2:
            self.transport.loseConnection()
            log.debug("pong missed: close connection")
        else:
            self.restartPingTimer()
            self.phase = 2
            self.nonce = random.randint(0, 0xFFFFFFFF)
            self.transport.write(messages.Ping(self.nonce).frame())
            log.debug("ping nonce: %s", self.nonce)

    def getInitialState(self):
        return (self.recvHeader, messages.Header.payload.size)

    def recvHeader(self, data):
        self.restartPingTimer()
        try:
            header = messages.Header.decode(data)
        except messages.ProtocolError as exc:
            log.error("Protocol error! %s", exc)
            self.transport.loseConnection()
            return
        if header.body_length == 0:
            log.debug("Ignoring empty message %#06x", header.msg_id)
            return self.getInitialState()
        self.msgid = header.msg_id
        fn, minlen = self.rxfn[self.msgid]
        if minlen >= 0 and header.body_length < minlen:
            return (self.ignoreBody, header.body_length)
        else:
            return (fn, header.body_length)

    # 0x0001
    def recvClientGreeting(self, body):
        try:
            greeting = messages.ClientGreeting.decode(body)
        except messages.ProtocolError as exc:
            log.error("Protocol error! %s", exc)
            self.transport.loseConnection()
            return
        if greeting.client_type != 0:
            log.error("unsupported client type %s", greeting.client_type)
            self.transport.loseConnection()
            return
        self.version = min(self.version, greeting.version)
        self.clientKey = greeting.server_key
        self.sess = self.factory.addClient(self, self.transport.getPeer())
        return self.getInitialState()

    # 0x0002
    def recvPong(self, body):
        try:
            pong = messages.Pong.decode(body)
        except messages.ProtocolError as exc:
            log.error("Protocol error! %s", exc)
            self.transport.loseConnection()
            return
        if pong.nonce != self.nonce:
            log.error("pong nonce does not match! %s!=%s", pong.nonce, self.nonce)
            self.transport.loseConnection()
        else:
            log.debug("pong nonce match")
            self.phase = 1
        return self.getInitialState()

    # 0x0006
    def recvInfo(self, body):
        try:
            info = messages.AddInfo.decode(body)
        except messages.ProtocolError:
            log.error("Ignoring info update")
            return self.getInitialState()
        if info.record_id:
            self.sess.rec_info(info.record_id, info.key, info.value)
        else:
            self.sess.ioc_info(info.key, info.value)
        return self.getInitialState()

    # 0x0003
    def recvAddRec(self, body):
        try:
            record = messages.AddRecord.decode(body)
        except messages.ProtocolError:
            log.error("Ignoring record update")
            return self.getInitialState()
        if record.is_alias:
            self.sess.add_alias(record.record_id, record.record_name)
        else:
            self.sess.add_record(record.record_id, record.record_type, record.record_name)

        return self.getInitialState()

    # 0x0004
    def recvDelRec(self, body):
        try:
            record = messages.DelRecord.decode(body)
        except messages.ProtocolError:
            log.error("Ignoring delete record update")
            return self.getInitialState()
        self.sess.del_record(record.record_id)
        return self.getInitialState()

    # 0x0005
    def recvDone(self, body):
        try:
            messages.UploadDone.decode(body)
        except messages.ProtocolError:
            log.error("Ignoring done update")
            return self.getInitialState()
        self.factory.isDone(self, self.active)
        self.sess.done()
        if self.phase == 1:
            self.writePing()

        elapsed_s = time.time() - self.uploadStart
        size_kb = self.uploadSize / 1024
        rate_kbs = size_kb / elapsed_s
        log.info(
            "Done message from %s:%s: uploaded %skB in %ss (%skB/s)",
            self.sess.ep.host,
            self.sess.ep.port,
            size_kb,
            elapsed_s,
            rate_kbs,
        )

        return self.getInitialState()

    def ignoreBody(self, body):
        return self.getInitialState()

    @classmethod
    def dfact(cls):
        return (cls.ignoreBody, -1)


@implementer(ITransaction)
class Transaction:
    def __init__(self, ep, id):
        self.connected = True
        self.initial = False
        self.source_address = ep
        self.srcid = id
        self.records_to_add, self.client_infos, self.record_infos_to_add = {}, {}, {}
        self.aliases = collections.defaultdict(list)
        self.records_to_delete = set()

    def show(self):
        log.info(str(self))

    def __str__(self):
        src = f"{self.source_address.host}:{self.source_address.port}"
        return (
            f"Transaction(Src:{src}, Init:{self.initial}, Conn:{self.connected},"
            f" Env:{len(self.client_infos)}, Rec:{len(self.records_to_add)},"
            f" Alias:{len(self.aliases)}, Info:{len(self.record_infos_to_add)},"
            f" Del:{len(self.records_to_delete)})"
        )

    def __repr__(self):
        return (
            f"Transaction("
            f"source_address={self.source_address}, "
            f"initial={self.initial}, "
            f"connected={self.connected}, "
            f"records_to_add={self.records_to_add}, "
            f"client_infos={self.client_infos}, "
            f"record_infos_to_add={self.record_infos_to_add}, "
            f"aliases={self.aliases}, "
            f"records_to_delete={self.records_to_delete})"
        )


class CollectionSession:
    timeout = 5.0
    trlimit = 5000

    def __init__(self, proto, endpoint):
        from twisted.internet import reactor

        log.info("Open session from %s", endpoint)
        self.reactor = reactor
        self.proto, self.ep = proto, endpoint
        self.transaction = Transaction(self.ep, id(self))
        self.transaction.initial = True
        self._commit_chain = defer.succeed(None)
        self._flush_deadline = None
        self.dirty = False

    def close(self):
        log.info("Close session from %s", self.ep)

        # Do not cancel self._commit_chain here. Any data commit that is still queued
        # behind the global lock must be allowed to complete so that channels
        # are registered as active in CF before the disconnect is processed.
        # The disconnect transaction is chained after self._commit_chain and will execute
        # once all preceding commits have finished.
        self.transaction = Transaction(self.ep, id(self))
        self.transaction.connected = False
        self.dirty = True
        self.flush()

    def flush(self):
        log.info("Flush session from %s", self.ep)
        self._flush_deadline = None
        if not self.dirty:
            return

        transaction, self.transaction = self.transaction, Transaction(self.ep, id(self))
        self.transaction.client_infos = dict(transaction.client_infos)
        self.dirty = False

        def commit(_ignored):
            log.info("Commit: %s", transaction)
            return defer.maybeDeferred(self.factory.commit, transaction)

        def abort(err):
            if err.check(defer.CancelledError):
                log.info("Commit cancelled: %s", transaction)
                return err
            else:
                log.error("Commit failure: %s", err)
                self.proto.transport.loseConnection()
                raise defer.CancelledError()

        self._commit_chain.addCallback(commit).addErrback(abort)

    # Flushes must NOT occur at arbitrary points in the data stream
    # because that can result in a PV and its record info or aliases being split
    # between transactions. Only flush after Add or Del or Done message received.
    def flush_safely(self):
        if self._flush_deadline and self._flush_deadline <= time.time():
            log.debug("flush_safely: timeout elapsed for %s", self.ep)
            self.flush()
        elif self.trlimit and self.trlimit <= (
            len(self.transaction.records_to_add) + len(self.transaction.records_to_delete)
        ):
            log.debug("flush_safely: trlimit %d reached for %s", self.trlimit, self.ep)
            self.flush()

    def mark_dirty(self):
        if not self._flush_deadline:
            self._flush_deadline = time.time() + self.timeout
        self.dirty = True

    def done(self):
        self.flush()

    def ioc_info(self, key, val):
        self.transaction.client_infos[key] = val
        self.mark_dirty()

    def add_record(self, record_id, record_type, record_name):
        self.flush_safely()
        self.transaction.records_to_add[record_id] = (record_name, record_type)
        self.mark_dirty()

    def add_alias(self, record_id, record_name):
        self.transaction.aliases[record_id].append(record_name)
        self.mark_dirty()

    def del_record(self, record_id):
        self.flush_safely()
        self.transaction.records_to_add.pop(record_id, None)
        self.transaction.records_to_delete.add(record_id)
        self.transaction.record_infos_to_add.pop(record_id, None)
        self.mark_dirty()

    def rec_info(self, record_id, key, val):
        try:
            client_infos = self.transaction.record_infos_to_add[record_id]
        except KeyError:
            client_infos = {}
            self.transaction.record_infos_to_add[record_id] = client_infos
        client_infos[key] = val
        self.mark_dirty()


class CastFactory(protocol.ServerFactory):
    protocol = CastReceiver
    session = CollectionSession

    maxActive = 3

    def __init__(self):
        # Flow control by limiting the number of concurrent
        # "active" connectons  Active means dumping lots of records.
        # connections become "inactive" by calling isDone()
        self.NActive = 0
        self.Wait = []

    def isDone(self, P, active):
        if not active:
            # connection closed before activation
            self.Wait.remove(P)
        elif len(self.Wait) > 0:
            # Others are waiting
            P2 = self.Wait.pop(0)
            P2.active = True
            P2.transport.resumeProducing()
            P2.connectionMade()
        else:
            self.NActive -= 1

    def buildProtocol(self, addr):
        active = self.NActive < self.maxActive
        P = self.protocol(active=active)
        P.factory = self
        if active:
            self.NActive += 1
        else:
            self.Wait.append(P)
        return P

    def addClient(self, proto, address):
        S = self.session(proto, address)
        S.factory = self
        return S

    # Note: this method replaced by RecService
    def commit(self, transaction):
        transaction.show()
