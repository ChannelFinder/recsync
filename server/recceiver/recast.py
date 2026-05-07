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

_log = logging.getLogger(__name__)


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
            self.T = self.reactor.callLater(self.timeout, self.writePing)
            self.transport.write(messages.ServerGreeting(self.version).frame())
            self.uploadStart = time.time()
        else:
            # apply brakes
            self.transport.pauseProducing()

    def connectionLost(self, reason=protocol.connectionDone):
        self.factory.isDone(self, self.active)
        if self.T and self.T.active():
            self.T.cancel()
        del self.T
        if self.sess:
            self.sess.close()
        del self.sess

    def restartPingTimer(self):
        T, self.T = self.T, self.reactor.callLater(self.timeout, self.writePing)
        if T and T.active():
            T.cancel()

    def writePing(self):
        if self.phase == 2:
            self.transport.loseConnection()
            _log.debug("pong missed: close connection")
        else:
            self.restartPingTimer()
            self.phase = 2
            self.nonce = random.randint(0, 0xFFFFFFFF)
            self.transport.write(messages.Ping(self.nonce).frame())
            _log.debug("ping nonce: " + str(self.nonce))

    def getInitialState(self):
        return (self.recvHeader, messages.Header.payload.size)

    def recvHeader(self, data):
        self.restartPingTimer()
        try:
            header = messages.Header.decode(data)
        except messages.ProtocolError as exc:
            _log.error(f"Protocol error! {exc}")
            self.transport.loseConnection()
            return
        if header.body_length == 0:
            _log.debug(f"Ignoring empty message {header.msg_id:#06x}")
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
            _log.error(f"Protocol error! {exc}")
            self.transport.loseConnection()
            return
        if greeting.client_type != 0:
            _log.error(f"unsupported client type {greeting.client_type}")
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
            _log.error(f"Protocol error! {exc}")
            self.transport.loseConnection()
            return
        if pong.nonce != self.nonce:
            _log.error(f"pong nonce does not match! {pong.nonce}!={self.nonce}")
            self.transport.loseConnection()
        else:
            _log.debug("pong nonce match")
            self.phase = 1
        return self.getInitialState()

    # 0x0006
    def recvInfo(self, body):
        try:
            info = messages.AddInfo.decode(body)
        except messages.ProtocolError:
            _log.error("Ignoring info update")
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
            _log.error("Ignoring record update")
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
            _log.error("Ignoring delete record update")
            return self.getInitialState()
        self.sess.del_record(record.record_id)
        return self.getInitialState()

    # 0x0005
    def recvDone(self, body):
        try:
            messages.UploadDone.decode(body)
        except messages.ProtocolError:
            _log.error("Ignoring done update")
            return self.getInitialState()
        self.factory.isDone(self, self.active)
        self.sess.done()
        if self.phase == 1:
            self.writePing()

        elapsed_s = time.time() - self.uploadStart
        size_kb = self.uploadSize / 1024
        rate_kbs = size_kb / elapsed_s
        source_address = "{}:{}".format(self.sess.ep.host, self.sess.ep.port)
        _log.info(
            "Done message from {source_address}: uploaded {size_kb}kB in {elapsed_s}s ({rate_kbs}kB/s)".format(
                source_address=source_address,
                size_kb=size_kb,
                elapsed_s=elapsed_s,
                rate_kbs=rate_kbs,
            )
        )

        return self.getInitialState()

    def ignoreBody(self, body):
        return self.getInitialState()

    @classmethod
    def dfact(cls):
        return (cls.ignoreBody, -1)


@implementer(ITransaction)
class Transaction(object):
    def __init__(self, ep, id):
        self.connected = True
        self.initial = False
        self.source_address = ep
        self.srcid = id
        self.records_to_add, self.client_infos, self.record_infos_to_add = {}, {}, {}
        self.aliases = collections.defaultdict(list)
        self.records_to_delete = set()

    def show(self):
        _log.info(str(self))

    def __str__(self):
        source_address = "{}:{}".format(self.source_address.host, self.source_address.port)
        init = self.initial
        conn = self.connected
        nenv = len(self.client_infos)
        nadd = len(self.records_to_add)
        ndel = len(self.records_to_delete)
        ninfo = len(self.record_infos_to_add)
        nalias = len(self.aliases)
        return "Transaction(Src:{}, Init:{}, Conn:{}, Env:{}, Rec:{}, Alias:{}, Info:{}, Del:{})".format(
            source_address, init, conn, nenv, nadd, nalias, ninfo, ndel
        )

    def __repr__(self):
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


class CollectionSession(object):
    timeout = 5.0
    trlimit = 5000

    def __init__(self, proto, endpoint):
        from twisted.internet import reactor

        _log.info("Open session from {endpoint}".format(endpoint=endpoint))
        self.reactor = reactor
        self.proto, self.ep = proto, endpoint
        self.transaction = Transaction(self.ep, id(self))
        self.transaction.initial = True
        self.C = defer.succeed(None)
        self.T = None
        self.dirty = False

    def close(self):
        _log.info("Close session from {ep}".format(ep=self.ep))

        # Do not cancel self.C here. Any data commit that is still queued
        # behind the global lock must be allowed to complete so that channels
        # are registered as active in CF before the disconnect is processed.
        # The disconnect transaction is chained after self.C and will execute
        # once all preceding commits have finished.
        self.transaction = Transaction(self.ep, id(self))
        self.transaction.connected = False
        self.dirty = True
        self.flush()

    def flush(self):
        _log.info("Flush session from {s}".format(s=self.ep))
        self.T = None
        if not self.dirty:
            return

        transaction, self.transaction = self.transaction, Transaction(self.ep, id(self))
        self.dirty = False

        def commit(_ignored):
            _log.info("Commit: {transaction}".format(transaction=transaction))
            return defer.maybeDeferred(self.factory.commit, transaction)

        def abort(err):
            if err.check(defer.CancelledError):
                _log.info("Commit cancelled: {transaction}".format(transaction=transaction))
                return err
            else:
                _log.error("Commit failure: {err}".format(err=err))
                self.proto.transport.loseConnection()
                raise defer.CancelledError()

        self.C.addCallback(commit).addErrback(abort)

    # Flushes must NOT occur at arbitrary points in the data stream
    # because that can result in a PV and its record info or aliases being split
    # between transactions. Only flush after Add or Del or Done message received.
    def flush_safely(self):
        if self.T and self.T <= time.time():
            _log.debug("flush_safely: timeout elapsed for %s", self.ep)
            self.flush()
        elif self.trlimit and self.trlimit <= (
            len(self.transaction.records_to_add) + len(self.transaction.records_to_delete)
        ):
            _log.debug("flush_safely: trlimit %d reached for %s", self.trlimit, self.ep)
            self.flush()

    def mark_dirty(self):
        if not self.T:
            self.T = time.time() + self.timeout
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
