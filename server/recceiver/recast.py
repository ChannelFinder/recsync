# -*- coding: utf-8 -*-

import logging
_log = logging.getLogger(__name__)

from zope.interface import implements

import struct, collections, random, sys

from twisted.protocols import stateful
from twisted.internet import protocol, reactor

from interfaces import ITransaction

_M = 0x5243

_Head = struct.Struct('>HHI')
assert _Head.size==8

_ping = struct.Struct('>I')
assert _ping.size==4

_s_greet = struct.Struct('>B')
assert _s_greet.size==1

_c_greet = struct.Struct('>BBxxI')
assert _c_greet.size==8

_c_info = struct.Struct('>IBxH')
assert _c_info.size==8

_c_rec = struct.Struct('>IBBH')
assert _c_rec.size==8

class CastReceiver(stateful.StatefulProtocol):

    reactor = reactor
    timeout = 3.0
    version = 0

    def __init__(self, active=True):
        self.sess, self.active = None, active

        self.rxfn = collections.defaultdict(self.dfact)

        self.rxfn[1] = (self.recvClientGreeting, _c_greet.size)
        self.rxfn[2] = (self.recvPong, _ping.size)
        self.rxfn[3] = (self.recvAddRec, _c_rec.size)
        self.rxfn[4] = (self.recvDelRec, _ping.size)
        self.rxfn[5] = (self.recvDone, -1)
        self.rxfn[6] = (self.recvInfo, _c_info.size)

    def writeMsg(self, msgid, body):
        head = _Head.pack(_M, msgid, len(body))
        msg = '%s%s'%(head, body)
        self.transport.write(msg)

    def connectionMade(self):
        if self.active:
            # Full speed ahead
            self.phase = 1
            self.T = self.reactor.callLater(self.timeout, self.timed)
            self.writeMsg(0x8001, _s_greet.pack(self.version))
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

    def restartTimed(self):
        T, self.T = self.T, self.reactor.callLater(self.timeout, self.timed)
        if T and T.active():
            T.cancel()

    def timed(self):
        if self.phase == 2:
            self.transport.loseConnection()
        else:
            self.restartTimed()
            self.phase = 2
            self.nonce = random.randint(0,0xffffffff)
            self.writeMsg(0x8002, _ping.pack(self.nonce))

    def getInitialState(self):
        return (self.recvHeader, 8)

    def recvHeader(self, data):
        self.restartTimed()
        magic, msgid, blen = _Head.unpack(data)
        if magic!=_M:
            _log.error('Protocol error! Bad magic %s',magic)
            self.transport.loseConnection()
            return
        self.msgid = msgid
        fn, minlen = self.rxfn[self.msgid]
        if minlen>=0 and blen < minlen:
            return (self.ignoreBody, blen)
        else:
            return (fn, blen)

    # 0x0001
    def recvClientGreeting(self, body):
        cver, ctype, skey = _c_greet.unpack(body[:_c_greet.size])
        if ctype!=0:
            _log.error("I don't understand you! %s", ctype)
            self.transport.loseConnection()
            return
        self.version = min(self.version, cver)        
        self.clientKey = skey
        self.sess = self.factory.addClient(self, self.transport.getHost())
        return self.getInitialState()

    # 0x0002
    def recvPong(self, body):
        nonce, = _ping.unpack(body[:_ping.size])
        if nonce != self.nonce:
            _log.error('pong nonce does not match! %s!=%s',nonce,self.nonce)
            self.transport.loseConnection()
        else:
            self.phase = 1
        return self.getInitialState()

    # 0x0006
    def recvInfo(self, body):
        rid, klen, vlen = _c_info.unpack(body[:_c_info.size])
        text = body[_c_info.size:]
        if klen==0 or klen+vlen < len(text):
            _log.error('Ignoring info update')
            return self.getInitialState()
        key = text[:klen]
        val = text[klen:klen+vlen]
        if rid:
            self.sess.recInfo(rid, key, val)
        else:
            self.sess.iocInfo(key, val)
        return self.getInitialState()

    # 0x0003
    def recvAddRec(self, body):
        rid, rtype, rtlen, rnlen = _c_rec.unpack(body[:_c_rec.size])
        text = body[_c_rec.size:]
        if rnlen==0 or rtlen+rnlen<len(text):
            _log.error('Ignoring record update')

        elif rtlen>0 and rtype==0:# new record
            rectype = text[:rtlen]
            recname = text[rtlen:rtlen+rnlen]
            self.sess.addRecord(rid, rectype, recname)

        elif rtype==1: # record alias
            recname = text[rtlen:rtlen+rnlen]
            self.sess.addAlias(rid, recname)

        return self.getInitialState()

    # 0x0004
    def recvDelRec(self, body):
        rid = _ping.unpack(body[:_ping.size])
        self.sess.delRecord(rid)
        return self.getInitialState()

    # 0x0005
    def recvDone(self, body):
        self.factory.isDone(self, self.active)
        self.sess.done()
        return self.getInitialState()

    def ignoreBody(self, body):
        return self.getInitialState()

    @classmethod
    def dfact(cls):
        return (cls.ignoreBody, -1)

class Transaction(object):
    implements(ITransaction)
    def __init__(self, ep, id):
        self.connected = True
        self.initial = False
        self.src = ep
        self.srcid = id
        self.addrec, self.infos, self.recinfos = {}, {}, {}
        self.aliases = collections.defaultdict(list)
        self.delrec = set()

    def show(self, fp=sys.stdout):
        if not _log.isEnabledFor(logging.INFO):
            return
        _log.info("# From %s:%d", self.src.host, self.src.port)
        if not self.connected:
            _log.info("#  connection lost")
            return
        for I in self.infos.iteritems():
            _log.info(" epicsEnvSet(\"%s\",\"%s\")", *I)
        for rid, (rname, rtype) in self.addrec.iteritems():
            _log.info(" record(%s, \"%s\") {", rtype, rname)
            for A in self.aliases.get(rid, []):
                _log.info("  alias(\"%s\")", A)
            for I in self.recinfos.get(rid,{}).iteritems():
                _log.info("  info(%s,\"%s\")", *I)
            _log.info(" }")
        _log.info("# End")

class CollectionSession(object):
    timeout = 5.0
    reactor = reactor
    
    def __init__(self, proto, endpoint):
        _log.info("Open session from %s",endpoint)
        self.proto, self.ep = proto, endpoint
        self.TR = Transaction(self.ep, id(self))
        self.TR.initial = True
        self.op = None # in progress commit op
        self.T = None
        self.dirty = False

    def close(self):
        if self.T and self.T.active():
            self.T.cancel()
        op, self.op = self.op, None
        if op:
            op.cancel()

        self.TR.connected = False
        self.factory.commit(self.TR)

    def flush(self, connected=True):
        self.T = None
        if not self.dirty or self.op:
            return

        TR, self.TR = self.TR, Transaction(self.ep, id(self))
        self.dirty = False
        op = self.factory.commit(TR)
        if op:
            op.addCallbacks(self.resume, self.abort)
            self.proto.transport.pauseProducing()
            self.op = op

    def resume(self, arg):
        self.op = None
        self.proto.transport.resumeProducing()

    def abort(self, arg):
        self.op = None
        self.proto.transport.loseConnection()

    def markDirty(self):
        if not self.T:
            self.T = self.reactor.callLater(self.timeout, self.flush)
        self.dirty = True

    def done(self):
        self.flush()

    def iocInfo(self, key, val):
        self.TR.infos[key] = val

        self.markDirty()

    def addRecord(self, rid, rtype, rname):
        self.TR.addrec[rid] = (rname, rtype)

        self.markDirty()

    def addAlias(self, rid, rname):
        self.TR.aliases[rid].append(rname)
        
    def delRecord(self, rid):
        self.TR.addrec.pop(rid, None)
        self.TR.delrec.add(rid)
        self.TR.recinfos.pop(rid, None)

        self.markDirty()

    def recInfo(self, rid, key, val):
        try:
            infos = self.TR.recinfos[rid]
        except KeyError:
            infos = {}
            self.TR.recinfos[rid] = infos

        infos[key] = val

        self.markDirty()

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
        elif len(self.Wait)>0:
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
        if not active:
            self.Wait.append(P)
        return P

    def addClient(self, proto, address):
        S = self.session(proto, address)
        S.factory = self
        return S

    #Note: this method replaced by RecService
    def commit(self, transaction):
        transaction.show()
