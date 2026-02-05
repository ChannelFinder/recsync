# -*- coding: utf-8 -*-

import logging
import random

from twisted.application import service
from twisted.internet import defer, pollreactor
from twisted.internet.error import CannotListenError
from twisted.python import log, usage
from zope.interface import implementer

from twisted import plugin

from .announce import Announcer
from .processors import ProcessorController
from .recast import CastFactory
from .udpbcast import SharedUDP

_log = logging.getLogger(__name__)

pollreactor.install()


class Log2Twisted(logging.StreamHandler):
    """Print logging module stream to the twisted log"""

    def __init__(self):
        super(Log2Twisted, self).__init__(stream=self)
        # The Twisted log publisher adds a newline,
        # so strip the newline added by the Python log handler.
        self.terminator = ""
        self.write = log.msg

    def flush(self):
        pass


class RecService(service.MultiService):
    def __init__(self, config):
        from twisted.internet import reactor

        self.reactor = reactor

        service.MultiService.__init__(self)
        self.annperiod = float(config.get("announceInterval", "15.0"))
        self.tcptimeout = float(config.get("tcptimeout", "15.0"))
        self.commitperiod = float(config.get("commitInterval", "5.0"))
        self.commitSizeLimit = int(config.get("commitSizeLimit", "0"))
        self.maxActive = int(config.get("maxActive", "20"))
        self.bind, _sep, portn = config.get("bind", "").strip().partition(":")
        self.addrlist = []

        self.port = int(portn or "0")

        for addr in config.get("addrlist", "").split(","):
            if not addr:
                continue
            addr, _, port = addr.strip().partition(":")

            if port:
                port = int(port)
                if port <= 0 or port > 0xFFFF:
                    raise usage.UsageError("Port numbers must be in the range [1,65535]")
            else:
                port = 5049

            self.addrlist.append((addr, port))

        if len(self.addrlist) == 0:
            self.addrlist = [("<broadcast>", 5049)]

    def privilegedStartService(self):
        _log.info("Starting RecService")

        # Start TCP server on random port
        self.tcpFactory = CastFactory()
        self.tcpFactory.protocol.timeout = self.tcptimeout
        self.tcpFactory.session.timeout = self.commitperiod
        self.tcpFactory.session.trlimit = self.commitSizeLimit
        self.tcpFactory.maxActive = self.maxActive

        # Attaching CastFactory to ProcessorController
        self.tcpFactory.commit = self.ctrl.commit

        self.tcp = self.reactor.listenTCP(self.port, self.tcpFactory, interface=self.bind)
        try:
            self.tcp.startListening()
        except CannotListenError:
            # older Twisted required this.
            # newer Twisted errors. sigh...
            pass

        # Find out which port is in use
        addr = self.tcp.getHost()
        _log.info("RecService listening on {addr}".format(addr=addr))

        self.key = random.randint(0, 0xFFFFFFFF)

        # start up the UDP announcer
        self.udpProto = Announcer(
            tcpport=addr.port,
            key=self.key,
            udpaddrs=self.addrlist,
            period=self.annperiod,
        )

        self.udp = SharedUDP(self.port, self.udpProto, reactor=self.reactor, interface=self.bind)
        self.udp.startListening()

        # This will start up plugin Processors
        service.MultiService.privilegedStartService(self)

    def stopService(self):
        _log.info("Stopping RecService")

        # This will stop plugin Processors
        D2 = defer.maybeDeferred(service.MultiService.stopService, self)

        U = defer.maybeDeferred(self.udp.stopListening)
        T = defer.maybeDeferred(self.tcp.stopListening)
        return defer.DeferredList([U, T, D2], consumeErrors=True)


class Options(usage.Options):
    optParameters = [
        ("config", "f", None, "Configuration file"),
    ]


@implementer(service.IServiceMaker, plugin.IPlugin)
class Maker(object):
    tapname = "recceiver"
    description = "RecCaster receiver server"

    options = Options

    def makeService(self, opts):
        ctrl = ProcessorController(cfile=opts["config"])
        conf = ctrl.config("recceiver")
        S = RecService(conf)
        S.addService(ctrl)
        S.ctrl = ctrl

        lvlname = conf.get("loglevel", "WARN")
        lvl = logging.getLevelName(lvlname)
        if not isinstance(lvl, (int,)):
            print("Invalid loglevel {}. Setting to WARN level instead.".format(lvlname))
            lvl = logging.WARN

        fmt = conf.get("logformat", "%(levelname)s:%(name)s %(message)s")

        handle = Log2Twisted()
        handle.setFormatter(logging.Formatter(fmt))
        root = logging.getLogger()
        root.addHandler(handle)
        root.setLevel(lvl)

        return S
