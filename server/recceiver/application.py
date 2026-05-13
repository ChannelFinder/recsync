# -*- coding: utf-8 -*-

import logging
import random

from twisted.application import service
from twisted.internet import defer, pollreactor, task
from twisted.internet.error import CannotListenError
from twisted.python import log as twisted_log
from twisted.python import usage
from zope.interface import implementer

from twisted import plugin

from . import metrics
from .announcer import Announcer, SharedUDP
from .processors import ProcessorController
from .recast import CastFactory

log = logging.getLogger(__name__)


class Log2Twisted(logging.StreamHandler):
    """Print logging module stream to the twisted log"""

    def __init__(self):
        super(Log2Twisted, self).__init__(stream=self)
        # The Twisted log publisher adds a newline,
        # so strip the newline added by the Python log handler.
        self.terminator = ""
        self.write = twisted_log.msg

    def flush(self):
        # Required by logging.StreamHandler; this handler writes directly to Twisted logs.
        return None


class RecService(service.MultiService):
    def __init__(self, config):
        from twisted.internet import reactor

        self.reactor = reactor

        service.MultiService.__init__(self)
        self._statusLoop = None
        self.annperiod = float(config.get("announceInterval", "15.0"))
        self.tcptimeout = float(config.get("tcptimeout", "15.0"))
        self.commitperiod = float(config.get("commitInterval", "5.0"))
        self.commitSizeLimit = int(config.get("commitSizeLimit", "0"))
        self.maxActive = int(config.get("maxActive", "20"))
        self.bind, _sep, portn = config.get("bind", "").strip().partition(":")
        self.addrlist = []

        self.port = int(portn or "0")
        self.statusInterval = float(config.get("statusInterval", "60.0"))
        self.metricsPort = int(config.get("metricsPort", "0"))

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
        log.info("Starting RecService")

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
        log.info("RecService listening on %s", addr)

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

        if self.metricsPort > 0:
            if metrics.available:
                self.reactor.listenTCP(self.metricsPort, metrics.make_site(), interface=self.bind)
                log.info("Prometheus metrics available on port %d", self.metricsPort)
            else:
                log.warning("metricsPort configured but prometheus_client is not installed; metrics disabled")

        metrics.connections_limit.set(self.tcpFactory.maxActive)

        if self.statusInterval > 0:
            self._statusLoop = task.LoopingCall(self._logStatus)
            self._statusLoop.start(self.statusInterval, now=False)

    def _logStatus(self):
        metrics.connections_active.set(self.tcpFactory.NActive)
        metrics.connections_waiting.set(len(self.tcpFactory.Wait))
        log.info(
            "status: connections active=%d/%d queued=%d",
            self.tcpFactory.NActive,
            self.tcpFactory.maxActive,
            len(self.tcpFactory.Wait),
        )

    def stopService(self):
        log.info("Stopping RecService")

        if self._statusLoop is not None and self._statusLoop.running:
            self._statusLoop.stop()

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

    def make_service(self, opts):
        pollreactor.install()
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

    def makeService(self, opts):  # NOSONAR - Twisted IServiceMaker API requires this exact name.
        return self.make_service(opts)
