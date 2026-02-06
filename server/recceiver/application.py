from __future__ import annotations

import contextlib
import logging
import secrets
from typing import Any, ClassVar, Protocol

from twisted.application import service
from twisted.internet import defer, pollreactor, reactor
from twisted.internet.error import CannotListenError
from twisted.python import log, usage
from zope.interface import implementer

from twisted import plugin  # type: ignore[attr-defined]

from .announce import Announcer
from .processors import ProcessorController
from .recast import CastFactory
from .udpbcast import SharedUDP

_log = logging.getLogger(__name__)

pollreactor.install()

MAX_PORT = 0xFFFF


class ConfigProto(Protocol):
    def get(self, key: str, D: str | None = None) -> str | None: ...


class Log2Twisted(logging.StreamHandler):
    """Print logging module stream to the twisted log"""

    def __init__(self) -> None:
        super().__init__(stream=self)
        # The Twisted log publisher adds a newline,
        # so strip the newline added by the Python log handler.
        self.terminator = ""
        self.write = log.msg

    def flush(self) -> None:
        pass


class RecService(service.MultiService):
    def __init__(self, config: ConfigProto) -> None:
        self.reactor = reactor

        service.MultiService.__init__(self)
        self.annperiod = float(config.get("announceInterval") or "15.0")
        self.tcptimeout = float(config.get("tcptimeout") or "15.0")
        self.commitperiod = float(config.get("commitInterval") or "5.0")
        self.commitSizeLimit = int(config.get("commitSizeLimit") or "0")
        self.maxActive = int(config.get("maxActive") or "20")
        self.bind, _sep, portn = (config.get("bind") or "").strip().partition(":")
        self.addrlist: list[tuple[str, int]] = []

        self.port = int(portn or "0")

        for raw_addr in (config.get("addrlist") or "").split(","):
            if not raw_addr:
                continue
            addr, _, port_str = raw_addr.strip().partition(":")

            if port_str:
                port = int(port_str)
                if port <= 0 or port > MAX_PORT:
                    msg = "Port numbers must be in the range [1,65535]"
                    raise usage.UsageError(msg)
            else:
                port = 5049

            self.addrlist.append((addr, port))

        if len(self.addrlist) == 0:
            self.addrlist = [("<broadcast>", 5049)]

    def privilegedStartService(self) -> None:
        _log.info("Starting RecService")

        # Start TCP server on random port
        self.tcpFactory = CastFactory()
        self.tcpFactory.protocol.timeout = self.tcptimeout
        self.tcpFactory.session.timeout = self.commitperiod
        self.tcpFactory.session.trlimit = self.commitSizeLimit
        self.tcpFactory.maxActive = self.maxActive

        # Attaching CastFactory to ProcessorController
        self.tcpFactory.commit = self.ctrl.commit  # type: ignore[method-assign]

        self.tcp = self.reactor.listenTCP(self.port, self.tcpFactory, interface=self.bind)
        with contextlib.suppress(CannotListenError):
            self.tcp.startListening()

        # Find out which port is in use
        addr = self.tcp.getHost()
        _log.info(f"RecService listening on {addr}")

        self.key = secrets.randbelow(0x100000000)

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

    def stopService(self) -> defer.DeferredList:
        _log.info("Stopping RecService")

        # This will stop plugin Processors
        D2 = defer.maybeDeferred(service.MultiService.stopService, self)

        U = defer.maybeDeferred(self.udp.stopListening)
        T = defer.maybeDeferred(self.tcp.stopListening)
        return defer.DeferredList([U, T, D2], consumeErrors=True)


class Options(usage.Options):
    optParameters: ClassVar[list[tuple[str, str, Any, str]]] = [
        ("config", "f", None, "Configuration file"),
    ]


@implementer(service.IServiceMaker, plugin.IPlugin)
class Maker:
    tapname = "recceiver"
    description = "RecCaster receiver server"

    options = Options

    def makeService(self, opts: dict[str, Any]) -> RecService:
        ctrl = ProcessorController(cfile=opts["config"])
        conf = ctrl.config("recceiver")
        S = RecService(conf)
        S.addService(ctrl)
        S.ctrl = ctrl

        lvlname = str(conf.get("loglevel") or "WARN")
        lvl = logging.getLevelName(lvlname)
        if not isinstance(lvl, (int,)):
            _log.warning(f"Invalid loglevel {lvlname}. Setting to WARN level instead.")
            lvl = logging.WARNING

        fmt = conf.get("logformat", "%(levelname)s:%(name)s %(message)s")

        handle = Log2Twisted()
        handle.setFormatter(logging.Formatter(fmt))
        root = logging.getLogger()
        root.addHandler(handle)
        root.setLevel(lvl)

        return S
