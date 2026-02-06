import logging
import struct
from typing import Any

from twisted.internet import protocol
from twisted.internet.error import MessageLengthError

_log = logging.getLogger(__name__)


_Ann = struct.Struct(">HH4sHHI")

__all__ = ["Announcer"]


class Announcer(protocol.DatagramProtocol):
    def __init__(
        self,
        tcpport: int,
        key: int = 0,
        tcpaddr: str = "\xff\xff\xff\xff",
        udpaddrs: list[tuple[str, int]] | None = None,
        period: float = 15.0,
    ) -> None:
        from twisted.internet import (  # noqa: PLC0415
            reactor,  # importing reactor does more than just import it, so we want to delay this until we need it
        )

        self.reactor = reactor

        self.msg = _Ann.pack(0x5243, 0, tcpaddr.encode("latin-1"), tcpport, 0, key)

        self.delay = period
        self.udps = udpaddrs or [("<broadcast>", 5049)]
        self.udpErr: set[tuple[str, int]] = set()
        self.D: Any = None
        if len(self.udps) == 0:
            msg = "Announce list is empty at start time..."
            raise RuntimeError(msg)

    def startProtocol(self) -> None:
        _log.info("Setup Announcer")
        self.D = self.reactor.callLater(0, self.sendOne)
        # we won't process any receieved traffic, so no reason to wake
        # up for it...
        self.transport.pauseProducing()

    def stopProtocol(self) -> None:
        _log.info("Stop Announcer")
        self.D.cancel()
        del self.D

    def datagramReceived(self, source_address: tuple[str, int]) -> None:
        pass  # ignore

    def sendOne(self) -> None:
        self.D = self.reactor.callLater(self.delay, self.sendOne)
        for A in self.udps:
            try:
                _log.debug(f"announce to {A}")
                self.transport.write(self.msg, A)
                try:
                    self.udpErr.remove(A)
                    _log.warning(f"announce OK to {A}")
                except KeyError:
                    pass
            except MessageLengthError:  # noqa: PERF203
                if A not in self.udpErr:
                    self.udpErr.add(A)
                    _log.exception(f"announce Error to {A}")
