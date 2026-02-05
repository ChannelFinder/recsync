import logging
import struct
import sys

from twisted.internet import protocol
from twisted.internet.error import MessageLengthError

_log = logging.getLogger(__name__)


_Ann = struct.Struct(">HH4sHHI")

__all__ = ["Announcer"]


class Announcer(protocol.DatagramProtocol):
    def __init__(
        self,
        tcpport,
        key=0,
        tcpaddr="\xff\xff\xff\xff",
        udpaddrs=[("<broadcast>", 5049)],
        period=15.0,
    ):
        from twisted.internet import reactor

        self.reactor = reactor

        if sys.version_info[0] < 3:
            self.msg = _Ann.pack(0x5243, 0, tcpaddr, tcpport, 0, key)
        else:
            self.msg = _Ann.pack(0x5243, 0, tcpaddr.encode("latin-1"), tcpport, 0, key)

        self.delay = period
        self.udps = udpaddrs
        self.udpErr = set()
        self.D = None
        if len(self.udps) == 0:
            raise RuntimeError("Announce list is empty at start time...")

    def startProtocol(self):
        _log.info("Setup Announcer")
        self.D = self.reactor.callLater(0, self.sendOne)
        # we won't process any receieved traffic, so no reason to wake
        # up for it...
        self.transport.pauseProducing()

    def stopProtocol(self):
        _log.info("Stop Announcer")
        self.D.cancel()
        del self.D

    def datagramReceived(self, source_address):
        pass  # ignore

    def sendOne(self):
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
            except MessageLengthError:
                if A not in self.udpErr:
                    self.udpErr.add(A)
                    _log.exception(f"announce Error to {A}")
