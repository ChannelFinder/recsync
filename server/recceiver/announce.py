# -*- coding: utf-8 -*-

import logging

from twisted.internet import protocol
from twisted.internet.error import MessageLengthError

from recceiver.protocol.announce import ANNOUNCE_PORT, BROADCAST_ADDRESS, Announce

_log = logging.getLogger(__name__)


__all__ = ["Announcer"]


class Announcer(protocol.DatagramProtocol):
    def __init__(
        self,
        tcpport,
        key=0,
        host=BROADCAST_ADDRESS,
        udpaddrs=None,
        period=15.0,
    ):
        from twisted.internet import reactor

        self.reactor = reactor

        if udpaddrs is None:
            udpaddrs = [("<broadcast>", ANNOUNCE_PORT)]

        self.msg = Announce(tcp_port=tcpport, key=key, host=host).encode()
        self.delay = period
        self.udps = udpaddrs
        self.udpErr = set()
        self.D = None
        if len(self.udps) == 0:
            raise RuntimeError("Announce list is empty at start time...")

    def startProtocol(self):
        _log.info("Setup Announcer")
        self.D = self.reactor.callLater(0, self.sendOne)
        # we won't process any received traffic, so no reason to wake
        # up for it...
        self.transport.pauseProducing()

    def stopProtocol(self):
        _log.info("Stop Announcer")
        self.D.cancel()
        del self.D

    def datagramReceived(self, datagram, addr):
        pass  # ignore

    def sendOne(self):
        self.D = self.reactor.callLater(self.delay, self.sendOne)
        for A in self.udps:
            try:
                _log.debug("announce to {s}".format(s=A))
                self.transport.write(self.msg, A)
                try:
                    self.udpErr.remove(A)
                    _log.warning("announce OK to {s}".format(s=A))
                except KeyError:
                    pass
            except MessageLengthError:
                if A not in self.udpErr:
                    self.udpErr.add(A)
                    _log.exception("announce Error to {s}".format(s=A))
