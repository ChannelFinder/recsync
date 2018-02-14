# -*- coding: utf-8 -*-

import sys
import struct

from twisted.internet import protocol, reactor
import logging

_log = logging.getLogger(__name__)


_Ann = struct.Struct('>HH4sHHI')

__all__ = ['Announcer']

class Announcer(protocol.DatagramProtocol):
    reactor = reactor

    def __init__(self, tcpport, key=0,
                 tcpaddr='\xff\xff\xff\xff',
                 udpaddrs=[('<broadcast>',5049)],
                 period=15.0):
        if sys.version_info[0] < 3:
            self.msg = _Ann.pack(0x5243, 0, tcpaddr, tcpport, 0, key)
        else:
            self.msg = _Ann.pack(0x5243, 0, tcpaddr.encode('latin-1'), tcpport, 0, key)

        self.delay = period
        self.udps = udpaddrs
        self.udpErr = set()
        self.D = None
        if len(self.udps)==0:
            raise RuntimeError('Announce list is empty at start time...')

    def startProtocol(self):
        _log.info('setup Announcer')
        self.D = self.reactor.callLater(0, self.sendOne)
        # we won't process any receieved traffic, so no reason to wake
        # up for it...
        self.transport.pauseProducing()

    def stopProtocol(self):
        self.D.cancel()
        del self.D
    
    def datagramReceived(self, src):
        pass # ignore

    def sendOne(self):
        self.D = self.reactor.callLater(self.delay, self.sendOne)
        for A in self.udps:
            try:
                _log.debug('announce to %s',A)
                self.transport.write(self.msg, A)
                try:
                    self.udpErr.remove(A)
                    _log.warn('announce OK to %s',A)
                except KeyError:
                    pass
            except:
                if A not in self.udpErr:
                    self.udpErr.add(A)
                    _log.exception('announce Error to %s',A)
