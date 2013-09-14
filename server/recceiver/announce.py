# -*- coding: utf-8 -*-

from twisted.internet import protocol, reactor
import struct

_Ann = struct.Struct('>HH4sHHI')

__all__ = ['Announcer']

class Announcer(protocol.DatagramProtocol):
    reactor = reactor

    def __init__(self, tcpport, key=0,
                 tcpaddr='\xff\xff\xff\xff',
                 udpaddrs=[('<broadcast>',5049)],
                 period=15.0):
        self.msg = _Ann.pack(0x5243, 0, tcpaddr, tcpport, 0, key)
        self.delay = period
        self.udps = udpaddrs
        self.udpErr = set()
        self.D = None
        if len(self.udps)==0:
            raise RuntimeError('Announce list is empty at start time...')

    def startProtocol(self):
        print 'setup Announcer'
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
                self.transport.write(self.msg, A)
                try:
                    self.udpErr.remove(A)
                    print 'announce OK',A
                except KeyError:
                    pass
            except:
                if A not in self.udpErr:
                    self.udpErr.add(A)
                    import traceback
                    traceback.print_exc()
                    print 'announce Error',A
