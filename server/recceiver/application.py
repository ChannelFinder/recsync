# -*- coding: utf-8 -*-

import random

from zope.interface import implements

from twisted import plugin
from twisted.python import usage
from twisted.internet import reactor, defer
from twisted.application import service

from recast import CastFactory
from udpbcast import SharedUDP
from announce import Announcer
from processors import ProcessorController


class RecService(service.MultiService):
    reactor = reactor

    def __init__(self, config):
        service.MultiService.__init__(self)
        self.annperiod = float(config.get('announceInterval', '15.0'))
        self.tcptimeout = float(config.get('tcptimeout', '15.0'))
        self.commitperiod = float(config.get('commitInterval', '5.0'))
        self.maxActive = int(config.get('maxActive', '20'))
        self.bind = config.get('bind', '')
        self.addrlist = []

        for addr in config.get('addrlist', '').split(','):
            if not addr:
                continue
            addr,_,port = addr.strip().partition(':')

            if port:
                port = int(port)
                if port<=0 or port>0xffff:
                    raise usage.UsageError('Port numbers must be in the range [1,65535]')
            else:
                port = 5049

            self.addrlist.append((addr, port))

        if len(self.addrlist)==0:
            self.addrlist = [('<broadcast>',5049)]


    def privilegedStartService(self):
        
        print 'Starting'

        # Start TCP server on random port
        self.tcpFactory = CastFactory()
        self.tcpFactory.protocol.timeout = self.tcptimeout
        self.tcpFactory.session.timeout = self.commitperiod
        self.tcpFactory.maxActive = self.maxActive
        
        # Attaching CastFactory to ProcessorController
        self.tcpFactory.commit = self.ctrl.commit

        self.tcp = self.reactor.listenTCP(0, self.tcpFactory,
                                          interface=self.bind)
        self.tcp.startListening()

        # Find out which port is in use
        addr = self.tcp.getHost()
        print 'listening on',addr

        self.key = random.randint(0,0xffffffff)

        # start up the UDP announcer
        self.udpProto = Announcer(tcpport=addr.port, key=self.key,
                                  udpaddrs=self.addrlist,
                                  period=self.annperiod)

        self.udp = SharedUDP(0, self.udpProto, reactor=self.reactor)
        self.udp.startListening()

        # This will start up plugin Processors
        service.MultiService.privilegedStartService(self)

    def stopService(self):
        # This will stop plugin Processors
        D2 = defer.maybeDeferred(service.MultiService.stopService, self)

        U = defer.maybeDeferred(self.udp.stopListening)
        T = defer.maybeDeferred(self.tcp.stopListening)
        return defer.DeferredList([U,T,D2], consumeErrors=True)

class Options(usage.Options):
    optParameters =[
        ("config","f",None,"Configuration file"),
    ]

class Maker(object):
    implements(service.IServiceMaker, plugin.IPlugin)
    tapname = 'recceiver'
    description = 'RecCaster receiver server'

    options = Options

    def makeService(self, opts):
        ctrl = ProcessorController(cfile=opts['config'])
        S = RecService(ctrl.config('recceiver'))
        S.addService(ctrl)
        S.ctrl = ctrl
        return S
