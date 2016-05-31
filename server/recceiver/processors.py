# -*- coding: utf-8 -*-

from zope.interface import implements

from os.path import expanduser

import traceback

import ConfigParser
from ConfigParser import SafeConfigParser as Parser

from twisted import plugin
from twisted.internet import defer
from twisted.application import service

import interfaces

__all__ = [
    'ShowProcessor',
    'ProcessorFactory',
]

class ConfigAdapter(object):
    def __init__(self, conf, section):
        self._C, self._S = conf, section

    def __len__(self):
        return len(self._C.items(self._S, raw=True))        

    def __contains__(self, key):
        return self._C.has_option(self._S, key)

    def get(self, key, D=None):
        try:
            return self._C.get(self._S, key)
        except ConfigParser.NoOptionError:
            return D

    def __getitem__(self, key):
        try:
            return self._C.get(self._S, key)
        except ConfigParser.NoOptionError:
            raise KeyError('No option value')

class ProcessorController(service.MultiService):
    
    defaults = {}    
    paths = ['/etc/recceiver.conf', '~/.recceiver.conf']
    def __init__(self, cfile=None):
        service.MultiService.__init__(self)
        parser = Parser(self.defaults)

        read = parser.read(map(expanduser, self.paths))

        if cfile:
            parser.readfp(open(cfile,'r'))

        if not cfile and len(read)==0:
            # no user configuration given so load some defaults
            parser.add_section('recceiver')
            parser.set('recceiver', 'procs', 'show')
        elif not parser.has_section('recceiver'):
            parser.add_section('recceiver')

        pnames = parser.get('recceiver', 'procs').split(',')

        plugs = {}
        
        for plug in plugin.getPlugins(interfaces.IProcessorFactory):
            plugs[plug.name] = plug

        self.procs = []

        for P in pnames:
            P = P.strip()
            plugname, _, instname = P.partition(':')
            if not instname:
                instname = plugname

            plug = plugs[plugname]
            
            if not parser.has_section(instname):
                parser.add_section(instname)

            inst = plug.build(instname, ConfigAdapter(parser, instname))

            self.procs.append(inst)
            self.addService(inst)

        self._C = parser

    def config(self, section):
        if not self._C.has_section(section):
            raise KeyError('No section')
        return ConfigAdapter(self._C, section)

    def commit(self, trans):
        defers = []
        bad = []
        
        for P in self.procs:
            try:
                D = P.commit(trans)
                if D:
                    defers.append(D)
            except:
                traceback.print_exc()
                bad.append(P)

        if bad:
            for B in bad:
                print 'Remove plugin',B
                self.procs.remove(B)
        
        if defers:
            return defer.DeferredList(defers)

class ShowProcessor(service.Service):
    implements(interfaces.IProcessor)

    def __init__(self, name, opts):
        self.name = name

    def startService(self):
        service.Service.startService(self)
        print 'Show processor',self.name,'starting'

    def commit(self, transaction):
        print '# From',self.name
        transaction.show()

    def stopService(self):
        service.Service.stopService(self)
        print 'Show processor stopping'

class ProcessorFactory(object):
    implements(plugin.IPlugin, interfaces.IProcessorFactory)

    name = None
    processor = None

    def __init__(self, name, proc):
        self.name, self.processor = name, proc

    def build(self, name, opts):
        P = self.processor(name, opts)
        P.factory = self
        return P
