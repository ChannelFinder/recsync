# -*- coding: utf-8 -*-

import configparser as ConfigParser
import logging
from configparser import ConfigParser as Parser
from os.path import expanduser

from zope.interface import implementer

from twisted import plugin
from twisted.application import service
from twisted.internet import defer, task

from . import interfaces

_log = logging.getLogger(__name__)

__all__ = [
    "ShowProcessor",
    "ProcessorFactory",
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

    def getboolean(self, key, D=None):
        try:
            return self._C.getboolean(self._S, key)
        except (ConfigParser.NoOptionError, ValueError):
            return D

    def __getitem__(self, key):
        try:
            return self._C.get(self._S, key)
        except ConfigParser.NoOptionError:
            raise KeyError("No option value")


class ProcessorController(service.MultiService):
    defaults = {}
    paths = ["/etc/recceiver.conf", "~/.recceiver.conf"]

    def __init__(self, cfile=None):
        service.MultiService.__init__(self)
        parser = Parser(self.defaults)

        read = parser.read(map(expanduser, self.paths))

        if cfile:
            parser.read_file(open(cfile, "r"))

        if not cfile and len(read) == 0:
            # no user configuration given so load some defaults
            parser.add_section("recceiver")
            parser.set("recceiver", "procs", "show")
        elif not parser.has_section("recceiver"):
            parser.add_section("recceiver")

        pnames = parser.get("recceiver", "procs").split(",")

        plugs = {}

        for plug in plugin.getPlugins(interfaces.IProcessorFactory):
            _log.debug("Available plugin: {name}".format(name=plug.name))
            plugs[plug.name] = plug

        self.procs = []

        for P in pnames:
            P = P.strip()
            plugname, _, instname = P.partition(":")
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
            raise KeyError("No section")
        return ConfigAdapter(self._C, section)

    def commit(self, trans):
        def punish(err, B):
            if err.check(defer.CancelledError):
                _log.debug("Cancel processing: {name}: {trans}".format(name=B.name, trans=trans))
                return err
            try:
                self.procs.remove(B)
                _log.error("Remove processor: {name}: {err}".format(name=B.name, err=err))
            except ValueError:
                _log.debug("Remove processor: {name}: aleady removed".format(name=B.name))
            return err

        defers = [defer.maybeDeferred(P.commit, trans).addErrback(punish, P) for P in self.procs]

        def findFirstError(result_list):
            for success, result in result_list:
                if not success:
                    return result

        return defer.DeferredList(defers, consumeErrors=True).addCallback(findFirstError)


@implementer(interfaces.IProcessor)
class ShowProcessor(service.Service):
    def __init__(self, name, opts):
        self.name = name
        self.lock = defer.DeferredLock()

    def startService(self):
        service.Service.startService(self)
        _log.info("Show processor '{processor}' starting".format(processor=self.name))

    def commit(self, transaction):
        def withLock(_ignored):
            # Why doesn't coiterate() just handle cancellation!?
            t = task.cooperate(self._commit(transaction))
            d = defer.Deferred(lambda d: t.stop())
            t.whenDone().chainDeferred(d)
            d.addErrback(stopToCancelled)
            d.addBoth(releaseLock)
            return d

        def stopToCancelled(err):
            if err.check(task.TaskStopped):
                raise defer.CancelledError()
            return err

        def releaseLock(result):
            self.lock.release()
            return result

        return self.lock.acquire().addCallback(withLock)

    def _commit(self, trans):
        _log.debug("# Show processor '{name}' commit".format(name=self.name))
        _log.info("# From {host}:{port}".format(host=trans.src.host, port=trans.src.port))
        if not trans.connected:
            _log.info("#  connection lost")
        for item in trans.infos.items():
            _log.info(" epicsEnvSet('{name}','{value}')".format(name=item[0], value=item[1]))
        for rid, (rname, rtype) in trans.addrec.items():
            _log.info(' record({rtype}, "{rname}") {{'.format(rtype=rtype, rname=rname))
            for alias in trans.aliases.get(rid, []):
                _log.info('  alias("{alias}")'.format(alias=alias))
            for item in trans.recinfos.get(rid, {}).items():
                _log.info('  info({name},"{value}")'.format(name=item[0], value=[1]))
            _log.info(" }")
            yield
        _log.info("# End")

    def stopService(self):
        service.Service.stopService(self)
        _log.info("Show processor '{name}' stopping".format(name=self.name))


@implementer(plugin.IPlugin, interfaces.IProcessorFactory)
class ProcessorFactory(object):
    name = None
    processor = None

    def __init__(self, name, processor):
        self.name, self.processor = name, processor

    def build(self, name, opts):
        P = self.processor(name, opts)
        P.factory = self
        return P
