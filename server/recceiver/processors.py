import configparser as ConfigParser
import logging
from configparser import ConfigParser as Parser
from os.path import expanduser

from twisted.application import service
from twisted.internet import defer, task
from zope.interface import implementer

from twisted import plugin

from . import interfaces

_log = logging.getLogger(__name__)

__all__ = [
    "ProcessorFactory",
    "ShowProcessor",
]


class ConfigAdapter:
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
            parser.read_file(open(cfile))

        if not cfile and len(read) == 0:
            # no user configuration given so load some defaults
            parser.add_section("recceiver")
            parser.set("recceiver", "procs", "show")
        elif not parser.has_section("recceiver"):
            parser.add_section("recceiver")

        pnames = parser.get("recceiver", "procs").split(",")

        plugs = {}

        for plug in plugin.getPlugins(interfaces.IProcessorFactory):
            _log.debug(f"Available plugin: {plug.name}")
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

    def commit(self, trans: interfaces.CommitTransaction):
        def punish(err, B):
            if err.check(defer.CancelledError):
                _log.debug(f"Cancel processing: {B.name}: {trans}")
                return err
            try:
                self.procs.remove(B)
                _log.error(f"Remove processor: {B.name}: {err}")
            except ValueError:
                _log.debug(f"Remove processor: {B.name}: aleady removed")
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
        _log.info(f"Show processor '{self.name}' starting")

    def commit(self, transaction: interfaces.CommitTransaction):
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

    def _commit(self, trans: interfaces.CommitTransaction):
        _log.debug(f"# Show processor '{self.name}' commit")
        _log.info(f"# From {trans.source_address.host}:{trans.source_address.port}")
        if not trans.connected:
            _log.info("#  connection lost")
        for item in trans.client_infos.items():
            _log.info(f" epicsEnvSet('{item[0]}','{item[1]}')")
        for record_id, (record_name, record_type) in trans.records_to_add.items():
            _log.info(
                f' record({record_type}, "{record_name}") {{',
            )
            for alias in trans.aliases.get(record_id, []):
                _log.info(f'  alias("{alias}")')
            for item in trans.record_infos_to_add.get(record_id, {}).items():
                _log.info(f'  info({item[0]},"{[1]}")')
            _log.info(" }")
            yield
        _log.info("# End")

    def stopService(self):
        service.Service.stopService(self)
        _log.info(f"Show processor '{self.name}' stopping")


@implementer(plugin.IPlugin, interfaces.IProcessorFactory)
class ProcessorFactory:
    name = None
    processor = None

    def __init__(self, name, processor):
        self.name, self.processor = name, processor

    def build(self, name, opts):
        P = self.processor(name, opts)
        P.factory = self
        return P
