# See files LICENSE and COPYRIGHT
# SPDX-License-Identifier: EPICS

import asyncio
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
import logging
from random import randint
import socket
from typing import Dict, Set, Tuple
from weakref import WeakSet

import proto
from .proto import readmsg, protoID
from .conf import ConfigParser, parse_ep

_log = logging.getLogger(__name__)

RecID = int
RecName = RecType = str
Infos = Dict[str, str]

class Op(Enum):
    Update = 0
    Disconnect = 1

@dataclass
class Transaction:
    """Batch of updates (or Disconnect) from a client
    """
    op: Op
    # Ignore remaining if op is Disconnect

    # Connected IP:port
    peer: Tuple[str, int]
    info: Infos = field(default_factory=dict)
    add_record: Dict[RecID, Tuple[RecName, RecType]] = field(default_factory=dict)
    del_record: Set[RecID] = field(default_factory=set)
    record_info: Dict[RecID, Infos] = field(default_factory=defaultdict(dict))

class Recceiver:
    cfg : ConfigParser
    key : int
    announcer : asyncio.Task
    client : Set["ClientHandler"]

    @classmethod
    async def start(klass, cfg: ConfigParser):
        R = klass(cfg)

        _log.debug('Starting %r', R)
        await R.listener()
        await R.announcer()
        _log.debug('Started %r', R)

        return R

    def __init__(self, cfg):
        self.cfg = cfg
        # pick a random key to distinguish this instance
        self.key = randint(0,0xffffffff)
        self.clients = WeakSet()
        self.maxActive = asyncio.Semaphore(int(cfg['maxActive']))
        self.tcptimeout = float(cfg['tcptimeout'])
        self.commitSizeLimit = int(cfg['commitSizeLimit'])
        self.commitInterval = int(cfg['commitInterval'])

    async def close(self):
        _log.debug('Stopping %r', self)

        # first, stop announcing
        self.announcer.cancel()
        try:
            await self.announcer
        except asyncio.CancelledError:
            pass

        # stop accepting new connections
        self.server.close()
        await self.server.wait_closed()

        # close existing connections
        clients, self.clients = set(self.clients), None
        # spoil self.clients because of possible race with pending new_client() callback
        for C in clients:
            C.writer.close()
        for C in clients:
            await C.writer.wait_closed()

        _log.debug('Stopped %r', self)

    def __aenter__(self):
        pass

    def __aexit__(self,A,B,C):
        await self.close()

    async def announcer(self):
        "Start announcer task"

        # digest configuration and prepare before launching Task
        # so that any error is immediate

        announceInterval = float(self.cfg['announceInterval'])

        dests = [parse_ep(ep, defport=5049) for ep in self.cfg['addrlist'].split(',')]

        # bind the same interface as the TCP socket, with a random port
        local_addr = (self.local_addr[0], 0)

        tr, _proto = await asyncio.get_running_loop() \
        .create_datagram_endpoint(asyncio.DatagramProtocol, reuse_address=True,
                                  local_addr=local_addr)

        # since the announcement message is static, prepare it now
        msg = proto.Announce.pack(
            protoID,
            0,
            socket.inet_aton(self.local_addr[0]),
            self.local_addr[1],
            self.key
        )

        self.announcer = asyncio.create_task(self.announcer_loop(dests, tr, msg, announceInterval), "Announcer")

    async def announcer_loop(self, dests, tr, msg, announceInterval):
        try:
            while True:
                _log.debug('Ping')
                for d in dests:
                    try:
                        tr.sendto(msg, d)
                    except: # TODO: ignore / info / warn to reduce error spam (eg. destination unreachable)
                        _log.exception('UDP Send error')

                await asyncio.sleep(announceInterval)
        except:
            _log.exception('Announcer fails')
            raise

    async def listener(self):
        "Start TCP listener"
        local_addr = parse_ep(self.cfg['bind'])

        self.server = await asyncio.start_server(self.new_client,
                                                 host=local_addr[0], port=local_addr[1])

        # find endpoint (w/ port#) actually bound
        self.local_addr = self.server.sockets[0].sockets[0].getsockname()[:2]

    async def new_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        # we are already in a Task
        C = ClientHandler(self, reader, writer)
        self.clients.add(C)
        await C.handle()

@dataclass
class ClientHandler:
    serv : Recceiver
    reader : asyncio.StreamReader
    writer : asyncio.StreamWriter
    peer : Tuple[str, int] = None
    active : Transaction = None
    activeSize : int = field(default=0)

    def __post_init__(self):
        self.peer = self.writer.get_extra_info('peername')
        self.active = Transaction(Op.Update, self.peer)

    async def handle(self):
        try:
            # initially waiting for client greeting
            msg = await readmsg(self.reader, server=False, timeout=self.serv.tcptimeout)
            if not isinstance(msg, proto.ClientGreet):
                raise RuntimeError("Protocol Violation")

            if msg.key!=self.server.key:
                # client acting on an announcement with a different key (maybe we just restarted?)
                _log.warn("Client w/ stale key %s != %s", msg.key, self.server.key)
                self.writer.close()
                yield self.writer.wait_closed()
                return

            # limit the number of clients concurrently dumping
            # to ~bound our resource usage
            with self.server.maxActive:
                # send greeting to provoke client to begin dumping
                self.writer.write(proto.ServerGreet(0).encode())

                while True:
                    msg = await readmsg(self.reader, server=False, timeout=self.serv.tcptimeout)
                    if isinstance(msg, proto.ClientDone):
                        break

                    self.handle_msg(msg)

            while True:
                msg = await readmsg(self.reader, server=False, timeout=self.serv.tcptimeout)
                self.handle_msg(msg)

            while True:
                if not self.active:
                    msg = readmsg(self.reader)

        except:
            _log.exception("Error from %s", self.peer)
            self.writer.close()
            # TODO: commit Transaction(Op.Disconnect)
            raise

    def handle_msg(self, msg: proto.Message):
        if isinstance(msg, proto.ClientAddRecord):
            self.active.add_record[msg.recid] = (msg.rname, msg.rtype)

        elif isinstance(msg, proto.ClientInfo):
            if msg.recid==0:
                self.active.info[msg.key] = msg.val
            else:
                self.active.record_info[msg.recid][msg.key] = msg.val

        else:
            return # ignore unexpected, but valid, messages

        self.activeSize += 1

        if self.commitSizeLimit and self.activeSize>=self.commitSizeLimit:
            pass # TODO: commit now
        elif self.activeSize==1:
            pass # TODO: start commit interval timer
