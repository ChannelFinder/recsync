# See files LICENSE and COPYRIGHT
# SPDX-License-Identifier: EPICS

import asyncio
from dataclasses import dataclass
import logging
import struct
import socket

_log = logging.getLogger(__name__)

# Protocol ID
protoID = 0x5243

## UDP Protocol ##

# (protoid, 0, addr, port, servKey)
Announce = struct.Struct('>HH4sHxxI')
assert Announce.size==16

## TCP Protocol ##

# (protoid, msgid, bodylen)
Header = struct.Struct('>HHI')
assert Header.size==8

class Message:
    id: int
    msg: struct.Struct
    __slots__ = ()

    @classmethod
    def decode(klass, body : bytes):
        return klass(*klass.msg.unpack(body[:klass.msg.size]))

    def encode(self) -> bytes:
        return self.msg.pack(*self)

@dataclass
class ServerPing(Message):
    id = 0x8002
    nonce = int
    msg = struct.Struct('>I')
    assert msg.size==4
    __slots__ = ()

@dataclass
class ClientPong(Message):
    id = 0x0002
    nonce = int
    msg = struct.Struct('>I')
    assert msg.size==4
    __slots__ = ()

@dataclass
class ServerGreet(Message):
    id = 0x8001
    msg = struct.Struct('>B')
    assert msg.size==1
    zero : int
    __slots__ = ()

@dataclass
class ClientGreet(Message):
    id = 0x0001
    msg = struct.Struct('>HxxI')
    assert msg.size==8
    zero : int
    key : int
    __slots__ = ()

@dataclass
class ClientInfo(Message):
    id = 0x0006
    msg = struct.Struct('>IBxH')
    assert msg.size==9
    recid : int
    key : str
    val : str
    __slots__ = ()

    @classmethod
    def decode(klass, body : bytes):
        recid, keylen, vallen = klass.msg.unpack(body[:klass.msg.size])
        key = body[klass.msg.size:klass.msg.size+keylen].decode()
        val = body[klass.msg.size+keylen:klass.msg.size+keylen+vallen].decode()
        return klass(recid, key, val)

    def encode(self) -> bytes:
        return b''.join((
            self.msg.pack(self.recid, len(self.key), len(self.val)),
            self.key,
            self.val,
        ))

@dataclass
class ClientDone(Message):
    id = 0x0005
    __slots__ = ()

    @classmethod
    def decode(klass, body : bytes):
        return klass()

    def encode(self) -> bytes:
        return b''

@dataclass
class ClientAddRecord(Message):
    id = 0x0003
    msg = struct.Struct('>IBBH')
    assert msg.size==8
    recid : int
    atype : int
    rtype : str
    rname : str
    __slots__ = ()

    @classmethod
    def decode(klass, body : bytes):
        recid, atype, tlen, nlen = klass.msg.unpack(body[:klass.msg.size])
        rtype = body[klass.msg.size:klass.msg.size+tlen].decode()
        rname = body[klass.msg.size+tlen:klass.msg.size+tlen+nlen].decode()
        return klass(recid, atype, rtype, rname)

    def encode(self) -> bytes:
        return b''.join((
            self.msg.pack(self.recid, self.atype, len(self.rtype), len(self.rname)),
            self.rtype,
            self.rname,
        ))

@dataclass
class ClientDelRecord(Message):
    id = 0x0001
    msg = struct.Struct('>I')
    assert msg.size==4
    recid : int
    __slots__ = ()

messages = (
    ServerPing,
    ClientPong,
    ServerGreet,
    ClientGreet,
    ClientInfo,
    ClientAddRecord,
    ClientDelRecord,
    ClientDone,
)
messages = {msg.id: msg for msg in messages}

async def _readmsg(reader: asyncio.StreamReader, server=False) -> Message:
    while True:
        ID, msg, blen = Header.unpack(await reader.readexactly(Header.size))
        if ID!=protoID or server ^ (ID >= 0x8000):
            raise RuntimeError("Header error")
        body = await reader.readexactly(blen)

        try:
            Msg = messages[msg]
        except KeyError:
            continue

        return Msg.decode(body)


async def readmsg(reader: asyncio.StreamReader, server=False, timeout=None) -> Message:
    return (await asyncio.wait_for(_readmsg(reader, server), timeout=timeout))

class UDPListener(asyncio.DatagramProtocol):
    def datagram_received(self, data, src):
        ID, zero, addr4, portn, key = Announce.unpack(data[:Announce.size])
        if ID!=protoID or zero!=0:
            return
        addr4 = socket.inet_ntoa(addr4)

        self.announcement(ep=(addr4, portn), key=key)

    def error_received(self, e):
        try:
            raise e
        except Exception:
            _log.exception('UDP Socket Error')
