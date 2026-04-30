# -*- coding: utf-8 -*-
"""Framework-neutral RecSync TCP protocol messages."""

import struct
from dataclasses import astuple, dataclass
from enum import IntEnum
from typing import ClassVar

PROTO_ID = 0x5243  # b'RC'


class RecordKind(IntEnum):
    RECORD = 0
    ALIAS = 1


class ProtocolError(ValueError):
    """Raised when a RecSync message violates the wire protocol."""


def _require_length(data, expected_length, message_name, exact=True):
    if exact and len(data) == expected_length:
        return
    if not exact and len(data) >= expected_length:
        return

    relation = "must be" if exact else "must be at least"
    raise ProtocolError(f"{message_name} {relation} {expected_length} bytes, got {len(data)}")


@dataclass(frozen=True)
class Header:
    """TCP frame header.

    Wire layout: PROTO_ID(2) + MSG_ID(2) + BLEN(4).
    """

    payload: ClassVar = struct.Struct("!HHI")

    msg_id: int
    body_length: int

    def encode(self):
        return self.payload.pack(PROTO_ID, int(self.msg_id), self.body_length)

    @classmethod
    def decode(cls, wire_bytes):
        _require_length(wire_bytes, cls.payload.size, cls.__name__)
        proto_id, msg_id, body_length = cls.payload.unpack(wire_bytes)
        if proto_id != PROTO_ID:
            raise ProtocolError(f"bad protocol id {proto_id:#06x}")
        return cls(msg_id, body_length)


assert Header.payload.size == 8


class TCPMessage:
    """Base class for TCP payloads carried by a RecSync frame."""

    payload = struct.Struct("!")
    msg_id = None

    def encode(self):
        return self.payload.pack(*astuple(self))

    def frame(self):
        body = self.encode()
        return Header(self.msg_id, len(body)).encode() + body

    @classmethod
    def decode(cls, body):
        _require_length(body, cls.payload.size, cls.__name__)
        return cls(*cls.payload.unpack(body))


assert TCPMessage.payload.size == 0


class ClientMessage(TCPMessage):
    """Message sent from RecCaster to RecCeiver (msg_id 0x0XXX)."""


class ServerMessage(TCPMessage):
    """Message sent from RecCeiver to RecCaster (msg_id 0x8XXX)."""


@dataclass(frozen=True)
class ClientGreeting(ClientMessage):
    """Client greeting sent on connection establishment.

    Wire layout: VERSION(1) + CLIENT_TYPE(1) + pad(2) + SERV_KEY(4).
    """

    payload: ClassVar = struct.Struct("!BBxxI")
    msg_id: ClassVar = 0x0001

    version: int
    client_type: int
    server_key: int


assert ClientGreeting.payload.size == 8


@dataclass(frozen=True)
class Pong(ClientMessage):
    """Ping response echoing the server's nonce.

    Wire layout: NONCE(4).
    """

    payload: ClassVar = struct.Struct("!I")
    msg_id: ClassVar = 0x0002

    nonce: int


assert Pong.payload.size == 4


@dataclass(frozen=True)
class AddRecord(ClientMessage):
    """Registers a new record or alias.

    Wire layout: RECID(4) + KIND(1) + RTLEN(1) + RNLEN(2) [+ RTYPE(RTLEN) + RNAME(RNLEN)].
    """

    payload: ClassVar = struct.Struct("!IBBH")
    msg_id: ClassVar = 0x0003

    record_id: int
    kind: RecordKind
    record_type: str
    record_name: str

    @property
    def is_alias(self):
        return self.kind == RecordKind.ALIAS

    def encode(self):
        kind = _record_kind(self.kind)
        record_type = self.record_type.encode()
        record_name = self.record_name.encode()
        _validate_record_lengths(kind, len(record_type), len(record_name))
        return (
            self.payload.pack(self.record_id, int(kind), len(record_type), len(record_name)) + record_type + record_name
        )

    @classmethod
    def decode(cls, body):
        _require_length(body, cls.payload.size, cls.__name__, exact=False)
        record_id, kind, record_type_length, record_name_length = cls.payload.unpack(body[: cls.payload.size])
        kind = _record_kind(kind)
        text = body[cls.payload.size :]
        _validate_record_lengths(kind, record_type_length, record_name_length)
        _require_length(text, record_type_length + record_name_length, "add record text")
        record_type = text[:record_type_length].decode()
        record_name = text[record_type_length:].decode()
        return cls(record_id, kind, record_type, record_name)


assert AddRecord.payload.size == 8


@dataclass(frozen=True)
class DelRecord(ClientMessage):
    """Removes a previously registered record.

    Wire layout: RECID(4).
    """

    payload: ClassVar = struct.Struct("!I")
    msg_id: ClassVar = 0x0004

    record_id: int


assert DelRecord.payload.size == 4


@dataclass(frozen=True)
class UploadDone(ClientMessage):
    """Signals the end of the initial record upload.

    Carries a dummy zero payload that is accepted but ignored.
    """

    payload: ClassVar = struct.Struct("!I")
    msg_id: ClassVar = 0x0005

    def encode(self):
        return self.payload.pack(0)

    @classmethod
    def decode(cls, body):
        _require_length(body, cls.payload.size, cls.__name__)
        return cls()


assert UploadDone.payload.size == 4


@dataclass(frozen=True)
class AddInfo(ClientMessage):
    """Key-value info update for a record or the IOC.

    Wire layout: RECID(4) + KEYLEN(1) + pad(1) + VALEN(2) [+ KEY(KEYLEN) + VALUE(VALEN)].
    RECID=0 targets the IOC itself rather than a specific record.
    """

    payload: ClassVar = struct.Struct("!IBxH")
    msg_id: ClassVar = 0x0006

    record_id: int
    key: str
    value: str

    def encode(self):
        key = self.key.encode()
        value = self.value.encode()
        if len(key) == 0:
            raise ProtocolError("add info key must not be empty")
        return self.payload.pack(self.record_id, len(key), len(value)) + key + value

    @classmethod
    def decode(cls, body):
        _require_length(body, cls.payload.size, cls.__name__, exact=False)
        record_id, key_length, value_length = cls.payload.unpack(body[: cls.payload.size])
        text = body[cls.payload.size :]
        if key_length == 0:
            raise ProtocolError("add info key must not be empty")
        _require_length(text, key_length + value_length, "add info text")
        key = text[:key_length].decode()
        value = text[key_length:].decode()
        return cls(record_id, key, value)


assert AddInfo.payload.size == 8


@dataclass(frozen=True)
class ServerGreeting(ServerMessage):
    """Server greeting sent on connection acceptance.

    Wire layout: VERSION(1).
    """

    payload: ClassVar = struct.Struct("!B")
    msg_id: ClassVar = 0x8001

    version: int = 0


assert ServerGreeting.payload.size == 1


@dataclass(frozen=True)
class Ping(ServerMessage):
    """Keepalive ping carrying a random nonce.

    Wire layout: NONCE(4).
    """

    payload: ClassVar = struct.Struct("!I")
    msg_id: ClassVar = 0x8002

    nonce: int


assert Ping.payload.size == 4


def _record_kind(value):
    try:
        return RecordKind(value)
    except ValueError:
        raise ProtocolError(f"unknown record kind {value}")


def _validate_record_lengths(kind, record_type_length, record_name_length):
    if record_name_length == 0:
        raise ProtocolError("add record name must not be empty")
    if kind == RecordKind.RECORD and record_type_length == 0:
        raise ProtocolError("record type must not be empty for records")
    if kind == RecordKind.ALIAS and record_type_length != 0:
        raise ProtocolError("record type must be empty for aliases")
