# -*- coding: utf-8 -*-
"""Framework-neutral RecSync UDP announce packet."""

import socket
import struct
from dataclasses import dataclass
from typing import ClassVar

from recceiver.protocol.messages import PROTO_ID, ProtocolError

ANNOUNCE_PORT = 5049
BROADCAST_ADDRESS = "255.255.255.255"


@dataclass(frozen=True)
class Announce:
    """UDP packet advertising the RecCeiver's TCP endpoint.

    Wire layout: PROTO_ID(2) + 0(2) + SERV_ADDR(4) + PORT(2) + pad(2) + SERV_KEY(4).
    """

    payload: ClassVar = struct.Struct("!HH4sHxxI")

    tcp_port: int
    key: int = 0
    host: str = BROADCAST_ADDRESS

    def encode(self):
        try:
            addr = socket.inet_aton(self.host)
        except OSError:
            raise ProtocolError(f"invalid announce address: {self.host}")
        reserved = 0
        return self.payload.pack(PROTO_ID, reserved, addr, self.tcp_port, self.key)

    @classmethod
    def decode(cls, wire_bytes):
        if len(wire_bytes) < cls.payload.size:
            raise ProtocolError(f"announce packet must be at least {cls.payload.size} bytes")
        proto_id, version, addr, tcp_port, key = cls.payload.unpack(wire_bytes[: cls.payload.size])
        if proto_id != PROTO_ID:
            raise ProtocolError(f"bad protocol id {proto_id:#06x}")
        if version != 0:
            raise ProtocolError(f"unsupported announce version {version}")
        return cls(tcp_port=tcp_port, key=key, host=socket.inet_ntoa(addr))


assert Announce.payload.size == 16
