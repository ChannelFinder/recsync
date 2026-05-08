import struct

import pytest

from recceiver.protocol import announce, messages


def test_encode_server_greeting():
    assert messages.ServerGreeting(0).frame() == struct.pack(
        ">HHIB", messages.PROTO_ID, messages.ServerGreeting.msg_id, 1, 0
    )


def test_encode_ping():
    assert messages.Ping(0x12345678).frame() == struct.pack(
        ">HHII", messages.PROTO_ID, messages.Ping.msg_id, 4, 0x12345678
    )


def test_decode_header_rejects_bad_protocol_id():
    with pytest.raises(messages.ProtocolError):
        messages.Header.decode(struct.pack(">HHI", 0, messages.AddRecord.msg_id, 8))


def test_decode_header_rejects_partial_header():
    with pytest.raises(messages.ProtocolError):
        messages.Header.decode(b"short")


def test_decode_client_greeting():
    greeting = messages.ClientGreeting.decode(struct.pack(">BBxxI", 3, 0, 0xCAFEF00D))

    assert greeting == messages.ClientGreeting(version=3, client_type=0, server_key=0xCAFEF00D)


def test_decode_pong_requires_exact_length():
    with pytest.raises(messages.ProtocolError):
        messages.Pong.decode(struct.pack(">II", 1, 2))


def test_decode_add_record():
    body = struct.pack(">IBBH", 11, 0, 2, 8) + b"aiIOC1:PV1"

    record = messages.AddRecord.decode(body)

    assert record == messages.AddRecord(
        record_id=11,
        kind=messages.RecordKind.RECORD,
        record_type="ai",
        record_name="IOC1:PV1",
    )
    assert not record.is_alias


def test_decode_add_alias():
    body = struct.pack(">IBBH", 11, 1, 0, 14) + b"IOC1:PV1:ALIAS"

    record = messages.AddRecord.decode(body)

    assert record == messages.AddRecord(
        record_id=11,
        kind=messages.RecordKind.ALIAS,
        record_type="",
        record_name="IOC1:PV1:ALIAS",
    )
    assert record.is_alias


@pytest.mark.parametrize(
    "body",
    [
        struct.pack(">IBBH", 11, 0, 0, 8) + b"IOC1:PV1",
        struct.pack(">IBBH", 11, 1, 2, 8) + b"aiIOC1:PV1",
        struct.pack(">IBBH", 11, 2, 2, 8) + b"aiIOC1:PV1",
        struct.pack(">IBBH", 11, 0, 2, 8) + b"ai",
        struct.pack(">IBBH", 11, 0, 2, 0) + b"ai",
    ],
)
def test_decode_add_record_rejects_malformed_body(body):
    with pytest.raises(messages.ProtocolError):
        messages.AddRecord.decode(body)


def test_decode_add_info_for_ioc_property():
    body = struct.pack(">IBxH", 0, 7, 5) + b"iocNameIOC-1"

    info = messages.AddInfo.decode(body)

    assert info == messages.AddInfo(record_id=0, key="iocName", value="IOC-1")


@pytest.mark.parametrize(
    "body",
    [
        struct.pack(">IBxH", 0, 0, 5) + b"IOC-1",
        struct.pack(">IBxH", 0, 7, 5) + b"iocName",
    ],
)
def test_decode_add_info_rejects_malformed_body(body):
    with pytest.raises(messages.ProtocolError):
        messages.AddInfo.decode(body)


def test_decode_delete_record():
    assert messages.DelRecord.decode(struct.pack(">I", 11)) == messages.DelRecord(11)


def test_decode_upload_done_uses_client_dummy_payload():
    assert messages.UploadDone.decode(struct.pack(">I", 0)) == messages.UploadDone()


def test_encode_announce_matches_c_layout():
    assert announce.Announce(tcp_port=1234, key=0xCAFEF00D, host="255.255.255.255").encode() == struct.pack(
        ">HH4sHHI",
        messages.PROTO_ID,
        0,
        b"\xff\xff\xff\xff",
        1234,
        0,
        0xCAFEF00D,
    )


def test_decode_announce():
    packet = struct.pack(">HH4sHHI", messages.PROTO_ID, 0, b"\x7f\x00\x00\x01", 1234, 0, 0xCAFEF00D)

    assert announce.Announce.decode(packet) == announce.Announce(
        tcp_port=1234,
        key=0xCAFEF00D,
        host="127.0.0.1",
    )
