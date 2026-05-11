from recceiver.cf.model import CFChannel, CFProperty, CFPropertyName, IOCInfo, PVStatus


def make_ioc(channelcount: int = 1) -> IOCInfo:
    return IOCInfo(
        host="1.2.3.4",  # NOSONAR
        hostname="ioc1.example.com",
        ioc_name="IOC1",
        ioc_ip="1.2.3.4",  # NOSONAR
        owner="engineer",
        time="2026-01-01T00:00:00",
        port=5064,
        channelcount=channelcount,
    )


def make_channel(name: str, recceiver_id: str = "test-recceiver", active: bool = True) -> CFChannel:
    status = PVStatus.ACTIVE if active else PVStatus.INACTIVE
    return CFChannel(
        name=name,
        owner="admin",
        properties=[
            CFProperty(CFPropertyName.PV_STATUS.value, "admin", status.value),
            CFProperty(CFPropertyName.RECCEIVER_ID.value, "admin", recceiver_id),
        ],
    )
