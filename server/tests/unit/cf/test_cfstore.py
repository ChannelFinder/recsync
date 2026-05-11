from configparser import ConfigParser

from recceiver.cf.config import CFConfig
from recceiver.cf.model import CFChannel, CFProperty, CFPropertyName, IocInfo, PVStatus, RecordInfo
from recceiver.cf.processor import CFProcessor
from recceiver.processors import ConfigAdapter
from tests.unit.cf.mock_adapter import MockCFAdapter


def make_adapter(section: str = "cf", values: dict = None, env: dict = None) -> ConfigAdapter:
    parser = ConfigParser()
    parser.add_section(section)
    for key, value in (values or {}).items():
        parser.set(section, key, str(value))
    adapter = ConfigAdapter(parser, section)
    if env:
        adapter.env_vars = env
    return adapter


class TestCFConfigLoads:
    def test_loads_defaults_without_error(self):
        adapter = make_adapter()
        config = CFConfig.loads(adapter)
        assert isinstance(config, CFConfig)

    def test_default_push_max_retries(self):
        adapter = make_adapter()
        config = CFConfig.loads(adapter)
        assert config.push_max_retries == 10

    def test_push_max_retries_from_config(self):
        adapter = make_adapter(values={"pushmaxretries": "3"})
        config = CFConfig.loads(adapter)
        assert config.push_max_retries == 3

    def test_push_max_retries_from_env(self):
        adapter = make_adapter(env={"pushmaxretries": "7"})
        config = CFConfig.loads(adapter)
        assert config.push_max_retries == 7

    def test_default_push_always_retry(self):
        adapter = make_adapter()
        config = CFConfig.loads(adapter)
        assert config.push_always_retry is True

    def test_alias_disabled_by_default(self):
        adapter = make_adapter()
        config = CFConfig.loads(adapter)
        assert config.alias_enabled is False

    def test_alias_enabled_from_config(self):
        adapter = make_adapter(values={"alias": "true"})
        config = CFConfig.loads(adapter)
        assert config.alias_enabled is True


RECCEIVER_ID = "test-recceiver"


def make_processor() -> CFProcessor:
    return CFProcessor("test", make_adapter())


def make_processor_with_mock():
    proc = CFProcessor("test", make_adapter(values={"recceiverid": RECCEIVER_ID}))
    adapter = MockCFAdapter()
    proc.client = adapter
    return proc, adapter


def make_active_channel(name: str) -> CFChannel:
    return CFChannel(
        name=name,
        owner="admin",
        properties=[
            CFProperty.active("admin"),
            CFProperty(CFPropertyName.RECCEIVER_ID.value, "admin", RECCEIVER_ID),
        ],
    )


def make_ioc(channelcount: int = 1) -> IocInfo:
    return IocInfo(
        host="1.2.3.4",
        hostname="ioc1.example.com",
        ioc_name="IOC1",
        ioc_ip="1.2.3.4",
        owner="engineer",
        time="2026-01-01T00:00:00",
        port=5064,
        channelcount=channelcount,
    )


class TestRemoveChannel:
    def test_missing_iocid_does_not_raise(self):
        proc = make_processor()
        iocid = "1.2.3.4:5064"
        proc.channel_ioc_ids["CHAN:1"].append(iocid)
        # iocid deliberately absent from proc.iocs
        proc.remove_channel("CHAN:1", iocid)
        assert "CHAN:1" not in proc.channel_ioc_ids

    def test_missing_iocid_preserves_channel_when_other_iocs_remain(self):
        proc = make_processor()
        iocid = "1.2.3.4:5064"
        proc.channel_ioc_ids["CHAN:1"].append(iocid)
        proc.channel_ioc_ids["CHAN:1"].append("9.9.9.9:5064")
        proc.remove_channel("CHAN:1", iocid)
        assert "CHAN:1" in proc.channel_ioc_ids
        assert "9.9.9.9:5064" in proc.channel_ioc_ids["CHAN:1"]

    def test_removes_ioc_when_channelcount_reaches_zero(self):
        proc = make_processor()
        ioc = make_ioc(channelcount=1)
        iocid = ioc.ioc_id
        proc.iocs[iocid] = ioc
        proc.channel_ioc_ids["CHAN:1"].append(iocid)
        proc.remove_channel("CHAN:1", iocid)
        assert iocid not in proc.iocs
        assert "CHAN:1" not in proc.channel_ioc_ids

    def test_keeps_ioc_when_channelcount_still_positive(self):
        proc = make_processor()
        ioc = make_ioc(channelcount=2)
        iocid = ioc.ioc_id
        proc.iocs[iocid] = ioc
        proc.channel_ioc_ids["CHAN:1"].append(iocid)
        proc.channel_ioc_ids["CHAN:2"].append(iocid)
        proc.remove_channel("CHAN:1", iocid)
        assert iocid in proc.iocs
        assert proc.iocs[iocid].channelcount == 1


class TestCleanService:
    def test_marks_active_channels_inactive(self):
        proc, adapter = make_processor_with_mock()
        adapter.set_channels([make_active_channel("PV:1"), make_active_channel("PV:2")])
        proc.clean_service()
        for name in ("PV:1", "PV:2"):
            status = next(p for p in adapter._channels[name].properties if p.name == CFPropertyName.PV_STATUS.value)
            assert status.value == PVStatus.INACTIVE.value

    def test_is_no_op_when_no_active_channels(self):
        proc, adapter = make_processor_with_mock()
        proc.clean_service()


class TestUpdateChannelFinder:
    def _make_proc(self):
        proc, adapter = make_processor_with_mock()
        proc.cancelled = False
        proc.managed_properties = set()
        proc.record_property_names_list = set()
        proc.env_vars = {}
        return proc, adapter

    def test_registers_new_channel_as_active(self):
        proc, adapter = self._make_proc()
        ioc = make_ioc()
        proc.iocs[ioc.ioc_id] = ioc

        proc._update_channelfinder({"PV:1": RecordInfo(pv_name="PV:1")}, [], ioc)

        assert "PV:1" in adapter._channels
        status = next(p for p in adapter._channels["PV:1"].properties if p.name == CFPropertyName.PV_STATUS.value)
        assert status.value == PVStatus.ACTIVE.value

    def test_orphans_channel_absent_from_local_state(self):
        proc, adapter = self._make_proc()
        ioc = make_ioc()
        iocid = ioc.ioc_id
        proc.iocs[iocid] = ioc
        # Channel is in CF under this IOC but has no entry in channel_ioc_ids —
        # processor has no record of it, so it should be marked inactive.
        adapter.set_channels(
            [
                CFChannel(
                    "PV:1",
                    "admin",
                    [
                        CFProperty.active("admin"),
                        CFProperty(CFPropertyName.IOC_ID.value, "admin", iocid),
                    ],
                )
            ]
        )

        proc._update_channelfinder({}, [], ioc)

        status = next(p for p in adapter._channels["PV:1"].properties if p.name == CFPropertyName.PV_STATUS.value)
        assert status.value == PVStatus.INACTIVE.value
