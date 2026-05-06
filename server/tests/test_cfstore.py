from collections import defaultdict
from configparser import ConfigParser

from recceiver.cfstore import CFConfig, CFProcessor, IocInfo
from recceiver.processors import ConfigAdapter


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


def make_processor() -> CFProcessor:
    return CFProcessor("test", make_adapter())


def make_ioc(channelcount: int = 1) -> IocInfo:
    return IocInfo(
        host="1.2.3.4",
        hostname="ioc1.example.com",
        ioc_name="IOC1",
        ioc_IP="1.2.3.4",
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
