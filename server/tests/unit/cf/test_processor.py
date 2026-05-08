import time

import pytest
from requests import RequestException

from recceiver.cf.model import CFChannel, CFProperty, CFPropertyName, PVStatus, RecordInfo
from recceiver.cf.processor import CFProcessor
from tests.unit.cf.conftest import DEFAULT_RECCEIVER_ID, make_channel, make_ioc
from tests.unit.cf.mock_adapter import MockCFAdapter
from tests.unit.conftest import make_adapter


def make_processor() -> CFProcessor:
    return CFProcessor("test", make_adapter())


def make_processor_with_mock():
    proc = CFProcessor("test", make_adapter(values={"recceiverid": DEFAULT_RECCEIVER_ID}))
    adapter = MockCFAdapter()
    proc.client = adapter
    return proc, adapter


class TestRemoveChannel:
    def test_missing_iocid_does_not_raise(self):
        proc = make_processor()
        iocid = make_ioc().id
        proc.channel_ioc_ids["CHAN:1"].append(iocid)
        # iocid deliberately absent from proc.iocs
        proc.remove_channel("CHAN:1", iocid)
        assert "CHAN:1" not in proc.channel_ioc_ids

    def test_missing_iocid_preserves_channel_when_other_iocs_remain(self):
        proc = make_processor()
        iocid = make_ioc().id
        other_iocid = "9.9.9.9:5064"  # NOSONAR
        proc.channel_ioc_ids["CHAN:1"].append(iocid)
        proc.channel_ioc_ids["CHAN:1"].append(other_iocid)
        proc.remove_channel("CHAN:1", iocid)
        assert "CHAN:1" in proc.channel_ioc_ids
        assert other_iocid in proc.channel_ioc_ids["CHAN:1"]

    def test_removes_ioc_when_channelcount_reaches_zero(self):
        proc = make_processor()
        ioc = make_ioc(channelcount=1)
        iocid = ioc.id
        proc.iocs[iocid] = ioc
        proc.channel_ioc_ids["CHAN:1"].append(iocid)
        proc.remove_channel("CHAN:1", iocid)
        assert iocid not in proc.iocs
        assert "CHAN:1" not in proc.channel_ioc_ids

    def test_keeps_ioc_when_channelcount_still_positive(self):
        proc = make_processor()
        ioc = make_ioc(channelcount=2)
        iocid = ioc.id
        proc.iocs[iocid] = ioc
        proc.channel_ioc_ids["CHAN:1"].append(iocid)
        proc.channel_ioc_ids["CHAN:2"].append(iocid)
        proc.remove_channel("CHAN:1", iocid)
        assert iocid in proc.iocs
        assert proc.iocs[iocid].channelcount == 1


class TestCleanService:
    def test_marks_active_channels_inactive(self):
        proc, adapter = make_processor_with_mock()
        adapter.set_channels([make_channel("PV:1"), make_channel("PV:2")])
        proc.clean_service()
        for name in ("PV:1", "PV:2"):
            status = next(p for p in adapter._channels[name].properties if p.name == CFPropertyName.PV_STATUS.value)
            assert status.value == PVStatus.INACTIVE.value

    def test_is_no_op_when_no_active_channels(self):
        proc, _ = make_processor_with_mock()
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
        proc.iocs[ioc.id] = ioc

        proc._update_channelfinder({"PV:1": RecordInfo(pv_name="PV:1")}, [], ioc)

        assert "PV:1" in adapter._channels
        status = next(p for p in adapter._channels["PV:1"].properties if p.name == CFPropertyName.PV_STATUS.value)
        assert status.value == PVStatus.ACTIVE.value

    def test_orphans_channel_absent_from_local_state(self):
        proc, adapter = self._make_proc()
        ioc = make_ioc()
        iocid = ioc.id
        proc.iocs[iocid] = ioc
        # Channel is in CF under this IOC but has no entry in channel_ioc_ids —
        # processor has no record of it, so it should be marked inactive.
        adapter.set_channels(
            [
                CFChannel(
                    "PV:1",
                    "admin",
                    [
                        CFProperty(CFPropertyName.PV_STATUS.value, "admin", PVStatus.ACTIVE.value),
                        CFProperty(CFPropertyName.IOC_ID.value, "admin", iocid),
                    ],
                )
            ]
        )

        proc._update_channelfinder({}, [], ioc)

        status = next(p for p in adapter._channels["PV:1"].properties if p.name == CFPropertyName.PV_STATUS.value)
        assert status.value == PVStatus.INACTIVE.value


class TestPushToCF:
    def test_abandons_push_when_processor_stops_during_retry(self, monkeypatch):
        monkeypatch.setattr(time, "sleep", lambda _: None)

        processor = make_processor()
        processor.running = True
        processor.cf_config.push_always_retry = True

        call_count = 0

        def failing_update(record_info_by_name, records_to_delete, ioc_info):
            nonlocal call_count
            call_count += 1
            processor.running = False
            raise RequestException("CF unreachable")

        monkeypatch.setattr(processor, "_update_channelfinder", failing_update)
        result = processor._push_to_cf({}, [], make_ioc())

        assert result is False
        assert call_count == 1
