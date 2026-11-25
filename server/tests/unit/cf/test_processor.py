import logging
import time
from dataclasses import replace

import pytest
from requests import RequestException
from twisted.internet import defer
from twisted.internet.address import IPv4Address

from recceiver.cf.model import CFChannel, CFProperty, CFPropertyName, PVStatus, RecordInfo
from recceiver.cf.processor import _SHARED_CHANNELS_SAMPLE_SIZE, CFProcessor
from recceiver.recast import Transaction
from tests.unit.cf.conftest import DEFAULT_RECCEIVER_ID, make_ioc
from tests.unit.cf.mock_adapter import MockCFAdapter
from tests.unit.conftest import make_adapter

_HOST_A = "1.2.3.4"  # NOSONAR


def make_transaction(host: str, port: int) -> Transaction:
    ep = IPv4Address("TCP", host, port)
    t = Transaction(ep, id=0)
    t.initial = True
    return t


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


class TestStartupCleanGate:
    def test_commit_waits_for_startup_clean(self, monkeypatch):
        proc = make_processor()
        proc.running = True
        transaction = make_transaction(_HOST_A, 5064)
        startup_clean = defer.Deferred()
        proc._startup_clean = startup_clean
        committed = []
        monkeypatch.setattr(proc, "_commit_with_lock", lambda tx: committed.append(tx))

        result = proc.commit(transaction)
        completed = []
        result.addCallback(completed.append)
        assert committed == []
        assert completed == []

        startup_clean.callback(None)

        assert committed == [transaction]
        assert completed == [None]

    def test_cancel_while_waiting_skips_commit_without_cancelling_startup(self, monkeypatch):
        proc = make_processor()
        proc.running = True
        startup_clean = defer.Deferred()
        proc._startup_clean = startup_clean
        committed = []
        monkeypatch.setattr(proc, "_commit_with_lock", lambda tx: committed.append(tx))
        transaction = make_transaction(_HOST_A, 5064)

        cancelled = proc.commit(transaction)
        errors = []
        cancelled.addErrback(errors.append)
        cancelled.cancel()
        assert errors[0].check(defer.CancelledError)
        assert not startup_clean.called

        following = proc.commit(transaction)
        completed = []
        following.addCallback(completed.append)
        startup_clean.callback(None)

        assert committed == [transaction]
        assert completed == [None]

    def test_cancel_while_waiting_for_lock_removes_queued_commit(self, monkeypatch):
        proc = make_processor()
        proc.running = True
        proc._startup_clean = defer.succeed(None)
        proc.lock.acquire()
        committed = []
        monkeypatch.setattr(proc, "_commit_with_lock", lambda tx: committed.append(tx))

        result = proc.commit(make_transaction(_HOST_A, 5064))
        errors = []
        result.addErrback(errors.append)
        result.cancel()
        proc.lock.release()

        assert errors[0].check(defer.CancelledError)
        assert committed == []
        assert not proc.lock.locked

    def test_cancel_active_commit_waits_for_worker_and_releases_lock(self, monkeypatch):
        proc = make_processor()
        proc.running = True
        proc._startup_clean = defer.succeed(None)
        worker = defer.Deferred()
        monkeypatch.setattr("recceiver.cf.processor.deferToThread", lambda *args: worker)

        result = proc.commit(make_transaction(_HOST_A, 5064))
        errors = []
        result.addErrback(errors.append)
        result.cancel()

        assert proc.cancelled
        assert proc.lock.locked
        assert not worker.called
        assert errors == []

        worker.callback(None)

        assert errors[0].check(defer.CancelledError)
        assert not proc.lock.locked


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

    def test_handle_channel_is_old_missing_last_ioc_does_not_raise(self):
        """If the last known IOC for a channel has departed, orphan the channel rather than raise."""
        from recceiver.cf.model import IOCInfo

        proc, adapter = self._make_proc()

        ioc_a = IOCInfo(
            host="1.2.3.4",  # NOSONAR
            hostname="ioc-a.example.com",
            ioc_name="IOC-A",
            ioc_ip="1.2.3.4",  # NOSONAR
            owner="admin",
            time="2026-01-01T00:00:00",
            port=5064,
            channelcount=0,
        )
        ioc_b = IOCInfo(
            host="5.6.7.8",  # NOSONAR
            hostname="ioc-b.example.com",
            ioc_name="IOC-B",
            ioc_ip="5.6.7.8",  # NOSONAR
            owner="admin",
            time="2026-01-01T00:00:00",
            port=5064,
            channelcount=0,
        )
        ioc_a_id = ioc_a.id  # "1.2.3.4:5064"
        ioc_b_id = ioc_b.id  # "5.6.7.8:5064"

        # PV:1 was last seen under IOC-A, which has since departed
        proc.channel_ioc_ids["PV:1"].append(ioc_a_id)
        # ioc_a deliberately absent from proc.iocs
        proc.iocs[ioc_b_id] = ioc_b

        # CF has PV:1 registered under IOC-B (by iocid property)
        adapter.set_channels(
            [
                CFChannel(
                    "PV:1",
                    "admin",
                    [
                        CFProperty(CFPropertyName.PV_STATUS.value, "admin", PVStatus.ACTIVE.value),
                        CFProperty(CFPropertyName.IOC_ID.value, "admin", ioc_b_id),
                    ],
                )
            ]
        )

        # IOC-B commits with no channels — PV:1 appears in old_channels, triggers _handle_channel_is_old
        proc._update_channelfinder({}, [], ioc_b)

        # Must not raise KeyError; PV:1 should be orphaned (marked Inactive)
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

    def test_gives_up_after_max_retries(self, monkeypatch):
        monkeypatch.setattr(time, "sleep", lambda _: None)

        processor = make_processor()
        processor.running = True
        processor.cf_config.push_always_retry = False
        processor.cf_config.push_max_retries = 2

        call_count = 0

        def failing_update(record_info_by_name, records_to_delete, ioc_info):
            nonlocal call_count
            call_count += 1
            raise RequestException("CF unreachable")

        monkeypatch.setattr(processor, "_update_channelfinder", failing_update)
        result = processor._push_to_cf({}, [], make_ioc())

        assert result is False
        assert call_count == 2

    def test_cancelled_flag_raises_cancelled_error(self):
        processor = make_processor()
        processor.running = True
        processor.cancelled = True
        ioc = make_ioc()
        processor.iocs[ioc.id] = ioc

        with pytest.raises(defer.CancelledError):
            processor._push_to_cf({}, [], ioc)


class TestSharedChannelsLog:
    _PHRASE = "also held by another IOC"
    _EARLIER = "only by an earlier connection of the same IOC"

    def _setup(self):
        proc = make_processor()
        ioc_a = make_ioc()
        ioc_b = replace(make_ioc(), host="5.6.7.8", ioc_name="IOC2")  # NOSONAR
        proc.iocs[ioc_a.id] = ioc_a
        return proc, ioc_a, ioc_b

    def _hold(self, proc, iocid, names):
        for name in names:
            proc.channel_ioc_ids[name].append(iocid)

    def _commit(self, proc, ioc, names, aliases=None, initial=True):
        t = make_transaction(ioc.host, ioc.port)
        t.initial = initial
        infos = {n: RecordInfo(pv_name=n, aliases=(aliases or {}).get(n, [])) for n in names}
        proc.update_ioc_infos(t, ioc, [], infos)

    def _records(self, caplog):
        return [r for r in caplog.records if self._PHRASE in r.getMessage() or self._EARLIER in r.getMessage()]

    def _messages(self, caplog):
        return [r.getMessage() for r in self._records(caplog)]

    def test_logs_one_warning_with_count_and_sample(self, caplog):
        proc, ioc_a, ioc_b = self._setup()
        self._hold(proc, ioc_a.id, ["PV:1", "PV:2"])

        self._commit(proc, ioc_b, ["PV:1", "PV:2", "PV:3"])

        (record,) = self._records(caplog)
        message = record.getMessage()
        assert record.levelname == "WARNING"
        assert ioc_b.id in message
        assert "2 channels also held by another IOC, 0 only by an earlier connection" in message
        assert f"PV:1 (IOC1@{ioc_a.id})" in message
        assert "PV:3" not in message

    def test_logs_nothing_when_no_channel_is_shared(self, caplog):
        proc, ioc_a, ioc_b = self._setup()
        self._hold(proc, ioc_a.id, ["PV:1"])

        self._commit(proc, ioc_b, ["PV:2"])

        assert self._messages(caplog) == []

    def test_logs_nothing_for_names_the_ioc_already_holds(self, caplog):
        proc, ioc_a, ioc_b = self._setup()
        self._hold(proc, ioc_a.id, ["PV:1"])
        self._commit(proc, ioc_b, ["PV:1"])
        caplog.clear()

        self._commit(proc, ioc_b, ["PV:1"], initial=False)

        assert self._messages(caplog) == []

    def test_logs_once_per_batch(self, caplog):
        proc, ioc_a, ioc_b = self._setup()
        self._hold(proc, ioc_a.id, ["PV:1", "PV:2"])

        self._commit(proc, ioc_b, ["PV:1"])
        self._commit(proc, ioc_b, ["PV:2"], initial=False)

        messages = self._messages(caplog)
        assert len(messages) == 2
        assert "PV:1" in messages[0]
        assert "PV:2" in messages[1]

    def test_caps_sample(self, caplog):
        proc, ioc_a, ioc_b = self._setup()
        names = [f"PV:{i}" for i in range(_SHARED_CHANNELS_SAMPLE_SIZE + 5)]
        self._hold(proc, ioc_a.id, names)

        self._commit(proc, ioc_b, names)

        (message,) = self._messages(caplog)
        assert f"{len(names)} channels also held by another IOC" in message
        assert message.count(ioc_a.id) == _SHARED_CHANNELS_SAMPLE_SIZE

    def test_includes_aliases_when_enabled(self, caplog):
        proc, ioc_a, ioc_b = self._setup()
        proc.cf_config.alias_enabled = True
        self._hold(proc, ioc_a.id, ["ALIAS:1"])

        self._commit(proc, ioc_b, ["PV:1"], aliases={"PV:1": ["ALIAS:1"]})

        (message,) = self._messages(caplog)
        assert "1 channels also held by another IOC" in message
        assert "ALIAS:1" in message

    def test_ignores_aliases_when_disabled(self, caplog):
        proc, ioc_a, ioc_b = self._setup()
        self._hold(proc, ioc_a.id, ["ALIAS:1"])

        self._commit(proc, ioc_b, ["PV:1"], aliases={"PV:1": ["ALIAS:1"]})

        assert self._messages(caplog) == []

    def test_marks_earlier_connection_of_same_ioc(self, caplog):
        proc, ioc_a, _ = self._setup()
        restarted = replace(ioc_a, port=40000)
        self._hold(proc, ioc_a.id, ["PV:1"])

        with caplog.at_level(logging.INFO):
            self._commit(proc, restarted, ["PV:1"])

        (record,) = self._records(caplog)
        message = record.getMessage()
        assert record.levelname == "INFO"
        assert self._PHRASE not in message
        assert "1 channels held only by an earlier connection of the same IOC" in message
        assert "PV:1 (same IOC, earlier connection)" in message

    def test_counts_other_ioc_and_earlier_connection_separately(self, caplog):
        proc, ioc_a, ioc_b = self._setup()
        proc.iocs[ioc_b.id] = ioc_b
        restarted = replace(ioc_a, port=40000)
        self._hold(proc, ioc_a.id, ["PV:1", "PV:2"])
        self._hold(proc, ioc_b.id, ["PV:3"])

        self._commit(proc, restarted, ["PV:1", "PV:2", "PV:3"])

        (record,) = self._records(caplog)
        message = record.getMessage()
        assert record.levelname == "WARNING"
        assert "1 channels also held by another IOC, 2 only by an earlier connection of the same IOC" in message
        assert f"PV:3 (IOC2@{ioc_b.id})" in message


class TestEvictIoc:
    def test_evict_ioc_removes_all_tracking_state(self):
        proc = make_processor()
        ioc = make_ioc(channelcount=2)
        iocid = ioc.id
        proc.iocs[iocid] = ioc
        proc._ioc_channels[iocid].add("PV:1")
        proc._ioc_channels[iocid].add("PV:2")
        proc.channel_ioc_ids["PV:1"].append(iocid)
        proc.channel_ioc_ids["PV:2"].append(iocid)

        proc._evict_ioc(iocid)

        assert iocid not in proc.iocs
        assert iocid not in proc._ioc_channels
        assert "PV:1" not in proc.channel_ioc_ids
        assert "PV:2" not in proc.channel_ioc_ids

    def test_evict_ioc_is_noop_for_unknown_ioc(self):
        proc = make_processor()
        proc._evict_ioc("1.2.3.4:5064")  # NOSONAR — must not raise

    def test_commit_thread_evicts_ioc_on_push_failure(self, monkeypatch):
        """On retry exhaustion for a connected transaction, IOC is evicted from memory."""
        proc = make_processor()
        proc.running = True
        ioc = make_ioc(channelcount=1)
        iocid = ioc.id

        # Pre-populate state as update_ioc_infos would have done
        proc.iocs[iocid] = ioc
        proc._ioc_channels[iocid].add("PV:1")
        proc.channel_ioc_ids["PV:1"].append(iocid)

        # Skip update_ioc_infos (state already set up) and fail the CF push
        monkeypatch.setattr(proc, "update_ioc_infos", lambda *a: None)
        monkeypatch.setattr(proc, "_push_to_cf", lambda *a: False)
        monkeypatch.setattr(proc, "transaction_to_record_infos", lambda *a: {})
        monkeypatch.setattr(CFProcessor, "record_info_by_name", staticmethod(lambda *a: {}))

        t = make_transaction(_HOST_A, 5064)
        t.connected = True
        t.records_to_delete = []
        t.client_infos = {"IOCNAME": "TEST-IOC", "HOSTNAME": "test.host"}

        with pytest.raises(defer.CancelledError):
            proc._commit_with_thread(t)

        assert iocid not in proc.iocs
        assert iocid not in proc._ioc_channels
        assert "PV:1" not in proc.channel_ioc_ids
