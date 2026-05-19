from unittest.mock import MagicMock

from requests import RequestException
from twisted.internet import defer
from twisted.internet.address import IPv4Address
from twisted.internet.defer import DeferredLock

from recceiver.cf.model import CFChannel, CFProperty, CFPropertyName, PVStatus, RecordInfo
from recceiver.cf.processor import CFProcessor, CFUpdateAbortedError
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


_HOST_A = "1.2.3.4"  # NOSONAR
_HOST_B = "5.6.7.8"  # NOSONAR


def make_transaction(host: str = _HOST_A, port: int = 5064) -> MagicMock:
    t = MagicMock()
    t.source_address = IPv4Address("TCP", host, port)
    return t


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
        proc.running = True
        proc.managed_properties = set()
        proc.record_property_names_list = set()
        proc.env_vars = {}
        return proc, adapter

    def test_registers_new_channel_as_active(self):
        proc, adapter = self._make_proc()
        ioc = make_ioc()
        proc.iocs[ioc.id] = ioc

        proc._update_channelfinder({"PV:1": RecordInfo(pv_name="PV:1")}, [], ioc, proc.iocs, proc.channel_ioc_ids)

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

        proc._update_channelfinder({}, [], ioc, proc.iocs, proc.channel_ioc_ids)

        status = next(p for p in adapter._channels["PV:1"].properties if p.name == CFPropertyName.PV_STATUS.value)
        assert status.value == PVStatus.INACTIVE.value


class TestPushToCF:
    """Tests for _push_to_cf_async: retry abandonment when stopped or cancelled."""

    def _run_async_push(self, monkeypatch, processor, ioc, side_effect_fn):
        iocid = ioc.id
        processor._cancelled[iocid] = False

        def sync_thread(fn, *args):
            try:
                fn(*args)
                return defer.succeed(None)
            except Exception as exc:
                return defer.fail(exc)

        monkeypatch.setattr("recceiver.cf.processor.deferToThread", sync_thread)
        monkeypatch.setattr("recceiver.cf.processor.task.deferLater", lambda *a, **kw: defer.succeed(None))
        monkeypatch.setattr(processor, "_update_channelfinder", side_effect_fn)

        results, errors = [], []
        processor._push_to_cf_async(iocid, {}, [], ioc, {}, {}).addCallbacks(results.append, errors.append)
        return results, errors

    def test_abandons_push_when_processor_stops_during_retry(self, monkeypatch):
        processor = make_processor()
        processor.running = True
        processor.cf_config.push_always_retry = True
        ioc = make_ioc()
        call_count = [0]

        def failing_update(*args):
            call_count[0] += 1
            processor.running = False
            raise RequestException("CF unreachable")

        results, errors = self._run_async_push(monkeypatch, processor, ioc, failing_update)
        assert len(results) == 1 and len(errors) == 0
        assert call_count[0] == 1

    def test_abandons_push_when_ioc_cancelled_during_retry(self, monkeypatch):
        processor = make_processor()
        processor.running = True
        processor.cf_config.push_always_retry = True
        ioc = make_ioc()
        iocid = ioc.id
        call_count = [0]

        def failing_update(*args):
            call_count[0] += 1
            processor._cancelled[iocid] = True
            raise RequestException("CF unreachable")

        results, errors = self._run_async_push(monkeypatch, processor, ioc, failing_update)
        assert len(results) == 1 and len(errors) == 0
        assert call_count[0] == 1


class TestPerIocLocking:
    def test_different_iocs_get_different_locks(self):
        proc = make_processor()
        lock_a = proc._get_ioc_lock(f"{_HOST_A}:5064")
        lock_b = proc._get_ioc_lock(f"{_HOST_B}:5064")
        assert lock_a is not lock_b

    def test_same_ioc_gets_same_lock(self):
        proc = make_processor()
        lock1 = proc._get_ioc_lock(f"{_HOST_A}:5064")
        lock2 = proc._get_ioc_lock(f"{_HOST_A}:5064")
        assert lock1 is lock2

    def test_commit_routes_to_correct_iocid(self, monkeypatch):
        proc = make_processor()
        routed_iocids = []

        def fake_lock_run(fn, transaction, iocid):
            routed_iocids.append(iocid)
            return defer.succeed(None)

        lock = DeferredLock()
        monkeypatch.setattr(lock, "run", fake_lock_run)
        monkeypatch.setattr(proc, "_get_ioc_lock", lambda _iocid: lock)

        proc.commit(make_transaction(_HOST_A, 5064))
        assert routed_iocids == [f"{_HOST_A}:5064"]

    def test_prune_removes_state_after_ioc_disconnects(self):
        proc = make_processor()
        iocid = f"{_HOST_A}:5064"
        proc._ioc_locks[iocid] = DeferredLock()
        proc._cancelled[iocid] = False
        proc._ioc_channels[iocid].add("CHAN:1")

        # iocid is NOT in proc.iocs → IOC has disconnected
        proc._prune_ioc_state(None, iocid)

        assert iocid not in proc._ioc_locks
        assert iocid not in proc._cancelled
        assert iocid not in proc._ioc_channels

    def test_prune_preserves_state_while_ioc_is_active(self):
        proc = make_processor()
        iocid = f"{_HOST_A}:5064"
        proc._ioc_locks[iocid] = DeferredLock()
        proc._cancelled[iocid] = False
        proc.iocs[iocid] = make_ioc()

        proc._prune_ioc_state(None, iocid)

        assert iocid in proc._ioc_locks
        assert iocid in proc._cancelled

    def test_prune_passes_result_through(self):
        proc = make_processor()
        iocid = f"{_HOST_A}:5064"
        assert proc._prune_ioc_state("sentinel", iocid) == "sentinel"


class TestCommitErrorHandling:
    """Test _commit_with_lock error routing without running real threads.

    _prepare_commit is simulated via a monkeypatched deferToThread; the CF
    push phase is controlled by patching _push_to_cf_async directly so each
    test exercises exactly one failure mode at a time.
    """

    def _run(self, monkeypatch, *, prepare_result, push_result=None):
        proc = make_processor()
        proc.running = True
        iocid = f"{_HOST_A}:5064"
        monkeypatch.setattr("recceiver.cf.processor.deferToThread", lambda *_a, **_kw: prepare_result)
        if push_result is not None:
            monkeypatch.setattr(proc, "_push_to_cf_async", lambda *_a, **_kw: push_result)

        results, errors = [], []
        proc._commit_with_lock(make_transaction(), iocid).addCallbacks(results.append, errors.append)
        return results, errors

    def test_aborted_commit_resolves_chain(self, monkeypatch):
        """CFUpdateAbortedError from exhausted retries lets the chain continue."""
        ioc = make_ioc()
        results, errors = self._run(
            monkeypatch,
            prepare_result=defer.succeed((ioc, {}, [], {}, {})),
            push_result=defer.fail(CFUpdateAbortedError("retries exhausted")),
        )
        assert len(results) == 1 and len(errors) == 0

    def test_unexpected_error_errbacks_chain(self, monkeypatch):
        ioc = make_ioc()
        results, errors = self._run(
            monkeypatch,
            prepare_result=defer.succeed((ioc, {}, [], {}, {})),
            push_result=defer.fail(RuntimeError("unexpected")),
        )
        assert len(results) == 0 and len(errors) == 1
        assert errors[0].check(RuntimeError)

    def test_service_stopped_cancel_errbacks_chain(self, monkeypatch):
        results, errors = self._run(
            monkeypatch,
            prepare_result=defer.fail(defer.CancelledError("service stopped")),
        )
        assert len(results) == 0 and len(errors) == 1
        assert errors[0].check(defer.CancelledError)

    def test_successful_commit_resolves_chain(self, monkeypatch):
        ioc = make_ioc()
        results, errors = self._run(
            monkeypatch,
            prepare_result=defer.succeed((ioc, {}, [], {}, {})),
            push_result=defer.succeed(None),
        )
        assert len(results) == 1 and len(errors) == 0
