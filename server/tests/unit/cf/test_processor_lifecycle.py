"""Deterministic service tests: run real workers at explicitly chosen boundaries."""

from collections import deque
from dataclasses import dataclass, field

import pytest
from twisted.internet import defer
from twisted.internet.address import IPv4Address

from recceiver.cf.model import PVStatus
from recceiver.cf.processor import CFProcessor
from recceiver.recast import Transaction
from tests.unit.cf.conftest import DEFAULT_RECCEIVER_ID, make_channel
from tests.unit.cf.mock_adapter import MockCFAdapter
from tests.unit.conftest import make_adapter


@dataclass
class ThreadJob:
    function: object
    args: tuple
    kwargs: dict
    result: defer.Deferred = field(default_factory=defer.Deferred)
    execution: object = field(default=None, init=False)
    delivered: bool = field(default=False, init=False)

    def execute(self):
        """Run worker code, but hold its completion until the test delivers it."""
        assert self.execution is None
        self.execution = defer.maybeDeferred(self.function, *self.args, **self.kwargs)

    def finish(self):
        assert self.execution is not None
        assert not self.delivered
        self.delivered = True
        self.execution.chainDeferred(self.result)

    def run(self):
        self.execute()
        self.finish()


class ControlledThreads:
    """Queue thread jobs without executing them or starting a reactor."""

    def __init__(self):
        self.pending = deque()
        self.submitted = []

    def submit(self, function, *args, **kwargs):
        job = ThreadJob(function, args, kwargs)
        self.pending.append(job)
        self.submitted.append(job)
        return job.result

    def take_next(self, expected_name):
        job = self.pending.popleft()
        assert job.function.__name__ == expected_name
        return job

    def run_next(self, expected_name):
        job = self.take_next(expected_name)
        job.run()
        return job


class Outcome:
    """Observe completion, including called-but-paused Deferreds; consume failures."""

    def __init__(self, deferred):
        self.values = []
        self.failures = []
        deferred.addCallbacks(self.values.append, self.failures.append)

    @property
    def completed(self):
        return bool(self.values or self.failures)

    def assert_success(self):
        assert self.values == [None]
        assert self.failures == []

    def assert_failure(self, exception):
        assert self.values == []
        assert len(self.failures) == 1
        assert self.failures[0].check(exception)


def transaction(name="LIVE:PV", ioc_name="IOC1"):
    tx = Transaction(IPv4Address("TCP", "1.2.3.4", 5064), id=0)
    tx.initial = True
    tx.client_infos.update({"IOCNAME": ioc_name, "HOSTNAME": "ioc.example.com"})
    tx.records_to_add[1] = (name, "ai")
    return tx


def status(adapter, name):
    return next(p.value for p in adapter._channels[name].properties if p.name == "pvStatus")


@pytest.fixture
def lifecycle(monkeypatch):
    threads = ControlledThreads()
    monkeypatch.setattr("recceiver.cf.processor.deferToThread", threads.submit)
    proc = CFProcessor(
        "test",
        make_adapter(values={"recceiverId": DEFAULT_RECCEIVER_ID, "statusInterval": 0}),
    )
    adapter = MockCFAdapter()
    proc.client = adapter
    yield proc, adapter, threads
    # Tests must account for every submitted worker and release every lock.
    assert not threads.pending
    assert all(job.delivered for job in threads.submitted)
    assert not proc.lock.locked
    if proc.running:
        stopped = Outcome(proc.stopService())
        if threads.pending:
            threads.run_next("clean_service")
        stopped.assert_success()
    assert not threads.pending
    assert all(job.delivered for job in threads.submitted)
    assert not proc.lock.locked
    assert not proc._startup_commit_waiters
    assert not proc._stopping


def test_start_commit_stop_uses_real_workers(lifecycle):
    proc, adapter, threads = lifecycle
    proc.startService()
    threads.run_next("clean_on_start")

    committed = Outcome(proc.commit(transaction()))
    assert not committed.completed
    threads.run_next("_commit_with_thread")
    committed.assert_success()
    assert status(adapter, "LIVE:PV") == PVStatus.ACTIVE.value

    stopped = Outcome(proc.stopService())
    assert not stopped.completed
    threads.run_next("clean_service")
    stopped.assert_success()
    assert status(adapter, "LIVE:PV") == PVStatus.INACTIVE.value


def test_executor_propagates_worker_exceptions():
    threads = ControlledThreads()

    def failing_worker():
        raise ValueError("worker failed")

    outcome = Outcome(threads.submit(failing_worker))
    assert not outcome.completed
    threads.run_next("failing_worker")
    outcome.assert_failure(ValueError)
    assert not threads.pending


def test_cleanup_precedes_live_commit_and_leaves_stale_channels_inactive(lifecycle):
    proc, adapter, threads = lifecycle
    adapter.set_channels([make_channel("LIVE:PV"), make_channel("STALE:PV")])
    proc.startService()
    committed = Outcome(proc.commit(transaction()))

    assert [job.function.__name__ for job in threads.submitted] == ["clean_on_start"]
    assert not committed.completed
    assert status(adapter, "LIVE:PV") == PVStatus.ACTIVE.value
    assert status(adapter, "STALE:PV") == PVStatus.ACTIVE.value

    threads.run_next("clean_on_start")
    assert not committed.completed
    assert status(adapter, "LIVE:PV") == PVStatus.INACTIVE.value
    assert status(adapter, "STALE:PV") == PVStatus.INACTIVE.value

    threads.run_next("_commit_with_thread")
    committed.assert_success()
    assert status(adapter, "LIVE:PV") == PVStatus.ACTIVE.value
    assert status(adapter, "STALE:PV") == PVStatus.INACTIVE.value


def test_startup_waiters_commit_serially(lifecycle):
    proc, adapter, threads = lifecycle
    proc.startService()
    first = Outcome(proc.commit(transaction("FIRST:PV", "FIRST")))
    second = Outcome(proc.commit(transaction("SECOND:PV", "SECOND")))
    assert len(threads.submitted) == 1

    threads.run_next("clean_on_start")
    assert len(threads.pending) == 1
    assert not first.completed
    assert not second.completed

    threads.run_next("_commit_with_thread")
    first.assert_success()
    assert not second.completed
    assert len(threads.pending) == 1
    assert "SECOND:PV" not in adapter._channels

    threads.run_next("_commit_with_thread")
    second.assert_success()
    assert status(adapter, "FIRST:PV") == PVStatus.ACTIVE.value
    assert status(adapter, "SECOND:PV") == PVStatus.ACTIVE.value


@pytest.mark.parametrize("clean_on_start", [True, False])
def test_commit_after_startup_does_not_repeat_cleanup(lifecycle, clean_on_start):
    proc, adapter, threads = lifecycle
    proc.cf_config.clean_on_start = clean_on_start
    adapter.set_channels([make_channel("STALE:PV")])
    proc.startService()
    threads.run_next("clean_on_start")
    expected = PVStatus.INACTIVE if clean_on_start else PVStatus.ACTIVE
    assert status(adapter, "STALE:PV") == expected.value

    committed = Outcome(proc.commit(transaction()))
    threads.run_next("_commit_with_thread")
    committed.assert_success()
    assert [job.function.__name__ for job in threads.submitted] == ["clean_on_start", "_commit_with_thread"]
    assert status(adapter, "STALE:PV") == expected.value


def test_cancelled_startup_waiter_does_not_affect_other_commits(lifecycle):
    proc, adapter, threads = lifecycle
    proc.startService()
    cancelled = proc.commit(transaction("CANCELLED:PV", "CANCELLED"))
    outcome = Outcome(cancelled)
    following = Outcome(proc.commit(transaction()))
    cancelled.cancel()
    outcome.assert_failure(defer.CancelledError)
    assert len(threads.submitted) == 1

    threads.run_next("clean_on_start")
    threads.run_next("_commit_with_thread")
    following.assert_success()
    assert "CANCELLED:PV" not in adapter._channels
    assert status(adapter, "LIVE:PV") == PVStatus.ACTIVE.value


def test_startup_failure_reaches_waiting_and_later_commits(lifecycle, monkeypatch):
    proc, adapter, threads = lifecycle

    def fail_find(_recceiver_id):
        raise ValueError("unexpected cleanup failure")

    monkeypatch.setattr(adapter, "find_active_for_recceiver", fail_find)
    proc.startService()
    first = Outcome(proc.commit(transaction()))
    second = Outcome(proc.commit(transaction("SECOND:PV", "SECOND")))
    threads.run_next("clean_on_start")
    first.assert_failure(ValueError)
    second.assert_failure(ValueError)
    later = Outcome(proc.commit(transaction("LATER:PV", "LATER")))
    later.assert_failure(ValueError)
    assert len(threads.submitted) == 1
    assert adapter._channels == {}
    # Shutdown consumes the shared gate, after all callers observed its failure.
    # Avoid running shutdown's worker with the patched, failing adapter method.
    proc.cf_config.clean_on_stop = False


@pytest.mark.parametrize("clean_on_stop", [True, False])
def test_shutdown_waits_for_active_commit_before_stop_cleanup(lifecycle, clean_on_stop):
    proc, adapter, threads = lifecycle
    proc.cf_config.clean_on_stop = clean_on_stop
    proc.startService()
    threads.run_next("clean_on_start")
    committed = Outcome(proc.commit(transaction()))
    worker = threads.take_next("_commit_with_thread")
    worker.execute()
    assert status(adapter, "LIVE:PV") == PVStatus.ACTIVE.value
    assert not committed.completed

    stopped = Outcome(proc.stopService())
    assert not stopped.completed
    assert not threads.pending
    assert proc.lock.locked

    worker.finish()
    committed.assert_success()
    if clean_on_stop:
        assert not stopped.completed
        threads.run_next("clean_service")
    stopped.assert_success()
    expected = PVStatus.INACTIVE if clean_on_stop else PVStatus.ACTIVE
    assert status(adapter, "LIVE:PV") == expected.value


@pytest.mark.parametrize("clean_on_stop", [True, False])
def test_shutdown_during_startup_cancels_waiters_and_waits_for_cleanup(lifecycle, clean_on_stop):
    proc, adapter, threads = lifecycle
    proc.cf_config.clean_on_stop = clean_on_stop
    adapter.set_channels([make_channel("STALE:PV")])
    proc.startService()
    first = Outcome(proc.commit(transaction()))
    second = Outcome(proc.commit(transaction("SECOND:PV", "SECOND")))

    shutdown = proc.stopService()
    assert proc.stopService() is shutdown
    stopped = Outcome(shutdown)
    first.assert_failure(defer.CancelledError)
    second.assert_failure(defer.CancelledError)
    assert not stopped.completed
    assert len(threads.submitted) == 1
    with pytest.raises(RuntimeError, match="stopping"):
        proc.startService()

    threads.run_next("clean_on_start")
    if clean_on_stop:
        assert not stopped.completed
        threads.run_next("clean_service")
    stopped.assert_success()
    assert status(adapter, "STALE:PV") == PVStatus.INACTIVE.value
    assert "LIVE:PV" not in adapter._channels
    assert "SECOND:PV" not in adapter._channels
    assert all(job.function.__name__ != "_commit_with_thread" for job in threads.submitted)


def test_restart_uses_fresh_cleanup_gate_without_old_transactions(lifecycle):
    proc, adapter, threads = lifecycle
    proc.startService()
    old_gate = proc._startup_clean
    old_commit = Outcome(proc.commit(transaction("OLD:PV", "OLD")))
    stopped = Outcome(proc.stopService())
    threads.run_next("clean_on_start")
    threads.run_next("clean_service")
    stopped.assert_success()
    old_commit.assert_failure(defer.CancelledError)

    proc.startService()
    assert proc._startup_clean is not old_gate
    restarted = Outcome(proc.commit(transaction()))
    assert not restarted.completed
    assert len(threads.pending) == 1
    threads.run_next("clean_on_start")
    assert not restarted.completed
    threads.run_next("_commit_with_thread")
    restarted.assert_success()
    assert "OLD:PV" not in adapter._channels
    assert status(adapter, "LIVE:PV") == PVStatus.ACTIVE.value


def test_initialization_failure_stops_service_without_launching_workers(lifecycle, monkeypatch):
    proc, adapter, threads = lifecycle

    def fail_properties():
        raise ValueError("invalid property setup")

    monkeypatch.setattr(adapter, "get_property_names", fail_properties)
    with pytest.raises(ValueError, match="invalid property setup"):
        proc.startService()
    assert not proc.running
    assert not proc.lock.locked
    assert threads.submitted == []
    Outcome(proc.commit(transaction())).assert_failure(defer.CancelledError)
    Outcome(proc.stopService()).assert_success()


def test_commit_before_start_and_after_stop_is_cancelled(lifecycle):
    proc, adapter, threads = lifecycle
    Outcome(proc.commit(transaction())).assert_failure(defer.CancelledError)
    proc.startService()
    threads.run_next("clean_on_start")
    stopped = Outcome(proc.stopService())
    threads.run_next("clean_service")
    stopped.assert_success()
    Outcome(proc.commit(transaction())).assert_failure(defer.CancelledError)
    assert len(threads.submitted) == 2
    assert adapter._channels == {}


def test_active_cancellation_waits_for_completion_before_next_commit(lifecycle):
    proc, adapter, threads = lifecycle
    proc.startService()
    threads.run_next("clean_on_start")
    cancelled = proc.commit(transaction("FIRST:PV", "FIRST"))
    first = Outcome(cancelled)
    worker = threads.take_next("_commit_with_thread")
    worker.execute()
    following = Outcome(proc.commit(transaction("SECOND:PV", "SECOND")))

    cancelled.cancel()
    assert proc.cancelled
    assert not first.completed
    assert not following.completed
    assert not threads.pending
    assert proc.lock.locked

    worker.finish()
    first.assert_failure(defer.CancelledError)
    assert not following.completed
    threads.run_next("_commit_with_thread")
    following.assert_success()
    assert not proc.lock.locked
    assert status(adapter, "SECOND:PV") == PVStatus.ACTIVE.value


def test_shutdown_waits_for_startup_failure_without_stranding_waiters(lifecycle, monkeypatch):
    proc, adapter, threads = lifecycle
    proc.cf_config.clean_on_stop = False

    def fail_find(_recceiver_id):
        raise ValueError("unexpected cleanup failure")

    monkeypatch.setattr(adapter, "find_active_for_recceiver", fail_find)
    proc.startService()
    committed = Outcome(proc.commit(transaction()))
    stopped = Outcome(proc.stopService())
    committed.assert_failure(defer.CancelledError)
    assert not stopped.completed
    threads.run_next("clean_on_start")
    stopped.assert_success()
    assert not proc._stopping


def test_reentrant_stop_from_cancelled_waiter_returns_same_shutdown(lifecycle):
    proc, _adapter, threads = lifecycle
    proc.startService()
    reentrant_shutdowns = []
    committed = proc.commit(transaction())

    def cancelled(failure):
        assert failure.check(defer.CancelledError)
        reentrant_shutdowns.append(proc.stopService())

    committed.addErrback(cancelled)
    shutdown = proc.stopService()
    assert reentrant_shutdowns == [shutdown]
    stopped = Outcome(shutdown)
    threads.run_next("clean_on_start")
    threads.run_next("clean_service")
    stopped.assert_success()


def test_shutdown_rejects_lock_queued_commit_without_starting_worker(lifecycle):
    proc, adapter, threads = lifecycle
    proc.startService()
    threads.run_next("clean_on_start")
    first = Outcome(proc.commit(transaction("FIRST:PV", "FIRST")))
    worker = threads.take_next("_commit_with_thread")
    worker.execute()
    queued = Outcome(proc.commit(transaction("QUEUED:PV", "QUEUED")))
    stopped = Outcome(proc.stopService())

    worker.finish()
    first.assert_success()
    queued.assert_failure(defer.CancelledError)
    assert [job.function.__name__ for job in threads.pending] == ["clean_service"]
    assert "QUEUED:PV" not in adapter._channels
    threads.run_next("clean_service")
    stopped.assert_success()
    assert status(adapter, "FIRST:PV") == PVStatus.INACTIVE.value
