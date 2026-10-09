"""Cleanup scope and retry tests, independent of Deferred lifecycle ordering."""

import pytest
from requests import RequestException

from recceiver.cf.model import PVStatus
from recceiver.cf.processor import CFProcessor
from tests.unit.cf.conftest import DEFAULT_RECCEIVER_ID, make_channel
from tests.unit.cf.mock_adapter import MockCFAdapter
from tests.unit.conftest import make_adapter


class CleanupAdapter(MockCFAdapter):
    def __init__(self, batch_size=None):
        super().__init__()
        self.batch_size = batch_size
        self.query_failures = 0
        self.update_failures = 0
        self.queries = 0
        self.update_attempts = 0
        self.updates = []

    def find_active_for_recceiver(self, recceiverid):
        self.queries += 1
        if self.query_failures:
            self.query_failures -= 1
            raise RequestException("temporary query failure")
        channels = super().find_active_for_recceiver(recceiverid)
        return channels if self.batch_size is None else channels[: self.batch_size]

    def update_property(self, prop, channel_names):
        self.update_attempts += 1
        if self.update_failures:
            self.update_failures -= 1
            raise RequestException("temporary update failure")
        super().update_property(prop, channel_names)
        self.updates.append((prop, list(channel_names)))


@pytest.fixture
def cleanup(monkeypatch):
    proc = CFProcessor("test", make_adapter(values={"recceiverId": DEFAULT_RECCEIVER_ID}))
    adapter = CleanupAdapter()
    proc.client = adapter
    sleeps = []
    monkeypatch.setattr("recceiver.cf.processor.time.sleep", sleeps.append)
    return proc, adapter, sleeps


def channel_status(adapter, name):
    return next(p.value for p in adapter._channels[name].properties if p.name == "pvStatus")


def test_cleanup_only_updates_this_receivers_active_channels(cleanup):
    proc, adapter, sleeps = cleanup
    adapter.set_channels(
        [
            make_channel("ACTIVE:PV"),
            make_channel("INACTIVE:PV", active=False),
            make_channel("FOREIGN:PV", recceiver_id="another-receiver"),
        ]
    )
    proc.clean_service()

    assert channel_status(adapter, "ACTIVE:PV") == PVStatus.INACTIVE.value
    assert channel_status(adapter, "INACTIVE:PV") == PVStatus.INACTIVE.value
    assert channel_status(adapter, "FOREIGN:PV") == PVStatus.ACTIVE.value
    assert len(adapter.updates) == 1
    prop, names = adapter.updates[0]
    assert names == ["ACTIVE:PV"]
    assert prop.name == "pvStatus"
    assert prop.value == PVStatus.INACTIVE.value
    assert prop.owner == proc.cf_config.username
    assert sleeps == []


def test_empty_cleanup_makes_no_updates(cleanup):
    proc, adapter, sleeps = cleanup
    proc.clean_service()
    assert adapter.queries == 1
    assert adapter.update_attempts == 0
    assert sleeps == []


def test_cleanup_exhausts_all_query_batches(cleanup):
    proc, adapter, sleeps = cleanup
    adapter.batch_size = 2
    adapter.set_channels([make_channel(f"PV:{index}") for index in range(5)])
    proc.clean_service()

    assert [names for _, names in adapter.updates] == [["PV:0", "PV:1"], ["PV:2", "PV:3"], ["PV:4"]]
    assert adapter.queries == 4
    assert all(channel_status(adapter, name) == PVStatus.INACTIVE.value for name in adapter._channels)
    assert sleeps == []


@pytest.mark.parametrize("failure_kind", ["query_failures", "update_failures"])
def test_cleanup_recovers_from_transient_failure(cleanup, failure_kind):
    proc, adapter, sleeps = cleanup
    proc.running = True
    setattr(adapter, failure_kind, 1)
    adapter.set_channels([make_channel("PV:1")])
    proc.clean_service()

    assert sleeps == [1]
    assert channel_status(adapter, "PV:1") == PVStatus.INACTIVE.value
    assert len(adapter.updates) == 1
    assert adapter.queries == 3
    assert adapter.update_attempts == (2 if failure_kind == "update_failures" else 1)


def test_cleanup_retry_backoff_is_capped_and_recovers_while_running(cleanup):
    proc, adapter, sleeps = cleanup
    proc.running = True
    adapter.query_failures = 14
    adapter.set_channels([make_channel("PV:1")])
    proc.clean_service()

    assert len(sleeps) == 14
    assert sleeps[:3] == [1, 1.5, 2.25]
    assert max(sleeps) == 60
    assert sleeps[-1] == 60
    assert channel_status(adapter, "PV:1") == PVStatus.INACTIVE.value


def test_stopped_cleanup_abandons_persistent_failure_with_bounded_retries(cleanup):
    proc, adapter, sleeps = cleanup
    proc.running = False
    adapter.query_failures = 100
    adapter.set_channels([make_channel("PV:1")])
    proc.clean_service()

    assert sleeps == [1, 1.5, 2.25, 3.375]
    assert adapter.queries == 4
    assert adapter.update_attempts == 0
    assert channel_status(adapter, "PV:1") == PVStatus.ACTIVE.value
