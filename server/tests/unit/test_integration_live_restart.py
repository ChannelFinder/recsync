from unittest.mock import Mock

import pytest

from recceiver.cf.adapter import PyCFClientAdapter
from recceiver.cf.model import CFChannel, CFProperty
from tests.integration import test_single_ioc


def test_live_restart_seeds_typed_properties_without_the_adapter_helper_migration(monkeypatch):
    receiver = CFProperty("recceiverID", "admin", "receiver")
    baseline = [CFChannel(name, "admin", [receiver]) for name in ["PV:1", "PV:1:alias"]]
    adapter = Mock(spec=PyCFClientAdapter)
    adapter.find_by_names.return_value = baseline
    adapter.find_active_for_recceiver.return_value = baseline
    compose = Mock()
    compose.get_container.return_value.ID = "container-id"
    monkeypatch.setattr(test_single_ioc, "wait_for_sync", lambda adapter, check: check(adapter))

    class SeedValidated(Exception):
        pass

    def validate_seed(channels):
        assert len(channels) == 1
        assert all(isinstance(prop, CFProperty) for prop in channels[0].properties)
        assert channels[0].as_dict()["properties"] == [
            receiver.as_dict(),
            {"name": "pvStatus", "owner": "admin", "value": "Active"},
        ]
        raise SeedValidated

    adapter.set_channels.side_effect = validate_seed
    with pytest.raises(SeedValidated):
        test_single_ioc.TestLiveIOCRestartRecceiver().test_restart_restores_live_channels_and_cleans_stale_channels(
            compose, adapter
        )
    adapter.set_channels.assert_called_once()
