from unittest.mock import Mock

import pytest

from recceiver.cf.adapter import PyCFClientAdapter
from recceiver.cf.model import CFChannel, CFProperty, CFPropertyName
from tests.integration.cf_client import (
    ACTIVE_PROPERTY,
    INACTIVE_PROPERTY,
    channels_match,
    check_channel_count,
    check_channel_property,
    create_adapter_from_compose,
    find_ioc_channels,
)


@pytest.fixture
def adapter():
    client = Mock()
    client.findByArgs.return_value = []
    return PyCFClientAdapter(client), client


def test_property_check_rejects_missing_channels(adapter):
    cf_adapter, _ = adapter
    assert not check_channel_property(cf_adapter, "MISSING:PV")


def test_property_check_uses_typed_properties_and_ignores_api_metadata(adapter):
    cf_adapter, client = adapter
    channel = CFChannel("PV:1", "admin", [ACTIVE_PROPERTY])
    payload = channel.as_dict()
    payload["properties"][0]["channels"] = []
    client.findByArgs.return_value = [payload]
    assert check_channel_property(cf_adapter, "PV:1")
    client.findByArgs.assert_called_once_with([("~name", "PV:1")])


@pytest.mark.parametrize(
    "prop",
    [INACTIVE_PROPERTY, CFProperty("pvStatus", "other-owner", "Active"), CFProperty("other", "admin", "Active")],
)
def test_property_check_requires_matching_name_owner_and_value(adapter, prop):
    cf_adapter, client = adapter
    client.findByArgs.return_value = [CFChannel("PV:1", "admin", [prop]).as_dict()]
    assert not check_channel_property(cf_adapter, "PV:1")


def test_property_check_requires_all_matching_channels_to_have_property(adapter):
    cf_adapter, client = adapter
    client.findByArgs.return_value = [
        CFChannel("PV:1", "admin", [ACTIVE_PROPERTY]).as_dict(),
        CFChannel("PV:2", "admin", [INACTIVE_PROPERTY]).as_dict(),
    ]
    assert not check_channel_property(cf_adapter)


def test_channel_count_uses_adapter_name_query(adapter):
    cf_adapter, client = adapter
    client.findByArgs.return_value = [CFChannel("PV:1", "admin", []).as_dict()]
    assert check_channel_count(cf_adapter, 1)
    assert not check_channel_count(cf_adapter, 2)
    assert client.findByArgs.call_args.args == ([("~name", "*")],)


@pytest.mark.parametrize("property_name", [CFPropertyName.PV_STATUS, "pvStatus"])
def test_channel_matching_is_independent_of_result_order(property_name):
    first = CFChannel("PV:1", "admin", [ACTIVE_PROPERTY])
    second = CFChannel("PV:2", "admin", [INACTIVE_PROPERTY])
    channels_match([first, second], [second, first], [property_name])


def test_channel_matching_rejects_different_names():
    with pytest.raises(AssertionError):
        channels_match([CFChannel("PV:1", "admin", [])], [CFChannel("PV:2", "admin", [])], [])


def test_channel_matching_rejects_different_owners():
    with pytest.raises(AssertionError):
        channels_match([CFChannel("PV:1", "admin", [])], [CFChannel("PV:1", "other", [])], [])


def test_channel_matching_rejects_changed_selected_properties():
    with pytest.raises(AssertionError):
        channels_match(
            [CFChannel("PV:1", "admin", [ACTIVE_PROPERTY])],
            [CFChannel("PV:1", "admin", [INACTIVE_PROPERTY])],
            [CFPropertyName.PV_STATUS],
        )


def test_channel_matching_ignores_unselected_timestamp_changes():
    channels_match(
        [CFChannel("PV:1", "admin", [ACTIVE_PROPERTY, CFProperty("time", "admin", "before")])],
        [CFChannel("PV:1", "admin", [ACTIVE_PROPERTY, CFProperty("time", "admin", "after")])],
        [CFPropertyName.PV_STATUS],
    )


def test_ioc_filter_uses_model_property_lookup(adapter):
    cf_adapter, client = adapter
    record = CFChannel("PV:1", "admin", [CFProperty("iocName", "admin", "IOC1")])
    alias = CFChannel("PV:1:alias", "admin", [CFProperty("iocName", "admin", "IOC1")])
    other = CFChannel("PV:2", "admin", [CFProperty("iocName", "admin", "IOC2")])
    missing = CFChannel("PV:3", "admin", [])
    client.findByArgs.return_value = [channel.as_dict() for channel in [record, alias, other, missing]]
    assert find_ioc_channels(cf_adapter, "IOC1") == [record, alias]


def test_factory_wraps_raw_client_at_the_boundary(monkeypatch):
    client_factory = Mock()
    monkeypatch.setattr("tests.integration.cf_client.ChannelFinderClient", client_factory)
    compose = Mock()
    compose.get_service_host_and_port.return_value = ("", 12345)
    cf_adapter = create_adapter_from_compose(compose)
    assert isinstance(cf_adapter, PyCFClientAdapter)
    client_factory.assert_called_once_with(
        BaseURL="http://localhost:12345/ChannelFinder", username="admin", password="password"
    )
    client_factory.return_value.findByArgs.return_value = []
    assert cf_adapter.find_by_names(["*"]) == []
    client_factory.return_value.findByArgs.assert_called_once_with([("~name", "*"), ("~size", 10000)])
