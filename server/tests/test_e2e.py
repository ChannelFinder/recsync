import logging

import pytest
from channelfinder import ChannelFinderClient

from .client_checks import create_client_and_wait
from .docker import setup_compose  # noqa: F401

LOG: logging.Logger = logging.getLogger(__name__)

RECSYNC_RESTART_DELAY = 30
# Number of channels expected in the default setup
# 4 iocs, 6 channels per ioc (2 reccaster.db, 2 somerecords.db, 2 aliases in somerecords.db)
EXPECTED_DEFAULT_CHANNEL_COUNT = 32


@pytest.fixture(scope="class")
def cf_client(setup_compose):  # noqa: F811
    return create_client_and_wait(setup_compose, EXPECTED_DEFAULT_CHANNEL_COUNT)


class TestE2E:
    def test_number_of_channels_and_channel_name(self, cf_client: ChannelFinderClient) -> None:
        channels = cf_client.find(name="*")
        assert len(channels) == EXPECTED_DEFAULT_CHANNEL_COUNT
        assert channels[0]["name"] == "IOC1-1:Msg-I"

    # Smoke Test Default Properties
    def test_number_of_aliases_and_alais_property(self, cf_client: ChannelFinderClient) -> None:
        channels = cf_client.find(property=[("alias", "*")])
        assert len(channels) == 8
        assert channels[0]["name"] == "IOC1-1:lix1"
        assert {
            "name": "alias",
            "value": "IOC1-1:li",
            "owner": "admin",
            "channels": [],
        } in channels[0]["properties"]

    def test_number_of_recordDesc_and_property(self, cf_client: ChannelFinderClient) -> None:
        channels = cf_client.find(property=[("recordDesc", "*")])
        assert len(channels) == 4
        assert {
            "name": "recordDesc",
            "value": "testdesc",
            "owner": "admin",
            "channels": [],
        } in channels[0]["properties"]

    def test_number_of_recordType_and_property(self, cf_client: ChannelFinderClient) -> None:
        channels = cf_client.find(property=[("recordType", "*")])
        assert len(channels) == EXPECTED_DEFAULT_CHANNEL_COUNT
        assert {
            "name": "recordType",
            "value": "stringin",
            "owner": "admin",
            "channels": [],
        } in channels[0]["properties"]
