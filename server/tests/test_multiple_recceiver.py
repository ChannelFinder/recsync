import logging
from pathlib import Path

import pytest
from channelfinder import ChannelFinderClient
from testcontainers.compose import DockerCompose

from .client_checks import BASE_ALIAS_COUNT, BASE_IOC_CHANNEL_COUNT, DEFAULT_CHANNEL_NAME, create_client_and_wait
from .docker import ComposeFixtureFactory

LOG: logging.Logger = logging.getLogger(__name__)

RECSYNC_RESTART_DELAY = 30
# Number of channels expected in the default setup
IOC_COUNT = 4
EXPECTED_DEFAULT_CHANNEL_COUNT = IOC_COUNT * BASE_IOC_CHANNEL_COUNT

setup_compose = ComposeFixtureFactory(Path("tests") / "docker" / "test-multi-recc.yml").return_fixture()


@pytest.fixture(scope="class")
def cf_client(setup_compose: DockerCompose):
    return create_client_and_wait(setup_compose, EXPECTED_DEFAULT_CHANNEL_COUNT)


class TestMultipleRecceiver:
    def test_number_of_channels_and_channel_name(self, cf_client: ChannelFinderClient) -> None:
        channels = cf_client.find(name="*")
        assert len(channels) == EXPECTED_DEFAULT_CHANNEL_COUNT
        assert channels[0]["name"] == DEFAULT_CHANNEL_NAME

    # Smoke Test Default Properties
    def test_number_of_aliases_and_alais_property(self, cf_client: ChannelFinderClient) -> None:
        channels = cf_client.find(property=[("alias", "*")])
        assert len(channels) == IOC_COUNT * BASE_ALIAS_COUNT
        assert channels[0]["name"] == DEFAULT_CHANNEL_NAME + ":alias"
        assert {
            "name": "alias",
            "value": DEFAULT_CHANNEL_NAME,
            "owner": "admin",
            "channels": [],
        } in channels[0]["properties"]

    def test_number_of_recordDesc_and_property(self, cf_client: ChannelFinderClient) -> None:
        channels = cf_client.find(property=[("recordDesc", "*")])
        assert len(channels) == EXPECTED_DEFAULT_CHANNEL_COUNT
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
            "value": "ai",
            "owner": "admin",
            "channels": [],
        } in channels[0]["properties"]
