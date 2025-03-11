import logging
import time

import pytest
from channelfinder import ChannelFinderClient

from .docker import setup_compose  # noqa: F401

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    encoding="utf-8",
)

LOG: logging.Logger = logging.getLogger(__name__)

MAX_WAIT_SECONDS = 180
TIME_PERIOD_INCREMENT = 2

# Number of channels expected in the default setup
# 4 iocs, 6 channels per ioc (2 reccaster.db, 2 somerecords.db, 2 aliases in somerecords.db)
EXPECTED_DEFAULT_CHANNELS = 24


def check_channel_count(cf_client, name="*"):
    channels = cf_client.find(name=name)
    LOG.debug("Found %s channels", len(channels))
    return len(channels) == EXPECTED_DEFAULT_CHANNELS


def wait_for_sync(cf_client, check):
    time_period_to_wait_seconds = 1
    total_seconds_waited = 0
    while total_seconds_waited < MAX_WAIT_SECONDS:
        if check(cf_client):
            break
        time.sleep(time_period_to_wait_seconds)
        total_seconds_waited += time_period_to_wait_seconds
        time_period_to_wait_seconds += TIME_PERIOD_INCREMENT


def create_client_and_wait(compose):
    LOG.info("Waiting for channels to sync")
    cf_client = create_client_from_compose(compose)
    wait_for_sync(cf_client, lambda cf_client: check_channel_count(cf_client))
    return cf_client


def create_client_from_compose(compose):
    cf_host, cf_port = compose.get_service_host_and_port("cf")
    cf_url = f"http://{cf_host if cf_host else 'localhost'}:{cf_port}/ChannelFinder"
    # wait for channels to sync
    LOG.info("CF URL: %s", cf_url)
    cf_client = ChannelFinderClient(BaseURL=cf_url)
    return cf_client


@pytest.fixture(scope="class")
def cf_client(setup_compose):  # noqa: F811
    return create_client_and_wait(setup_compose)


class TestE2E:
    def test_number_of_channels_and_channel_name(self, cf_client) -> None:
        channels = cf_client.find(name="*")
        assert len(channels) == EXPECTED_DEFAULT_CHANNELS
        assert channels[0]["name"] == "IOC1-1:Msg-I"

    # Smoke Test Default Properties
    def test_number_of_aliases_and_alais_property(self, cf_client) -> None:
        channels = cf_client.find(property=[("alias", "*")])
        assert len(channels) == 8
        assert channels[0]["name"] == "IOC1-1:lix1"
        assert {
            "name": "alias",
            "value": "IOC1-1:li",
            "owner": "admin",
            "channels": [],
        } in channels[0]["properties"]

    def test_number_of_recordDesc_and_property(self, cf_client) -> None:
        channels = cf_client.find(property=[("recordDesc", "*")])
        assert len(channels) == 4
        assert {
            "name": "recordDesc",
            "value": "testdesc",
            "owner": "admin",
            "channels": [],
        } in channels[0]["properties"]

    def test_number_of_recordType_and_property(self, cf_client) -> None:
        channels = cf_client.find(property=[("recordType", "*")])
        assert len(channels) == EXPECTED_DEFAULT_CHANNELS
        assert {
            "name": "recordType",
            "value": "stringin",
            "owner": "admin",
            "channels": [],
        } in channels[0]["properties"]
