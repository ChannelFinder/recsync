import logging
import time
from typing import Callable

from channelfinder import ChannelFinderClient
from testcontainers.compose import DockerCompose

LOG: logging.Logger = logging.getLogger(__name__)

ACTIVE_PROPERTY = {"name": "pvStatus", "owner": "admin", "value": "Active", "channels": []}
INACTIVE_PROPERTY = {"name": "pvStatus", "owner": "admin", "value": "Inactive", "channels": []}
MAX_WAIT_SECONDS = 180
TIME_PERIOD_INCREMENT = 2


def channel_match(channel0, channel1, properties_to_match: list[str]):
    assert channel0["name"] == channel1["name"]
    assert channel0["owner"] == channel1["owner"]

    for prop in channel0["properties"]:
        assert not (prop["name"] in properties_to_match and prop not in channel1["properties"]), (
            f"Property {prop} not found in channel {channel1['name']}"
        )


def channels_match(channels_begin, channels_end, properties_to_match: list[str]):
    for channel_index, channel in enumerate(channels_begin):
        channel_match(channel, channels_end[channel_index], properties_to_match)


def check_channel_count(cf_client: ChannelFinderClient, name="*", expected_channel_count=24):
    channels = cf_client.find(name=name)
    LOG.debug("Found %s channels", len(channels))
    return len(channels) == expected_channel_count


def check_channel_property(cf_client: ChannelFinderClient, name="*", prop=ACTIVE_PROPERTY):
    channels = cf_client.find(name=name)
    active_channels = (prop in channel["properties"] for channel in channels)
    return all(active_channels)


def wait_for_sync(cf_client: ChannelFinderClient, check: Callable[[ChannelFinderClient], bool]) -> bool:
    time_period_to_wait_seconds = 1
    total_seconds_waited = 0
    while total_seconds_waited < MAX_WAIT_SECONDS:
        if check(cf_client):
            return True
        time.sleep(time_period_to_wait_seconds)
        total_seconds_waited += time_period_to_wait_seconds
        time_period_to_wait_seconds += TIME_PERIOD_INCREMENT
    return False


def create_client_and_wait(compose: DockerCompose, expected_channel_count=24) -> ChannelFinderClient:
    LOG.info("Waiting for channels to sync")
    cf_client = create_client_from_compose(compose)
    assert wait_for_sync(
        cf_client, lambda cf_client: check_channel_count(cf_client, expected_channel_count=expected_channel_count)
    )
    return cf_client


def create_client_from_compose(compose: DockerCompose) -> ChannelFinderClient:
    cf_host, cf_port = compose.get_service_host_and_port("cf")
    cf_url = f"http://{cf_host if cf_host else 'localhost'}:{cf_port}/ChannelFinder"
    # wait for channels to sync
    LOG.info("CF URL: %s", cf_url)
    cf_client = ChannelFinderClient(BaseURL=cf_url, username="admin", password="password")
    return cf_client
