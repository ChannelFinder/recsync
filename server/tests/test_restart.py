import logging

import pytest
from channelfinder import ChannelFinderClient
from testcontainers.compose import DockerCompose

from .client_checks import (
    INACTIVE_PROPERTY,
    channels_match,
    check_channel_property,
    create_client_and_wait,
    wait_for_sync,
)
from .docker import restart_container, setup_compose, shutdown_container  # noqa: F401

PROPERTIES_TO_MATCH = ["pvStatus", "recordType", "recordDesc", "alias", "hostName", "iocName", "recceiverID"]

LOG: logging.Logger = logging.getLogger(__name__)

EXPECTED_DEFAULT_CHANNEL_COUNT = 32


@pytest.fixture(scope="class")
def cf_client(setup_compose):  # noqa: F811
    return create_client_and_wait(setup_compose, expected_channel_count=EXPECTED_DEFAULT_CHANNEL_COUNT)


class TestRestartIOC:
    def test_channels_same_after_restart(self, setup_compose: DockerCompose, cf_client: ChannelFinderClient) -> None:  # noqa: F811
        channels_begin = cf_client.find(name="*")
        restart_container(setup_compose, "ioc1-1")
        assert wait_for_sync(cf_client, lambda cf_client: check_channel_property(cf_client, "IOC1-1:Msg-I"))
        channels_end = cf_client.find(name="*")
        assert len(channels_begin) == len(channels_end)
        channels_match(channels_begin, channels_end, PROPERTIES_TO_MATCH)

    def test_manual_channels_same_after_restart(
        self,
        setup_compose: DockerCompose,  # noqa: F811
        cf_client: ChannelFinderClient,
    ) -> None:
        test_property = {"name": "test_property", "owner": "testowner"}
        cf_client.set(properties=[test_property])
        test_property_value = test_property | {"value": "test_value"}
        channels = cf_client.find(name="IOC1-1:Msg-I")
        channels[0]["properties"] = [test_property_value]
        cf_client.set(property=test_property)
        channels_begin = cf_client.find(name="*")
        restart_container(setup_compose, "ioc1-1")
        assert wait_for_sync(cf_client, lambda cf_client: check_channel_property(cf_client, "IOC1-1:Msg-I"))
        channels_end = cf_client.find(name="*")
        assert len(channels_begin) == len(channels_end)
        channels_match(channels_begin, channels_end, PROPERTIES_TO_MATCH + ["test_property"])


def check_connection_active(cf_client: ChannelFinderClient) -> bool:
    try:
        cf_client.find(name="*")
    except Exception:
        return False
    return True


class TestRestartChannelFinder:
    def test_channels_same_after_restart(self, setup_compose: DockerCompose, cf_client: ChannelFinderClient) -> None:  # noqa: F811
        channels_begin = cf_client.find(name="*")
        restart_container(setup_compose, "cf")
        assert wait_for_sync(cf_client, check_connection_active)
        channels_end = cf_client.find(name="*")
        assert len(channels_begin) == len(channels_end)
        channels_match(channels_begin, channels_end, PROPERTIES_TO_MATCH)
        shutdown_container(setup_compose, "ioc1-1")
        assert wait_for_sync(
            cf_client, lambda cf_client: check_channel_property(cf_client, "IOC1-1:Msg-I", INACTIVE_PROPERTY)
        )
        channels_inactive = cf_client.find(property=[("iocName", "IOC1-1")])
        assert all(INACTIVE_PROPERTY in ch["properties"] for ch in channels_inactive)
