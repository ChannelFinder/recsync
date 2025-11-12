import logging
import time
from pathlib import Path

import pytest
from channelfinder import ChannelFinderClient
from testcontainers.compose import DockerCompose

from .client_checks import (
    BASE_IOC_CHANNEL_COUNT,
    DEFAULT_CHANNEL_NAME,
    INACTIVE_PROPERTY,
    channels_match,
    check_channel_property,
    create_client_and_wait,
    wait_for_sync,
)
from .docker import (
    ComposeFixtureFactory,
    clone_container,
    restart_container,
    shutdown_container,
    start_container,
)

PROPERTIES_TO_MATCH = ["pvStatus", "recordType", "recordDesc", "alias", "hostName", "iocName", "recceiverID"]

LOG: logging.Logger = logging.getLogger(__name__)

setup_compose = ComposeFixtureFactory(Path("tests") / "docker" / "test-single-ioc.yml").return_fixture()


@pytest.fixture(scope="class")
def cf_client(setup_compose: DockerCompose) -> ChannelFinderClient:  # noqa: F811
    return create_client_and_wait(setup_compose, expected_channel_count=BASE_IOC_CHANNEL_COUNT)


class TestRestartIOC:
    def test_channels_same_after_restart(self, setup_compose: DockerCompose, cf_client: ChannelFinderClient) -> None:  # noqa: F811
        channels_begin = cf_client.find(name="*")
        restart_container(setup_compose, "ioc1-1")
        assert wait_for_sync(cf_client, lambda cf_client: check_channel_property(cf_client, DEFAULT_CHANNEL_NAME))
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
        channels = cf_client.find(name=DEFAULT_CHANNEL_NAME)
        channels[0]["properties"] = [test_property_value]
        cf_client.set(property=test_property)
        channels_begin = cf_client.find(name="*")
        restart_container(setup_compose, "ioc1-1")
        assert wait_for_sync(cf_client, lambda cf_client: check_channel_property(cf_client, DEFAULT_CHANNEL_NAME))
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
    def test_status_property_works_after_cf_restart(
        self,
        setup_compose: DockerCompose,  # noqa: F811
        cf_client: ChannelFinderClient,
    ) -> None:
        # Arrange
        # Act
        restart_container(setup_compose, "cf")
        assert wait_for_sync(cf_client, check_connection_active)

        # Assert
        shutdown_container(setup_compose, "ioc1-1")
        assert wait_for_sync(
            cf_client, lambda cf_client: check_channel_property(cf_client, DEFAULT_CHANNEL_NAME, INACTIVE_PROPERTY)
        )
        channels_inactive = cf_client.find(property=[("iocName", "IOC1-1")])
        assert all(INACTIVE_PROPERTY in ch["properties"] for ch in channels_inactive)


class TestShutdownChannelFinder:
    def test_status_property_works_between_cf_down(
        self,
        setup_compose: DockerCompose,  # noqa: F811
        cf_client: ChannelFinderClient,
    ) -> None:
        # Arrange
        cf_container_id = shutdown_container(setup_compose, "cf")
        time.sleep(10)  # Wait to ensure CF is down while IOC is down

        # Act
        shutdown_container(setup_compose, "ioc1-1")
        time.sleep(10)  # Wait to ensure CF is down while IOC is down
        start_container(setup_compose, container_id=cf_container_id)
        assert wait_for_sync(cf_client, check_connection_active)

        # Assert
        assert wait_for_sync(
            cf_client, lambda cf_client: check_channel_property(cf_client, DEFAULT_CHANNEL_NAME, INACTIVE_PROPERTY)
        )
        channels_inactive = cf_client.find(property=[("iocName", "IOC1-1")])
        assert all(INACTIVE_PROPERTY in ch["properties"] for ch in channels_inactive)


class TestMoveIocHost:
    def test_move_ioc_host(
        self,
        setup_compose: DockerCompose,  # noqa: F811
        cf_client: ChannelFinderClient,
    ) -> None:
        channels_begin = cf_client.find(name="*")
        clone_container(setup_compose, "ioc1-1-new", host_name="ioc1-1")
        wait_for_sync(cf_client, lambda cf_client: check_channel_property(cf_client, DEFAULT_CHANNEL_NAME))
        channels_end = cf_client.find(name="*")
        assert len(channels_begin) == len(channels_end)
