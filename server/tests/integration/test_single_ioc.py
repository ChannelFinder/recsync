import logging
import time
from pathlib import Path

import pytest
from testcontainers.compose import DockerCompose

from recceiver.cf.adapter import PyCFClientAdapter
from recceiver.cf.model import CFProperty, CFPropertyName

from .cf_client import (
    BASE_IOC_CHANNEL_COUNT,
    DEFAULT_CHANNEL_NAME,
    INACTIVE_PROPERTY,
    channels_match,
    check_channel_property,
    create_adapter_and_wait,
    create_adapter_from_compose,
    find_ioc_channels,
    wait_for_sync,
)
from .docker_compose import (
    ComposeFixtureFactory,
    clone_container,
    kill_container,
    restart_container,
    shutdown_container,
    start_container,
)

PROPERTIES_TO_MATCH = [
    CFPropertyName.PV_STATUS,
    CFPropertyName.RECORD_TYPE,
    CFPropertyName.RECORD_DESC,
    CFPropertyName.ALIAS,
    CFPropertyName.HOSTNAME,
    CFPropertyName.IOC_NAME,
    CFPropertyName.RECCEIVER_ID,
]

LOG: logging.Logger = logging.getLogger(__name__)

setup_compose = ComposeFixtureFactory(
    Path("tests/integration/resources/docker-compose/test-single-ioc.yml")
).return_fixture()


@pytest.fixture(scope="class")
def cf_adapter(setup_compose: DockerCompose) -> PyCFClientAdapter:  # noqa: F811
    return create_adapter_and_wait(setup_compose, expected_channel_count=BASE_IOC_CHANNEL_COUNT)


class TestRestartIOC:
    def test_channels_same_after_restart(self, setup_compose: DockerCompose, cf_adapter: PyCFClientAdapter) -> None:  # noqa: F811
        channels_begin = cf_adapter.find_by_names(["*"])
        restart_container(setup_compose, "ioc1-1")
        assert wait_for_sync(cf_adapter, lambda adapter: check_channel_property(adapter, DEFAULT_CHANNEL_NAME))
        channels_end = cf_adapter.find_by_names(["*"])
        assert len(channels_begin) == len(channels_end)
        channels_match(channels_begin, channels_end, PROPERTIES_TO_MATCH)

    def test_manual_channels_same_after_restart(
        self,
        setup_compose: DockerCompose,  # noqa: F811
        cf_adapter: PyCFClientAdapter,
    ) -> None:
        test_property = CFProperty("test_property", "testowner", "test_value")
        cf_adapter.set_property(test_property.name, test_property.owner)
        channels = cf_adapter.find_by_names([DEFAULT_CHANNEL_NAME])
        channels[0].properties = [test_property]
        cf_adapter.set_property(test_property.name, test_property.owner)
        channels_begin = cf_adapter.find_by_names(["*"])
        restart_container(setup_compose, "ioc1-1")
        assert wait_for_sync(cf_adapter, lambda adapter: check_channel_property(adapter, DEFAULT_CHANNEL_NAME))
        channels_end = cf_adapter.find_by_names(["*"])
        assert len(channels_begin) == len(channels_end)
        channels_match(channels_begin, channels_end, PROPERTIES_TO_MATCH + [test_property.name])


def check_connection_active(adapter: PyCFClientAdapter) -> bool:
    try:
        adapter.find_by_names(["*"])
    except Exception:
        return False
    return True


class TestRestartChannelFinder:
    def test_status_property_works_after_cf_restart(
        self,
        setup_compose: DockerCompose,  # noqa: F811
        cf_adapter: PyCFClientAdapter,
    ) -> None:
        restart_container(setup_compose, "cf")
        refreshed_adapter = create_adapter_from_compose(setup_compose)
        assert wait_for_sync(refreshed_adapter, check_connection_active)

        shutdown_container(setup_compose, "ioc1-1")
        assert wait_for_sync(
            refreshed_adapter,
            lambda adapter: check_channel_property(adapter, DEFAULT_CHANNEL_NAME, INACTIVE_PROPERTY),
        )
        channels_inactive = find_ioc_channels(refreshed_adapter, "IOC1-1")
        assert all(INACTIVE_PROPERTY in channel.properties for channel in channels_inactive)


class TestShutdownChannelFinder:
    def test_status_property_works_between_cf_down(
        self,
        setup_compose: DockerCompose,  # noqa: F811
        cf_adapter: PyCFClientAdapter,
    ) -> None:
        cf_container_id = shutdown_container(setup_compose, "cf")
        time.sleep(10)  # Wait to ensure CF is down while IOC is down

        shutdown_container(setup_compose, "ioc1-1")
        time.sleep(10)  # Wait to ensure CF is down while IOC is down
        start_container(setup_compose, container_id=cf_container_id)
        refreshed_adapter = create_adapter_from_compose(setup_compose)
        assert wait_for_sync(refreshed_adapter, check_connection_active)

        assert wait_for_sync(
            refreshed_adapter,
            lambda adapter: check_channel_property(adapter, DEFAULT_CHANNEL_NAME, INACTIVE_PROPERTY),
        )
        channels_inactive = find_ioc_channels(refreshed_adapter, "IOC1-1")
        assert all(INACTIVE_PROPERTY in channel.properties for channel in channels_inactive)


class TestCleanStopRecceiver:
    def test_clean_stop_marks_channels_inactive(
        self, setup_compose: DockerCompose, cf_adapter: PyCFClientAdapter
    ) -> None:  # noqa: F811
        shutdown_container(setup_compose, "recc1")
        assert wait_for_sync(
            cf_adapter,
            lambda adapter: check_channel_property(adapter, DEFAULT_CHANNEL_NAME, INACTIVE_PROPERTY),
        )
        channels_inactive = find_ioc_channels(cf_adapter, "IOC1-1")
        assert all(INACTIVE_PROPERTY in channel.properties for channel in channels_inactive)


class TestCleanStartRecceiver:
    def test_startup_sweep_marks_stale_channels_inactive(
        self, setup_compose: DockerCompose, cf_adapter: PyCFClientAdapter
    ) -> None:  # noqa: F811
        # SIGKILL bypasses cleanOnStop, leaving channels Active in CF.
        receiver_id = kill_container(setup_compose, "recc1")
        shutdown_container(setup_compose, "ioc1-1")
        start_container(setup_compose, container_id=receiver_id)
        assert wait_for_sync(
            cf_adapter,
            lambda adapter: check_channel_property(adapter, DEFAULT_CHANNEL_NAME, INACTIVE_PROPERTY),
        )
        channels_inactive = find_ioc_channels(cf_adapter, "IOC1-1")
        assert all(INACTIVE_PROPERTY in channel.properties for channel in channels_inactive)


class TestMoveIocHost:
    def test_move_ioc_host(
        self,
        setup_compose: DockerCompose,  # noqa: F811
        cf_adapter: PyCFClientAdapter,
    ) -> None:
        channels_begin = cf_adapter.find_by_names(["*"])
        clone_container(setup_compose, "ioc1-1-new", host_name="ioc1-1")
        wait_for_sync(cf_adapter, lambda adapter: check_channel_property(adapter, DEFAULT_CHANNEL_NAME))
        channels_end = cf_adapter.find_by_names(["*"])
        assert len(channels_begin) == len(channels_end)
