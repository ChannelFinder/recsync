import logging
import time
from pathlib import Path
from uuid import uuid4

import pytest
from channelfinder import ChannelFinderClient
from testcontainers.compose import DockerCompose

from recceiver.cf.adapter import PyCFClientAdapter
from recceiver.cf.model import CFChannel, CFProperty, CFPropertyName, PVStatus

from .cf_client import (
    BASE_IOC_CHANNEL_COUNT,
    DEFAULT_CHANNEL_NAME,
    INACTIVE_PROPERTY,
    channels_match,
    check_channel_property,
    create_client_and_wait,
    create_client_from_compose,
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

PROPERTIES_TO_MATCH = ["pvStatus", "recordType", "recordDesc", "alias", "hostName", "iocName", "recceiverID"]

LOG: logging.Logger = logging.getLogger(__name__)

setup_compose = ComposeFixtureFactory(
    Path("tests/integration/resources/docker-compose/test-single-ioc.yml")
).return_fixture()


@pytest.fixture(scope="class")
def cf_client(setup_compose: DockerCompose) -> ChannelFinderClient:  # noqa: F811
    return create_client_and_wait(setup_compose, expected_channel_count=BASE_IOC_CHANNEL_COUNT)


@pytest.fixture(scope="class")
def cf_adapter(setup_compose: DockerCompose) -> PyCFClientAdapter:  # noqa: F811
    return PyCFClientAdapter(create_client_and_wait(setup_compose, expected_channel_count=BASE_IOC_CHANNEL_COUNT))


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
        refreshed_cf_client = create_client_from_compose(setup_compose)
        assert wait_for_sync(refreshed_cf_client, check_connection_active)

        # Assert
        shutdown_container(setup_compose, "ioc1-1")
        assert wait_for_sync(
            refreshed_cf_client,
            lambda client: check_channel_property(client, DEFAULT_CHANNEL_NAME, INACTIVE_PROPERTY),
        )
        channels_inactive = refreshed_cf_client.find(property=[("iocName", "IOC1-1")])
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
        refreshed_cf_client = create_client_from_compose(setup_compose)
        assert wait_for_sync(refreshed_cf_client, check_connection_active)

        # Assert
        assert wait_for_sync(
            refreshed_cf_client,
            lambda client: check_channel_property(client, DEFAULT_CHANNEL_NAME, INACTIVE_PROPERTY),
        )
        channels_inactive = refreshed_cf_client.find(property=[("iocName", "IOC1-1")])
        assert all(INACTIVE_PROPERTY in ch["properties"] for ch in channels_inactive)


class TestCleanStopRecceiver:
    def test_clean_stop_marks_channels_inactive(
        self, setup_compose: DockerCompose, cf_client: ChannelFinderClient
    ) -> None:  # noqa: F811
        shutdown_container(setup_compose, "recc1")
        assert wait_for_sync(
            cf_client,
            lambda client: check_channel_property(client, DEFAULT_CHANNEL_NAME, INACTIVE_PROPERTY),
        )
        channels_inactive = cf_client.find(property=[("iocName", "IOC1-1")])
        assert all(INACTIVE_PROPERTY in ch["properties"] for ch in channels_inactive)


class TestCleanStartRecceiver:
    def test_startup_sweep_marks_stale_channels_inactive(
        self, setup_compose: DockerCompose, cf_client: ChannelFinderClient
    ) -> None:  # noqa: F811
        # Kill recceiver hard — cleanOnStop does NOT run, channels stay Active in CF
        recc1_id = kill_container(setup_compose, "recc1")
        # Stop IOC so it cannot reconnect when the recceiver comes back
        shutdown_container(setup_compose, "ioc1-1")
        # Start recceiver — cleanOnStart sweep should mark the stale channels Inactive
        start_container(setup_compose, container_id=recc1_id)
        assert wait_for_sync(
            cf_client,
            lambda client: check_channel_property(client, DEFAULT_CHANNEL_NAME, INACTIVE_PROPERTY),
        )
        channels_inactive = cf_client.find(property=[("iocName", "IOC1-1")])
        assert all(INACTIVE_PROPERTY in ch["properties"] for ch in channels_inactive)


class TestLiveIOCRestartRecceiver:
    def test_restart_restores_live_channels_and_cleans_stale_channels(
        self, setup_compose: DockerCompose, cf_adapter: PyCFClientAdapter
    ) -> None:  # noqa: F811
        baseline = cf_adapter.find_by_names(["*"])
        assert len(baseline) == BASE_IOC_CHANNEL_COUNT
        expected_names = {channel.name for channel in baseline}
        live_names = sorted(expected_names)
        receiver = baseline[0].property(CFPropertyName.RECCEIVER_ID)
        assert receiver is not None and receiver.value is not None
        active = CFProperty(CFPropertyName.PV_STATUS.value, "admin", PVStatus.ACTIVE.value)
        inactive = CFProperty(CFPropertyName.PV_STATUS.value, "admin", PVStatus.INACTIVE.value)
        stale_name = f"RECSYNC:STALE:{uuid4().hex}"
        receiver_id = setup_compose.get_container("recc1").ID

        def recovered(adapter: PyCFClientAdapter) -> bool:
            channels = adapter.find_active_for_recceiver(receiver.value)
            stale = adapter.find_by_names([stale_name])
            return (
                {channel.name for channel in channels} == expected_names
                and all(
                    channel.property_value(CFPropertyName.TIME) not in (None, previous_times[channel.name])
                    for channel in channels
                )
                and len(stale) == 1
                and stale[0].has_property(inactive)
            )

        assert wait_for_sync(
            cf_adapter,
            lambda adapter: {ch.name for ch in adapter.find_active_for_recceiver(receiver.value)} == expected_names,
        ), "IOC channels did not become Active before restart"
        receiver_stopped = False
        try:
            # The class-scoped Compose fixture removes the seeded channel along
            # with the rest of the ChannelFinder data during teardown.
            cf_adapter.set_channels([CFChannel(stale_name, "admin", [receiver, active])])
            assert wait_for_sync(
                cf_adapter,
                lambda adapter: any(ch.has_property(active) for ch in adapter.find_by_names([stale_name])),
            )
            # Leave the IOC running so its upload competes with startup cleanup.
            kill_container(setup_compose, "recc1")
            receiver_stopped = True
            # Startup cleanup changes only pvStatus. A changed timestamp proves
            # each live channel was uploaded again, rather than left Active.
            previous_times = {
                channel.name: channel.property_value(CFPropertyName.TIME)
                for channel in cf_adapter.find_by_names(live_names)
            }
            assert set(previous_times) == expected_names
            assert all(value is not None for value in previous_times.values()), "Live channels are missing timestamps"
            start_container(setup_compose, container_id=receiver_id)
            receiver_stopped = False
            assert wait_for_sync(cf_adapter, recovered), "Fresh IOC upload and stale-channel cleanup did not complete"
        finally:
            if receiver_stopped:
                start_container(setup_compose, container_id=receiver_id)


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
