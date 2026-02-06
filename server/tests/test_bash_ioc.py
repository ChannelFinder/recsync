import logging
import threading
from pathlib import Path

from channelfinder import ChannelFinderClient
from docker.models.containers import Container, ExecResult
from testcontainers.compose import DockerCompose

from docker import DockerClient

from .client_checks import (
    BASE_IOC_CHANNEL_COUNT,
    DEFAULT_CHANNEL_NAME,
    INACTIVE_PROPERTY,
    check_channel_property,
    create_client_and_wait,
    wait_for_sync,
)
from .docker_utils import ComposeFixtureFactory

LOG: logging.Logger = logging.getLogger(__name__)

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    encoding="utf-8",
)

setup_compose = ComposeFixtureFactory(Path("tests") / "docker" / "test-bash-ioc.yml").return_fixture()


def docker_exec_new_command(container: Container, command: str, env: dict | None = None) -> None:
    def stream_logs(exec_result: ExecResult, cmd: str) -> None:
        if LOG.level <= logging.DEBUG:
            LOG.debug("Logs from %s with command %s", container.name, cmd)
            for line in exec_result.output:
                LOG.debug(line.decode().strip())

    exec_result = container.exec_run(command, tty=True, stream=True, environment=env)
    log_thread = threading.Thread(
        target=stream_logs,
        args=(
            exec_result,
            command,
        ),
    )
    log_thread.start()


def start_ioc(setup_compose: DockerCompose, db_file: str | None = None) -> Container:
    ioc_container = setup_compose.get_container("ioc1-1")
    docker_client = DockerClient()
    docker_ioc = docker_client.containers.get(ioc_container.ID)
    docker_exec_new_command(docker_ioc, "./demo /ioc/st.cmd", env={"DB_FILE": db_file} if db_file else None)
    return docker_ioc


def restart_ioc(
    ioc_container: Container,
    cf_client: ChannelFinderClient,
    channel_name: str,
    new_db_file: str,
) -> Container:
    ioc_container.stop()
    LOG.info("Waiting for channels to go inactive")
    assert wait_for_sync(
        cf_client,
        lambda cf_client: check_channel_property(cf_client, name=channel_name, prop=INACTIVE_PROPERTY),
    )
    ioc_container.start()

    docker_exec_new_command(ioc_container, "./demo /ioc/st.cmd", env={"DB_FILE": new_db_file})
    # Detach by not waiting for the thread to finish

    LOG.debug("ioc1-1 restart")
    assert wait_for_sync(cf_client, lambda cf_client: check_channel_property(cf_client, name=channel_name)), (
        "ioc1-1 failed to restart and sync"
    )


class TestRemoveInfoTag:
    def test_remove_infotag(self, setup_compose: DockerCompose) -> None:
        """Test that removing an infotag from a record works"""
        test_channel_count = 1
        # Arrange
        docker_ioc = start_ioc(setup_compose, db_file="test_remove_infotag_before.db")
        LOG.info("Waiting for channels to sync")
        cf_client = create_client_and_wait(setup_compose, expected_channel_count=test_channel_count)

        # Check ioc1-1 has ai:test with info tag "archive"
        LOG.debug('Checking ioc1-1 has ai:test with info tag "archive"')
        channels = cf_client.find(name=DEFAULT_CHANNEL_NAME)
        TEST_INFO_TAG = {"name": "archive", "owner": "admin", "value": "testing", "channels": []}

        assert any(TEST_INFO_TAG in ch["properties"] for ch in channels), (
            "Info tag 'archive' not found in channel before removal"
        )

        # Act
        restart_ioc(docker_ioc, cf_client, DEFAULT_CHANNEL_NAME, "test_remove_infotag_after.db")

        # Assert
        channels = cf_client.find(name=DEFAULT_CHANNEL_NAME)
        LOG.debug("archive channels: %s", channels)
        assert all(TEST_INFO_TAG not in ch["properties"] for ch in channels), (
            "Info tag 'archive' still found in channel after removal"
        )


class TestRemoveChannel:
    def test_remove_channel(self, setup_compose: DockerCompose) -> None:
        """Test that removing a channel works correctly."""
        # Arrange
        docker_ioc = start_ioc(setup_compose, db_file="test_remove_channel_before.db")
        LOG.info("Waiting for channels to sync")
        cf_client = create_client_and_wait(setup_compose, expected_channel_count=2)

        # Check ioc1-1 has base channel
        LOG.debug("Checking ioc1-1 has both channels before removal")
        check_channel_property(cf_client, name=DEFAULT_CHANNEL_NAME)
        second_channel_name = f"{DEFAULT_CHANNEL_NAME}-2"
        check_channel_property(cf_client, name=second_channel_name)

        # Act
        restart_ioc(docker_ioc, cf_client, DEFAULT_CHANNEL_NAME, "test_remove_channel_after.db")

        # Assert
        check_channel_property(cf_client, name=second_channel_name, prop=INACTIVE_PROPERTY)
        check_channel_property(cf_client, name=DEFAULT_CHANNEL_NAME)


class TestRemoveAlias:
    def test_remove_alias(self, setup_compose: DockerCompose) -> None:
        """Test that removing an alias works correctly."""
        # Arrange
        docker_ioc = start_ioc(setup_compose)
        LOG.info("Waiting for channels to sync")
        cf_client = create_client_and_wait(setup_compose, expected_channel_count=BASE_IOC_CHANNEL_COUNT)

        # Check before alias status
        LOG.debug('Checking ioc1-1 has ai:base_pv3 has an Active alias"')
        channel_alias_name = f"{DEFAULT_CHANNEL_NAME}:alias"
        check_channel_property(cf_client, name=DEFAULT_CHANNEL_NAME)
        check_channel_property(cf_client, name=channel_alias_name)

        # Act
        restart_ioc(docker_ioc, cf_client, DEFAULT_CHANNEL_NAME, "test_remove_alias_after.db")

        # Assert
        check_channel_property(cf_client, name=DEFAULT_CHANNEL_NAME)
        check_channel_property(cf_client, name=channel_alias_name, prop=INACTIVE_PROPERTY)
