import logging
import threading
from pathlib import Path
from typing import Optional

from channelfinder import ChannelFinderClient
from testcontainers.compose import DockerCompose

from docker import DockerClient
from docker.models.containers import Container

from .client_checks import (
    DEFAULT_CHANNEL_NAME,
    INACTIVE_PROPERTY,
    check_channel_property,
    create_client_and_wait,
    wait_for_sync,
)
from .docker import ComposeFixtureFactory

LOG: logging.Logger = logging.getLogger(__name__)

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    encoding="utf-8",
)

setup_compose = ComposeFixtureFactory(Path("tests") / "docker" / "test-bash-ioc.yml").return_fixture()


def docker_exec_new_command(container: Container, command: str, env: Optional[dict] = None) -> None:
    def stream_logs(exec_result, cmd: str):
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


def start_ioc(setup_compose: DockerCompose, db_file: Optional[str] = None) -> Container:
    ioc_container = setup_compose.get_container("ioc1-1")
    docker_client = DockerClient()
    docker_ioc = docker_client.containers.get(ioc_container.ID)
    docker_exec_new_command(docker_ioc, "./demo /ioc/st.cmd", env={"DB_FILE": db_file} if db_file else None)
    return docker_ioc


def restart_ioc(
    ioc_container: Container, cf_client: ChannelFinderClient, channel_name: str, new_db_file: str
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
    assert wait_for_sync(cf_client, lambda cf_client: check_channel_property(cf_client, name=channel_name))
    LOG.debug("ioc1-1 has restarted and synced")


class TestRemoveInfoTag:
    def test_remove_infotag(self, setup_compose: DockerCompose) -> None:  # noqa: F811
        """
        Test that removing an infotag from a record works
        """
        test_channel_count = 1
        # Arrange
        docker_ioc = start_ioc(setup_compose, db_file="test_remove_infotag_before.db")
        LOG.info("Waiting for channels to sync")
        cf_client = create_client_and_wait(setup_compose, expected_channel_count=test_channel_count)

        # Check ioc1-1 has ai:test with info tag "archive"
        LOG.debug('Checking ioc1-1 has ai:test with info tag "archive"')
        channels = cf_client.find(name=DEFAULT_CHANNEL_NAME)
        TEST_INFO_TAG = {"archive": "testing", "owner": "admin", "value": "Active", "channels": []}

        assert len(channels) == test_channel_count
        assert TEST_INFO_TAG in channels[0]["properties"]

        # Act
        restart_ioc(docker_ioc, cf_client, DEFAULT_CHANNEL_NAME, "test_remove_infotag_after.db")

        # Assert
        channels = cf_client.find(name=DEFAULT_CHANNEL_NAME)
        LOG.debug("archive channels: %s", channels)
        assert len(channels) == test_channel_count
        assert TEST_INFO_TAG not in channels[0]["properties"]

