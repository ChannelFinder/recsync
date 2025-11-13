import logging
import threading
from pathlib import Path

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


def docker_exec_new_command(container: Container, command: str, env: dict | None = None) -> None:
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


class TestRemoveProperty:
    def test_remove_property(self, setup_compose: DockerCompose) -> None:  # noqa: F811
        """
        Test that the setup in the docker compose creates channels in channelfinder
        """
        ioc_container = setup_compose.get_container("ioc1-1")
        docker_client = DockerClient()
        docker_ioc = docker_client.containers.get(ioc_container.ID)
        docker_exec_new_command(docker_ioc, "./demo /ioc/st.cmd")

        LOG.info("Waiting for channels to sync")
        cf_client = create_client_and_wait(setup_compose, expected_channel_count=2)

        # Check ioc1-1 has ai:test with info tag "archive"
        LOG.debug('Checking ioc1-1 has ai:test with info tag "archive"')
        channel = cf_client.find(name=DEFAULT_CHANNEL_NAME)

        def get_len_archive_properties(channel):
            return len([prop for prop in channel[0]["properties"] if prop["name"] == "archive"])

        assert get_len_archive_properties(channel) == 1

        docker_ioc.stop()
        LOG.info("Waiting for channels to go inactive")
        assert wait_for_sync(
            cf_client,
            lambda cf_client: check_channel_property(cf_client, name=DEFAULT_CHANNEL_NAME, prop=INACTIVE_PROPERTY),
        )
        docker_ioc.start()

        docker_exec_new_command(docker_ioc, "./demo /ioc/st.cmd", env={"DB_FILE": "test_remove_infotag.db"})
        # Detach by not waiting for the thread to finish

        LOG.debug("ioc1-1 restart")
        assert wait_for_sync(cf_client, lambda cf_client: check_channel_property(cf_client, name=DEFAULT_CHANNEL_NAME))
        LOG.debug("ioc1-1 has restarted and synced")

        channel = cf_client.find(name=DEFAULT_CHANNEL_NAME)
        LOG.debug("archive channel: %s", channel)
        assert get_len_archive_properties(channel) == 0
