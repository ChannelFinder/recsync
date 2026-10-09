import logging
import threading
from pathlib import Path
from typing import Optional

from docker.models.containers import Container
from testcontainers.compose import DockerCompose

from docker import DockerClient
from recceiver.cf.adapter import PyCFClientAdapter
from recceiver.cf.model import CFProperty

from .cf_client import (
    BASE_IOC_CHANNEL_COUNT,
    DEFAULT_CHANNEL_NAME,
    INACTIVE_PROPERTY,
    check_channel_property,
    create_adapter_and_wait,
    wait_for_sync,
)
from .docker_compose import ComposeFixtureFactory

LOG: logging.Logger = logging.getLogger(__name__)

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    encoding="utf-8",
)

setup_compose = ComposeFixtureFactory(
    Path("tests/integration/resources/docker-compose/test-bash-ioc.yml")
).return_fixture()


def docker_exec_new_command(container: Container, command: str, env: Optional[dict] = None) -> None:
    def stream_logs(exec_result, cmd: str):
        if LOG.level <= logging.DEBUG:
            LOG.debug("Logs from %s with command %s", container.name, cmd)
            for line in exec_result.output:
                LOG.debug(line.decode().strip())

    exec_result = container.exec_run(command, tty=True, stream=True, environment=env)
    log_thread = threading.Thread(target=stream_logs, args=(exec_result, command))
    log_thread.start()


def start_ioc(setup_compose: DockerCompose, db_file: Optional[str] = None) -> Container:
    ioc_container = setup_compose.get_container("ioc1-1")
    docker_client = DockerClient()
    docker_ioc = docker_client.containers.get(ioc_container.ID)
    docker_exec_new_command(docker_ioc, "./demo /ioc/st.cmd", env={"DB_FILE": db_file} if db_file else None)
    return docker_ioc


def restart_ioc(ioc_container: Container, cf_adapter: PyCFClientAdapter, channel_name: str, new_db_file: str) -> None:
    ioc_container.stop()
    LOG.info("Waiting for channels to go inactive")
    assert wait_for_sync(
        cf_adapter,
        lambda adapter: check_channel_property(adapter, name=channel_name, prop=INACTIVE_PROPERTY),
    )
    ioc_container.start()
    docker_exec_new_command(ioc_container, "./demo /ioc/st.cmd", env={"DB_FILE": new_db_file})

    LOG.debug("ioc1-1 restart")
    assert wait_for_sync(cf_adapter, lambda adapter: check_channel_property(adapter, name=channel_name)), (
        "ioc1-1 failed to restart and sync"
    )


class TestRemoveInfoTag:
    def test_remove_infotag(self, setup_compose: DockerCompose) -> None:
        """Removing an infotag from a record removes its CF property."""
        docker_ioc = start_ioc(setup_compose, db_file="test_remove_infotag_before.db")
        cf_adapter = create_adapter_and_wait(setup_compose, expected_channel_count=1)
        info_tag = CFProperty("archive", "admin", "testing")
        channels = cf_adapter.find_by_names([DEFAULT_CHANNEL_NAME])
        assert any(channel.property(info_tag.name) == info_tag for channel in channels), (
            "Info tag 'archive' not found before removal"
        )

        restart_ioc(docker_ioc, cf_adapter, DEFAULT_CHANNEL_NAME, "test_remove_infotag_after.db")

        channels = cf_adapter.find_by_names([DEFAULT_CHANNEL_NAME])
        assert all(not channel.has_property(info_tag) for channel in channels), (
            "Info tag 'archive' still found in channel after removal"
        )


class TestRemoveChannel:
    def test_remove_channel(self, setup_compose: DockerCompose) -> None:  # noqa: F811
        """Removing a channel marks it Inactive while keeping the live record Active."""
        docker_ioc = start_ioc(setup_compose, db_file="test_remove_channel_before.db")
        cf_adapter = create_adapter_and_wait(setup_compose, expected_channel_count=2)
        second_channel_name = f"{DEFAULT_CHANNEL_NAME}-2"
        check_channel_property(cf_adapter, name=DEFAULT_CHANNEL_NAME)
        check_channel_property(cf_adapter, name=second_channel_name)

        restart_ioc(docker_ioc, cf_adapter, DEFAULT_CHANNEL_NAME, "test_remove_channel_after.db")

        check_channel_property(cf_adapter, name=second_channel_name, prop=INACTIVE_PROPERTY)
        check_channel_property(cf_adapter, name=DEFAULT_CHANNEL_NAME)


class TestRemoveAlias:
    def test_remove_alias(self, setup_compose: DockerCompose) -> None:  # noqa: F811
        """Removing an alias marks it Inactive while keeping its record Active."""
        docker_ioc = start_ioc(setup_compose)
        cf_adapter = create_adapter_and_wait(setup_compose, expected_channel_count=BASE_IOC_CHANNEL_COUNT)
        channel_alias_name = f"{DEFAULT_CHANNEL_NAME}:alias"
        check_channel_property(cf_adapter, name=DEFAULT_CHANNEL_NAME)
        check_channel_property(cf_adapter, name=channel_alias_name)

        restart_ioc(docker_ioc, cf_adapter, DEFAULT_CHANNEL_NAME, "test_remove_alias_after.db")

        check_channel_property(cf_adapter, name=DEFAULT_CHANNEL_NAME)
        check_channel_property(cf_adapter, name=channel_alias_name, prop=INACTIVE_PROPERTY)
