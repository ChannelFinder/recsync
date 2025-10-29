import logging
from pathlib import Path
from typing import Optional

import pytest
from testcontainers.compose import DockerCompose

from docker import DockerClient

LOG: logging.Logger = logging.getLogger(__name__)


def test_compose(compose_file=Path("docker") / Path("test-multi-recc.yml")) -> DockerCompose:
    current_path = Path(__file__).parent.resolve()

    return DockerCompose(
        str(current_path.parent.resolve()),
        compose_file_name=str(current_path.parent.joinpath(compose_file).resolve()),
        build=True,
    )


def fetch_containers_and_log_logs(compose: DockerCompose) -> None:
    docker_client = DockerClient()
    conts = {container.ID: container for container in compose.get_containers()}
    for cont_id, cont in conts.items():
        log = docker_client.containers.get(cont_id).logs()
        LOG.debug("Info for container %s", cont)
        LOG.debug("Logs for container %s", cont.Name)
        LOG.debug(log.decode("utf-8"))


@pytest.fixture(scope="class")
def setup_compose():
    LOG.info("Setup test environment")
    compose = test_compose()
    compose.start()
    yield compose
    LOG.info("Teardown test environment")
    LOG.info("Stopping docker compose")
    if LOG.level <= logging.DEBUG:
        fetch_containers_and_log_logs(compose)
    compose.stop()


def restart_container(compose: DockerCompose, host_name: str) -> str:
    container = compose.get_container(host_name)
    docker_client = DockerClient()
    docker_client.containers.get(container.ID).stop()
    docker_client.containers.get(container.ID).start()
    return container.ID


def shutdown_container(compose: DockerCompose, host_name: str) -> str:
    container = compose.get_container(host_name)
    docker_client = DockerClient()
    docker_client.containers.get(container.ID).stop()
    return container.ID


def start_container(
    compose: DockerCompose, host_name: Optional[str] = None, container_id: Optional[str] = None
) -> None:
    container_id = container_id or compose.get_container(host_name).ID
    if container_id:
        docker_client = DockerClient()
        docker_client.containers.get(container_id).start()
