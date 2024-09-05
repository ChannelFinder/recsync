import pathlib
import time
from docker import DockerClient
from testcontainers.compose import DockerCompose

from channelfinder import ChannelFinderClient
import logging

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    encoding="utf-8",
)

LOG: logging.Logger = logging.getLogger(__name__)

MAX_WAIT_SECONDS = 180
EXPECTED_DEFAULT_CHANNELS = 24


def fullSetupDockerCompose() -> DockerCompose:
    current_path = pathlib.Path(__file__).parent.resolve()

    return DockerCompose(
        str(current_path.parent.resolve()),
        compose_file_name=str(
            current_path.parent.joinpath("test-compose.yml").resolve()
        ),
        build=True,
    )


class TestE2E:
    compose: DockerCompose

    def setup_method(self) -> None:
        """Setup the test environment"""
        LOG.info("Setting up test")
        self.compose = fullSetupDockerCompose()
        self.compose.start()

    def teardown_method(self) -> None:
        """Teardown the test environment"""

        LOG.info("Tearing down test")
        if self.compose:
            LOG.info("Stopping docker compose")
            if LOG.level <= logging.DEBUG:
                docker_client = DockerClient()
                conts = {
                    container.ID: container
                    for container in self.compose.get_containers()
                }
                for cont_id, cont in conts.items():
                    log = docker_client.containers.get(cont_id).logs()
                    LOG.debug("Info for container %s", cont)
                    LOG.debug("Logs for container %s", cont.Name)
                    LOG.debug(log.decode("utf-8"))
            self.compose.stop()

    def test_smoke(self) -> None:
        """
        Test that the setup in the docker compose creates channels in channelfinder
        """
        LOG.info("Waiting for channels to sync")
        cf_host, cf_port = self.compose.get_service_host_and_port("cf")
        cf_url = f"http://{cf_host if cf_host else 'localhost'}:{cf_port}/ChannelFinder"
        # wait for channels to sync
        LOG.info("CF URL: %s", cf_url)
        cf_client = ChannelFinderClient(BaseURL=cf_url)
        self.wait_for_sync(cf_client)
        channels = cf_client.find(name="*")
        assert len(channels) == EXPECTED_DEFAULT_CHANNELS
        assert channels[0]["name"] == "IOC1-1:Msg-I"

    def wait_for_sync(self, cf_client):
        seconds_to_wait = 1
        total_seconds_waited = 0
        while total_seconds_waited < MAX_WAIT_SECONDS:
            try:
                channels = cf_client.find(name="*")
                LOG.info(
                    "Found %s in %s seconds",
                    len(channels),
                    total_seconds_waited,
                )
                if len(channels) == EXPECTED_DEFAULT_CHANNELS:
                    break
            except Exception as e:
                LOG.error(e)
            time.sleep(seconds_to_wait)
            total_seconds_waited += seconds_to_wait
            seconds_to_wait += 2
