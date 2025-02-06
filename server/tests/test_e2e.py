import pathlib
import time
from testcontainers.compose import DockerCompose

from channelfinder import ChannelFinderClient
import logging

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    encoding="utf-8",
)

LOG: logging.Logger = logging.getLogger(__name__)


def fullSetupDockerCompose() -> DockerCompose:
    current_path = pathlib.Path(__file__).parent.resolve()

    return DockerCompose(
        str(current_path.parent.resolve()),
        compose_file_name=str(
            current_path.parent.joinpath("docker-compose.yml").resolve()
        ),
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
            LOG.debug(self.compose.get_logs())
            self.compose.stop()

    def test_smoke(self) -> None:
        """
        Test that the setup in the docker compose creates channels in channelfinder
        """
        LOG.info("Waiting for channels to sync")
        # wait for channels to sync
        cf_client = ChannelFinderClient()
        for seconds in range(10):
            try:
                channels = cf_client.find(name="*")
                LOG.info("Found %s in %s", len(channels), seconds)
                if len(channels) == 24:
                    break
            except Exception as e:
                LOG.error(e)
            time.sleep(1)
        channels = cf_client.find(name="*")
        assert len(channels) == 24
        assert channels[0]["name"] == "IOC1-1::li"
