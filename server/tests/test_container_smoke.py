import time
from pathlib import Path

import pytest

from docker import DockerClient

SERVER_DIR = Path(__file__).parent.parent


@pytest.fixture(scope="session")
def recceiver_image():
    client = DockerClient()
    image, _ = client.images.build(path=str(SERVER_DIR), tag="recceiver-test:smoke", rm=True)
    yield image.id
    client.images.remove(image.id, force=True)


class TestContainerSmoke:
    def test_container_starts_and_parses_config(self, recceiver_image):
        """Verify the container reaches the CF connection attempt rather than crashing during config parsing.

        A config parsing error (e.g. AttributeError on a missing ConfigAdapter method) causes the
        container to exit before CF_START is logged. A healthy startup logs CF_START first and only
        then fails to connect to CF.
        """
        client = DockerClient()
        container = client.containers.run(
            recceiver_image,
            environment={
                "RECCEIVER_RECCEIVER_PROCS": "cf",
                "RECCEIVER_RECCEIVER_LOGLEVEL": "INFO",
                "RECCEIVER_CF_BASEURL": "https://nonexistent-cf:8080/ChannelFinder",
                "RECCEIVER_CF_CFUSERNAME": "admin",
                "RECCEIVER_CF_CFPASSWORD": "password",
            },
            detach=True,
        )
        try:
            time.sleep(10)
            logs = container.logs().decode()
            assert "AttributeError" not in logs, f"Container crashed with AttributeError:\n{logs}"
            assert "CF_START" in logs, f"CF_START not found in logs — service may not have started:\n{logs}"
        finally:
            container.stop(timeout=5)
            container.remove()
