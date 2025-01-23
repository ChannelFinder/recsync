import pathlib
import time
from testcontainers.compose import DockerCompose

from channelfinder import ChannelFinderClient


def fullSetupDockerCompose() -> DockerCompose:
    current_path = pathlib.Path(__file__).parent.resolve()

    return DockerCompose(
        str(current_path.parent.resolve()),
        compose_file_name=str(
            current_path.parent.joinpath("docker-compose.yml").resolve()
        ),
    )


def test_smoke() -> None:
    """
    Test that the setup in the docker compose creates channels in channelfinder
    """
    with fullSetupDockerCompose() as compose:
        compose.start()
        # wait for channels to sync
        time.sleep(10)
        cf_client = ChannelFinderClient()
        channels = cf_client.find(name="*")
        assert len(channels) == 24
        assert channels[0]["name"] == "IOC1-1::li"
