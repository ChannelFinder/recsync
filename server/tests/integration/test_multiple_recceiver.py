import logging
from pathlib import Path

import pytest
from testcontainers.compose import DockerCompose

from recceiver.cf.adapter import PyCFClientAdapter
from recceiver.cf.model import CFProperty, CFPropertyName

from .cf_client import BASE_ALIAS_COUNT, BASE_IOC_CHANNEL_COUNT, DEFAULT_CHANNEL_NAME, create_adapter_and_wait
from .docker_compose import ComposeFixtureFactory

LOG: logging.Logger = logging.getLogger(__name__)

RECSYNC_RESTART_DELAY = 30
IOC_COUNT = 4
EXPECTED_DEFAULT_CHANNEL_COUNT = IOC_COUNT * BASE_IOC_CHANNEL_COUNT

setup_compose = ComposeFixtureFactory(
    Path("tests/integration/resources/docker-compose/test-multi-recc.yml")
).return_fixture()


@pytest.fixture(scope="class")
def cf_adapter(setup_compose: DockerCompose) -> PyCFClientAdapter:  # noqa: F811
    return create_adapter_and_wait(setup_compose, EXPECTED_DEFAULT_CHANNEL_COUNT)


class TestMultipleRecceiver:
    def test_number_of_channels_and_channel_name(self, cf_adapter: PyCFClientAdapter) -> None:
        channels = cf_adapter.find_by_names(["*"])
        assert len(channels) == EXPECTED_DEFAULT_CHANNEL_COUNT
        assert channels[0].name == DEFAULT_CHANNEL_NAME

    def test_number_of_aliases_and_alais_property(self, cf_adapter: PyCFClientAdapter) -> None:
        aliases = [
            channel
            for channel in cf_adapter.find_by_names(["*"])
            if any(prop.name == CFPropertyName.ALIAS.value for prop in channel.properties)
        ]
        assert len(aliases) == IOC_COUNT * BASE_ALIAS_COUNT
        assert aliases[0].name == DEFAULT_CHANNEL_NAME + ":alias"
        assert CFProperty(CFPropertyName.ALIAS.value, "admin", DEFAULT_CHANNEL_NAME) in aliases[0].properties

    def test_number_of_record_desc_and_property(self, cf_adapter: PyCFClientAdapter) -> None:
        channels = [
            channel
            for channel in cf_adapter.find_by_names(["*"])
            if any(prop.name == CFPropertyName.RECORD_DESC.value for prop in channel.properties)
        ]
        assert len(channels) == EXPECTED_DEFAULT_CHANNEL_COUNT
        expected = CFProperty(CFPropertyName.RECORD_DESC.value, "admin", "testdesc")
        assert expected in channels[0].properties

    def test_number_of_record_type_and_property(self, cf_adapter: PyCFClientAdapter) -> None:
        channels = [
            channel
            for channel in cf_adapter.find_by_names(["*"])
            if any(prop.name == CFPropertyName.RECORD_TYPE.value for prop in channel.properties)
        ]
        assert len(channels) == EXPECTED_DEFAULT_CHANNEL_COUNT
        expected = CFProperty(CFPropertyName.RECORD_TYPE.value, "admin", "ai")
        assert expected in channels[0].properties
