import logging
import time
from typing import Callable, Sequence, Union

from channelfinder import ChannelFinderClient
from testcontainers.compose import DockerCompose

from recceiver.cf.adapter import PyCFClientAdapter
from recceiver.cf.model import CFChannel, CFProperty, CFPropertyName, PVStatus

LOG: logging.Logger = logging.getLogger(__name__)

ACTIVE_PROPERTY = CFProperty(CFPropertyName.PV_STATUS.value, "admin", PVStatus.ACTIVE.value)
INACTIVE_PROPERTY = CFProperty(CFPropertyName.PV_STATUS.value, "admin", PVStatus.INACTIVE.value)
MAX_WAIT_SECONDS = 180
TIME_PERIOD_INCREMENT = 2
DEFAULT_CHANNEL_NAME = "IOC1-1:ai:test"

BASE_ALIAS_COUNT = 1
BASE_RECORD_COUNT = 1
BASE_IOC_CHANNEL_COUNT = BASE_ALIAS_COUNT + BASE_RECORD_COUNT


def channel_match(
    channel0: CFChannel, channel1: CFChannel, properties_to_match: Sequence[Union[CFPropertyName, str]]
) -> None:
    assert channel0.name == channel1.name
    assert channel0.owner == channel1.owner
    property_names = {name.value if isinstance(name, CFPropertyName) else name for name in properties_to_match}
    for prop in channel0.properties:
        if prop.name in property_names:
            assert prop in channel1.properties, f"Property {prop} not found in channel {channel1.name}"


def channels_match(
    channels_begin: list[CFChannel],
    channels_end: list[CFChannel],
    properties_to_match: Sequence[Union[CFPropertyName, str]],
) -> None:
    for index, channel in enumerate(channels_begin):
        channel_match(channel, channels_end[index], properties_to_match)


def check_channel_count(adapter: PyCFClientAdapter, expected_channel_count: int, name: str = "*") -> bool:
    channels = adapter.find_by_names([name])
    LOG.debug("Found %s channels", len(channels))
    return len(channels) == expected_channel_count


def check_channel_property(adapter: PyCFClientAdapter, name: str = "*", prop: CFProperty = ACTIVE_PROPERTY) -> bool:
    channels = adapter.find_by_names([name])
    return all(prop in channel.properties for channel in channels)


def find_ioc_channels(adapter: PyCFClientAdapter, ioc_name: str) -> list[CFChannel]:
    return [
        channel
        for channel in adapter.find_by_names(["*"])
        if any(prop.name == CFPropertyName.IOC_NAME.value and prop.value == ioc_name for prop in channel.properties)
    ]


def wait_for_sync(adapter: PyCFClientAdapter, check: Callable[[PyCFClientAdapter], bool]) -> bool:
    time_period_to_wait_seconds = 1
    total_seconds_waited = 0
    while total_seconds_waited < MAX_WAIT_SECONDS:
        if check(adapter):
            return True
        time.sleep(time_period_to_wait_seconds)
        total_seconds_waited += time_period_to_wait_seconds
        time_period_to_wait_seconds += TIME_PERIOD_INCREMENT
    return False


def create_adapter_and_wait(compose: DockerCompose, expected_channel_count: int) -> PyCFClientAdapter:
    LOG.info("Waiting for channels to sync")
    adapter = create_adapter_from_compose(compose)
    assert wait_for_sync(adapter, lambda adapter: check_channel_count(adapter, expected_channel_count))
    return adapter


def create_adapter_from_compose(compose: DockerCompose) -> PyCFClientAdapter:
    cf_host, cf_port = compose.get_service_host_and_port("cf", 8080)
    cf_url = f"http://{cf_host if cf_host else 'localhost'}:{cf_port}/ChannelFinder"
    LOG.info("CF URL: %s", cf_url)
    return PyCFClientAdapter(
        ChannelFinderClient(BaseURL=cf_url, username="admin", password="password"), size_limit=10000
    )
