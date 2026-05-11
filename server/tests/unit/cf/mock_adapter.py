from typing import Any, Dict, List

from requests import HTTPError

from recceiver.cf.model import CFChannel, CFProperty, CFPropertyName, PVStatus

MOCK_CF_HTTP_ERROR = "Mock Channelfinder Client HTTPError"


class MockCFAdapter:
    """In-memory ChannelFinderAdapter for unit tests."""

    def __init__(self):
        self._channels: Dict[str, CFChannel] = {}
        self.connected = True
        self.fail_find = False
        self.fail_set = False

    def find_by_ioc_id(self, iocid: str) -> List[CFChannel]:
        if not self.connected or self.fail_find:
            raise HTTPError(MOCK_CF_HTTP_ERROR, response=self)
        return [
            ch
            for ch in self._channels.values()
            if any(p.name == CFPropertyName.IOC_ID.value and p.value == iocid for p in ch.properties)
        ]

    def find_by_names(self, names: List[str]) -> List[CFChannel]:
        if not self.connected or self.fail_find:
            raise HTTPError(MOCK_CF_HTTP_ERROR, response=self)
        return [self._channels[n] for n in names if n in self._channels]

    def find_active_for_recceiver(self, recceiverid: str) -> List[CFChannel]:
        if not self.connected or self.fail_find:
            raise HTTPError(MOCK_CF_HTTP_ERROR, response=self)
        return [
            ch
            for ch in self._channels.values()
            if any(p.name == CFPropertyName.PV_STATUS.value and p.value == PVStatus.ACTIVE.value for p in ch.properties)
            and any(p.name == CFPropertyName.RECCEIVER_ID.value and p.value == recceiverid for p in ch.properties)
        ]

    def set_channels(self, channels: List[CFChannel]) -> None:
        if not self.connected or self.fail_set:
            raise HTTPError(MOCK_CF_HTTP_ERROR, response=self)
        for channel in channels:
            self._channels[channel.name] = channel

    def update_property(self, prop: CFProperty, channel_names: List[str]) -> None:
        if not self.connected or self.fail_find:
            raise HTTPError(MOCK_CF_HTTP_ERROR, response=self)
        for name in channel_names:
            self._update_channel_with_prop(prop, name)

    def get_all_properties(self) -> List[Dict[str, Any]]:
        if not self.connected:
            raise HTTPError(MOCK_CF_HTTP_ERROR, response=self)
        return [{"name": n} for n in ("hostName", "iocName", "pvStatus", "time", "iocid", "iocIP", "recceiverID")]

    def set_property(self, _name: str, _owner: str) -> None:
        if not self.connected:
            raise HTTPError(MOCK_CF_HTTP_ERROR, response=self)

    def _update_channel_with_prop(self, prop: CFProperty, channel_name: str) -> None:
        if channel_name not in self._channels:
            return
        for p in self._channels[channel_name].properties:
            if p.name == prop.name:
                p.value = prop.value
                p.owner = prop.owner
                return
