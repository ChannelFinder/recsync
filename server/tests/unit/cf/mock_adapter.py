from typing import Any, Dict, List

from requests import HTTPError
from twisted.internet.address import IPv4Address

from recceiver.cf.model import CFChannel, CFProperty, CFPropertyName, PVStatus

MOCK_CF_HTTP_ERROR = "Mock Channelfinder Client HTTPError"


class MockCFAdapter:
    """In-memory ChannelFinderAdapter for unit tests."""

    def __init__(self):
        self._channels: Dict[str, CFChannel] = {}
        self.connected = True
        self.fail_find = False
        self.fail_set = False

    def find_by_args(self, args: List) -> List[CFChannel]:
        if not self.connected or self.fail_find:
            raise HTTPError(MOCK_CF_HTTP_ERROR, response=self)
        key, value = args[0]
        if key == CFPropertyName.IOC_ID:
            return self._find_by_iocid(key, value)
        if key == "~name":
            return self._find_by_names(str(value).split("|"))
        if key == CFPropertyName.PV_STATUS and value == PVStatus.ACTIVE:
            return self._find_active()
        return []

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

    def _find_by_iocid(self, key, value) -> List[CFChannel]:
        return [ch for ch in self._channels.values() if any(p.name == key and p.value == value for p in ch.properties)]

    def _find_by_names(self, names: List[str]) -> List[CFChannel]:
        return [self._channels[n] for n in names if n in self._channels]

    def _find_active(self) -> List[CFChannel]:
        return [
            ch
            for ch in self._channels.values()
            if any(p.name == CFPropertyName.PV_STATUS and p.value == PVStatus.ACTIVE for p in ch.properties)
        ]

    def _update_channel_with_prop(self, prop: CFProperty, channel_name: str) -> None:
        if channel_name not in self._channels:
            return
        for p in self._channels[channel_name].properties:
            if p.name == prop.name:
                p.value = prop.value
                p.owner = prop.owner
                return


class MockConfig:
    def get(self, _name, _target):
        return "cf-engi"


class MockTransaction:
    def __init__(self):
        self.addrec = {}
        self.src = IPv4Address("TCP", "testhosta", 1111)
        self.delrec = ()
        self.infos = {"CF_USERNAME": "cf-update", "ENGINEER": "cf-engi"}
        self.initial = True
        self.connected = True
        self.fail_set = False
        self.fail_find = False
        self.recinfos = {}
