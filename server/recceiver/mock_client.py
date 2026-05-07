from requests import HTTPError
from twisted.internet.address import IPv4Address

from recceiver.cfstore import CFPropertyName, PVStatus

MOCK_CF_HTTP_ERROR = "Mock Channelfinder Client HTTPError"


class MockCFClient:
    def __init__(self):
        self.cf = {}
        self.connected = True
        self.fail_find = False
        self.fail_set = False

    def _find_by_iocid(self, key, value):
        return [
            channel
            for channel in self.cf.values()
            if any(prop["name"] == key and prop["value"] == value for prop in channel["properties"])
        ]

    def _find_by_names(self, names):
        return [self.cf[name] for name in names if name in self.cf]

    def _find_active(self):
        return [
            channel
            for channel in self.cf.values()
            if any(
                prop["name"] == CFPropertyName.PV_STATUS and prop["value"] == PVStatus.ACTIVE
                for prop in channel["properties"]
            )
        ]

    def findByArgs(self, args):  # NOSONAR - mirrors pyCFClient API.
        if not self.connected:
            raise HTTPError(MOCK_CF_HTTP_ERROR, response=self)

        key, value = args[0]
        if key == CFPropertyName.IOC_ID:
            return self._find_by_iocid(key, value)
        if key == "~name":
            return self._find_by_names(str(value).split("|"))
        if key == CFPropertyName.PV_STATUS and value == PVStatus.ACTIVE:
            return self._find_active()
        return []

    def findProperty(self, prop_name):  # NOSONAR - mirrors pyCFClient API.
        if not self.connected:
            raise HTTPError(MOCK_CF_HTTP_ERROR, response=self)
        if prop_name in ("hostName", "iocName", "pvStatus", "time", "iocid"):
            return prop_name

    def set(self, channels):
        if not self.connected or self.fail_set:
            raise HTTPError(MOCK_CF_HTTP_ERROR, response=self)
        for channel in channels:
            self.addChannel(channel)

    def update(self, property, channelNames):  # NOSONAR - mirrors pyCFClient API.
        if not self.connected or self.fail_find:
            raise HTTPError(MOCK_CF_HTTP_ERROR, response=self)
        for channel in channelNames:
            self.__updateChannelWithProp(property, channel)

    def addChannel(self, channel):  # NOSONAR - mirrors pyCFClient API.
        self.cf[channel["name"]] = channel

    def __updateChannelWithProp(self, property, channel):  # NOSONAR - legacy helper kept for compatibility.
        if channel in self.cf:
            for prop in self.cf[channel]["properties"]:
                if prop["name"] == property["name"]:
                    prop["value"] = property["value"]
                    prop["owner"] = property["owner"]  # also overwriting owner because that's what CF does
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
