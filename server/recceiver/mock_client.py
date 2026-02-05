from requests import HTTPError
from twisted.internet.address import IPv4Address


class mock_client:
    def __init__(self):
        self.cf = {}
        self.connected = True
        self.fail_find = False
        self.fail_set = False

    def findByArgs(self, args):
        if not self.connected:
            raise HTTPError("Mock Channelfinder Client HTTPError", response=self)
        result = []

        if args[0][0] == "iocid":  # returning old
            for ch in self.cf:
                name_flag = False
                for props in self.cf[ch]["properties"]:
                    if props["name"] == args[0][0]:
                        if props["value"] == args[0][1]:
                            name_flag = True
                if name_flag:
                    result.append(self.cf[ch])
            return result
        if args[0][0] == "~name":
            names = str(args[0][1]).split("|")
            return [self.cf[name] for name in names if name in self.cf]
        if args[0][0] == "pvStatus" and args[0][1] == "Active":
            for ch in self.cf:
                for prop in self.cf[ch]["properties"]:
                    if prop["name"] == "pvStatus":
                        if prop["value"] == "Active":
                            result.append(self.cf[ch])
            return result

    def findProperty(self, prop_name):
        if not self.connected:
            raise HTTPError("Mock Channelfinder Client HTTPError", response=self)
        if prop_name in ["hostName", "iocName", "pvStatus", "time", "iocid"]:
            return prop_name

    def set(self, channels):
        if not self.connected or self.fail_set:
            raise HTTPError("Mock Channelfinder Client HTTPError", response=self)
        for channel in channels:
            self.addChannel(channel)

    def update(self, property, channelNames):
        if not self.connected or self.fail_find:
            raise HTTPError("Mock Channelfinder Client HTTPError", response=self)
        for channel in channelNames:
            self.__updateChannelWithProp(property, channel)

    def addChannel(self, channel):
        self.cf[channel["name"]] = channel

    def __updateChannelWithProp(self, property, channel):
        if channel in self.cf:
            for prop in self.cf[channel]["properties"]:
                if prop["name"] == property["name"]:
                    prop["value"] = property["value"]
                    prop["owner"] = property["owner"]  # also overwriting owner because that's what CF does
                    return


class mock_conf:
    def __init__(self):
        pass

    def get(self, name, target):
        return "cf-engi"


class mock_TR:
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
