from twisted.internet.address import IPv4Address
from requests import HTTPError
class mock_client():
    def __init__(self):
        self.cf = {}
        self.connected = True
        self.fail_set = False

    def findByArgs(self, args):
        if not self.connected:
            raise HTTPError("Mock ChannelfinderClient HTTPError", response=self)
        else:
            result = []

            if len(args) > 1:  # returning old
                for ch in self.cf:
                    name_flag = False
                    prop_flag = False
                    for props in self.cf[ch][u'properties']:
                        if props[u'name'] == args[0][0]:
                            if props[u'value'] == args[0][1]:
                                name_flag = True
                        if props[u'name'] == args[1][0]:
                            if props[u'value'] == args[1][1]:
                                prop_flag = True
                    if name_flag and prop_flag:
                        result.append(self.cf[ch])
                return result
            else:
                if args[0][0] == '~name' and args[0][1] in self.cf:
                    return [self.cf[args[0][1]]]
                if args[0][0] == 'pvStatus' and args[0][1] == 'Active':
                    for ch in self.cf:
                        for prop in self.cf[ch]['properties']:
                            if prop['name'] == 'pvStatus':
                                if prop['value'] == 'Active':
                                    result.append(self.cf[ch])
                    return result

    def findProperty(self, prop_name):
        if not self.connected:
            raise HTTPError("Mock ChannelfinderClient HTTPError", response=self)
        else:
            # print "findProperty:  ", prop_name
            pass

    def set(self, channels):
        if not self.connected or self.fail_set:  # if not fail_set?
            raise HTTPError("Mock ChannelfinderClient HTTPError", response=self)
        else:
            #print "channels:\n", channels
            for channel in channels:
                self.addChannel(channel)
            #print "CF:\n", self.cf

    def addChannel(self, channel):
        self.cf[channel[u'name']] = channel


class mock_conf():
    def __init__(self):
        pass

    def get(self, name, target):
        return "cf-update"

class mock_TR():
    def __init__(self):
        #self.addrec = {5570560: ('test:lo', 'longout'), 5636096: ('test:Msg-I', 'stringin'), 5701632: ('test:li', 'longin'), 5767168: ('test:State-Sts', 'mbbi')}
        #self.addrec = {1: ('name', 'longout')}
        self.addrec = {}
        self.src = IPv4Address('TCP', 'testhosta', 1111)
        self.delrec = ()
        self.infos = {'CF_USERNAME': 'cf-update', 'ENGINEER': 'cf-engi'}
        self.initial = True
        self.connected = True