class mock_client():
    def __init__(self):
        self.cf = {}

    def findByArgs(self, args):
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
            if args[0][1] in self.cf:
                return [self.cf[args[0][1]]]

    def findProperty(self, prop_name):
        # print "findProperty:  ", prop_name
        pass

    def set(self, channels):
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

class mock_src():
    def __init__(self):
        self.host = '10.0.2.15'
        self.port = 43891

class mock_TR():
    def __init__(self):
        #self.addrec = {5570560: ('test:lo', 'longout'), 5636096: ('test:Msg-I', 'stringin'), 5701632: ('test:li', 'longin'), 5767168: ('test:State-Sts', 'mbbi')}
        #self.addrec = {1: ('name', 'longout')}
        self.addrec = {}
        self.src = mock_src()
        self.delrec = ()
        self.infos = {'CF_USERNAME': 'cf-update', 'ENGINEER': 'cf-engi'}
        self.initial = True
        self.connected = True