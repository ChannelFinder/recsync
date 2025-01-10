from twisted.trial.unittest import TestCase
from twisted.internet import defer
from mock_client import mock_conf
from mock_client import mock_TR
from mock_client import mock_client
from cfstore import CFProcessor

import threading


class MyTestCase(TestCase):
    def setUp(self):
        conf = mock_conf()
        self.cfstore = CFProcessor("cf", conf)
        self.cfstore.currentTime = getTime
        self.maxDiff = None

    @defer.inlineCallbacks
    def test_3_iocs(self):
        self.cfclient = mock_client()
        self.cfstore.client = self.cfclient
        self.cfstore.startService()
        TR1 = mock_TR()
        TR1.src.host = 'testhosta'
        TR1.src.port = 1111
        TR1.addrec = {1: ('ch1', 'longin'), 5: ('ch5', 'longin'), 6: ('ch6', 'stringin'), 7: ('ch7', 'longout')}
        deferred = yield self.cfstore.commit(TR1)
        self.assertDictEqual(self.cfstore.client.cf,
                             {u'ch1': abbr('cf-engi', u'ch1', 'testhosta', 1111, 'Active'),
                              u'ch5': abbr('cf-engi', u'ch5', 'testhosta', 1111, 'Active'),
                              u'ch6': abbr('cf-engi', u'ch6', 'testhosta', 1111, 'Active'),
                              u'ch7': abbr('cf-engi', u'ch7', 'testhosta', 1111, 'Active')})

        TR2 = mock_TR()
        TR2.src.host = 'testhostb'
        TR2.src.port = 2222
        TR2.addrec = {1: ('ch1', 'longin'), 2: ('ch2', 'longout'), 3: ('ch3', 'stringout'), 7: ('ch7', 'longout')}
        deferred = yield self.cfstore.commit(TR2)
        self.assertDictEqual(self.cfstore.client.cf,
                             {u'ch1': abbr('cf-engi', u'ch1', 'testhostb', 2222, 'Active'),
                              u'ch2': abbr('cf-engi', u'ch2', 'testhostb', 2222, 'Active'),
                              u'ch3': abbr('cf-engi', u'ch3', 'testhostb', 2222, 'Active'),
                              u'ch5': abbr('cf-engi', u'ch5', 'testhosta', 1111, 'Active'),
                              u'ch6': abbr('cf-engi', u'ch6', 'testhosta', 1111, 'Active'),
                              u'ch7': abbr('cf-engi', u'ch7', 'testhostb', 2222, 'Active')})

        TR3 = mock_TR()
        TR3.src.host = 'testhostc'
        TR3.src.port = 3333
        TR3.addrec = {1: ('ch1', 'longin'), 2: ('ch2', 'longout'), 4: ('ch4', 'stringin'), 6: ('ch6', 'stringin')}
        deferred = yield self.cfstore.commit(TR3)
        self.assertDictEqual(self.cfstore.client.cf,
                             {u'ch1': abbr('cf-engi', u'ch1', 'testhostc', 3333, 'Active'),
                              u'ch2': abbr('cf-engi', u'ch2', 'testhostc', 3333, 'Active'),
                              u'ch3': abbr('cf-engi', u'ch3', 'testhostb', 2222, 'Active'),
                              u'ch4': abbr('cf-engi', u'ch4', 'testhostc', 3333, 'Active'),
                              u'ch5': abbr('cf-engi', u'ch5', 'testhosta', 1111, 'Active'),
                              u'ch6': abbr('cf-engi', u'ch6', 'testhostc', 3333, 'Active'),
                              u'ch7': abbr('cf-engi', u'ch7', 'testhostb', 2222, 'Active')})

        TR4 = mock_TR()
        TR4.initial = False
        TR4.addrec = {}
        TR4.connected = False  # simulated IOC Disconnect
        TR4.src.host = 'testhostc'
        TR4.src.port = 3333
        deferred = yield self.cfstore.commit(TR4)
        assertChannelsDictEqual(self, self.cfstore.client.cf,
                             {u'ch1': abbr('cf-engi', u'ch1', 'testhostb', 2222, 'Active'),
                              u'ch2': abbr('cf-engi', u'ch2', 'testhostb', 2222, 'Active'),
                              u'ch3': abbr('cf-engi', u'ch3', 'testhostb', 2222, 'Active'),
                              u'ch4': abbr('cf-engi', u'ch4', 'testhostc', 3333, 'Inactive'),
                              u'ch5': abbr('cf-engi', u'ch5', 'testhosta', 1111, 'Active'),
                              u'ch6': abbr('cf-engi', u'ch6', 'testhosta', 1111, 'Active'),
                              u'ch7': abbr('cf-engi', u'ch7', 'testhostb', 2222, 'Active')})

        TR5 = mock_TR()
        TR5.src.host = 'testhostc'
        TR5.src.port = 3333
        TR5.addrec = {1: ('ch1', 'longin'), 2: ('ch2', 'longout'), 4: ('ch4', 'stringin'), 6: ('ch6', 'stringin')}
        deferred = yield self.cfstore.commit(TR5)
        assertChannelsDictEqual(self, self.cfstore.client.cf,
                             {u'ch1': abbr('cf-engi', u'ch1', 'testhostc', 3333, 'Active'),
                              u'ch2': abbr('cf-engi', u'ch2', 'testhostc', 3333, 'Active'),
                              u'ch3': abbr('cf-engi', u'ch3', 'testhostb', 2222, 'Active'),
                              u'ch4': abbr('cf-engi', u'ch4', 'testhostc', 3333, 'Active'),
                              u'ch5': abbr('cf-engi', u'ch5', 'testhosta', 1111, 'Active'),
                              u'ch6': abbr('cf-engi', u'ch6', 'testhostc', 3333, 'Active'),
                              u'ch7': abbr('cf-engi', u'ch7', 'testhostb', 2222, 'Active')})

        TR6 = mock_TR()
        TR6.initial = False
        TR6.addrec = {}
        TR6.connected = False  # simulated IOC Disconnect
        TR6.src.host = 'testhostb'
        TR6.src.port = 2222
        deferred = yield self.cfstore.commit(TR6)
        assertChannelsDictEqual(self, self.cfstore.client.cf,
                             {u'ch1': abbr('cf-engi', u'ch1', 'testhostc', 3333, 'Active'),
                              u'ch2': abbr('cf-engi', u'ch2', 'testhostc', 3333, 'Active'),
                              u'ch3': abbr('cf-engi', u'ch3', 'testhostb', 2222, 'Inactive'),
                              u'ch4': abbr('cf-engi', u'ch4', 'testhostc', 3333, 'Active'),
                              u'ch5': abbr('cf-engi', u'ch5', 'testhosta', 1111, 'Active'),
                              u'ch6': abbr('cf-engi', u'ch6', 'testhostc', 3333, 'Active'),
                              u'ch7': abbr('cf-engi', u'ch7', 'testhosta', 1111, 'Active')})

        TR7 = mock_TR()
        TR7.initial = False
        TR7.addrec = {}
        TR7.connected = False  # simulated IOC Disconnect
        TR7.src.host = 'testhosta'
        TR7.src.port = 1111
        deferred = yield self.cfstore.commit(TR7)
        assertChannelsDictEqual(self, self.cfstore.client.cf,
                             {u'ch1': abbr('cf-engi', u'ch1', 'testhostc', 3333, 'Active'),
                              u'ch2': abbr('cf-engi', u'ch2', 'testhostc', 3333, 'Active'),
                              u'ch3': abbr('cf-engi', u'ch3', 'testhostb', 2222, 'Inactive'),
                              u'ch4': abbr('cf-engi', u'ch4', 'testhostc', 3333, 'Active'),
                              u'ch5': abbr('cf-engi', u'ch5', 'testhosta', 1111, 'Inactive'),
                              u'ch6': abbr('cf-engi', u'ch6', 'testhostc', 3333, 'Active'),
                              u'ch7': abbr('cf-engi', u'ch7', 'testhosta', 1111, 'Inactive')})

        TR8 = mock_TR()
        TR8.initial = False
        TR8.addrec = {}
        TR8.connected = False  # simulated IOC Disconnect
        TR8.src.host = 'testhostc'
        TR8.src.port = 3333
        deferred = yield self.cfstore.commit(TR8)
        assertChannelsDictEqual(self, self.cfstore.client.cf,
                             {u'ch1': abbr('cf-engi', u'ch1', 'testhostc', 3333, 'Inactive'),
                              u'ch2': abbr('cf-engi', u'ch2', 'testhostc', 3333, 'Inactive'),
                              u'ch3': abbr('cf-engi', u'ch3', 'testhostb', 2222, 'Inactive'),
                              u'ch4': abbr('cf-engi', u'ch4', 'testhostc', 3333, 'Inactive'),
                              u'ch5': abbr('cf-engi', u'ch5', 'testhosta', 1111, 'Inactive'),
                              u'ch6': abbr('cf-engi', u'ch6', 'testhostc', 3333, 'Inactive'),
                              u'ch7': abbr('cf-engi', u'ch7', 'testhosta', 1111, 'Inactive')})

        TR9 = mock_TR()
        TR9.src.host = 'testhostb'
        TR9.src.port = 2222
        TR9.addrec = {1: ('ch1', 'longin'), 2: ('ch2', 'longout'), 3: ('ch3', 'stringout'), 7: ('ch7', 'longout')}
        deferred = yield self.cfstore.commit(TR9)
        self.assertDictEqual(self.cfstore.client.cf,
                             {u'ch1': abbr('cf-engi', u'ch1', 'testhostb', 2222, 'Active'),
                              u'ch2': abbr('cf-engi', u'ch2', 'testhostb', 2222, 'Active'),
                              u'ch3': abbr('cf-engi', u'ch3', 'testhostb', 2222, 'Active'),
                              u'ch4': abbr('cf-engi', u'ch4', 'testhostc', 3333, 'Inactive'),
                              u'ch5': abbr('cf-engi', u'ch5', 'testhosta', 1111, 'Inactive'),
                              u'ch6': abbr('cf-engi', u'ch6', 'testhostc', 3333, 'Inactive'),
                              u'ch7': abbr('cf-engi', u'ch7', 'testhostb', 2222, 'Active')})

        TR10 = mock_TR()
        TR10.src.host = 'testhosta'
        TR10.src.port = 1111
        TR10.addrec = {1: ('ch1', 'longin'), 2: ('ch2', 'longout'), 3: ('ch3', 'stringout'), 4: ('ch4', 'stringin'), 5: ('ch5', 'longin'), 6: ('ch6', 'stringin'), 7: ('ch7', 'longout')}
        deferred = yield self.cfstore.commit(TR10)
        self.assertDictEqual(self.cfstore.client.cf,
                             {u'ch1': abbr('cf-engi', u'ch1', 'testhosta', 1111, 'Active'),
                              u'ch2': abbr('cf-engi', u'ch2', 'testhosta', 1111, 'Active'),
                              u'ch3': abbr('cf-engi', u'ch3', 'testhosta', 1111, 'Active'),
                              u'ch4': abbr('cf-engi', u'ch4', 'testhosta', 1111, 'Active'),
                              u'ch5': abbr('cf-engi', u'ch5', 'testhosta', 1111, 'Active'),
                              u'ch6': abbr('cf-engi', u'ch6', 'testhosta', 1111, 'Active'),
                              u'ch7': abbr('cf-engi', u'ch7', 'testhosta', 1111, 'Active')})

    @defer.inlineCallbacks
    def test_no_CFS(self):
        self.cfclient = mock_client()
        self.cfclient.connected = False
        self.cfstore.client = self.cfclient
        rcon_thread = threading.Timer(2, self.simulate_reconnect)
        rcon_thread.start()
        self.cfstore.startService()
        TR1 = mock_TR()
        TR1.src.host = 'testhosta'
        TR1.src.port = 1111
        TR1.addrec = {1: ('ch1', 'longin'), 5: ('ch5', 'longin'), 6: ('ch6', 'stringin'), 7: ('ch7', 'longout')}
        deferred = yield self.cfstore.commit(TR1)
        self.assertDictEqual(self.cfstore.client.cf,
                             {u'ch1': abbr('cf-engi', u'ch1', 'testhosta', 1111, 'Active'),
                              u'ch5': abbr('cf-engi', u'ch5', 'testhosta', 1111, 'Active'),
                              u'ch6': abbr('cf-engi', u'ch6', 'testhosta', 1111, 'Active'),
                              u'ch7': abbr('cf-engi', u'ch7', 'testhosta', 1111, 'Active')})

    @defer.inlineCallbacks
    def test_set_fail(self):
        self.cfclient = mock_client()
        self.cfclient.fail_set = True
        self.cfstore.client = self.cfclient
        self.cfstore.startService()
        TR1 = mock_TR()
        TR1.src.host = 'testhosta'
        TR1.src.port = 1111
        TR1.addrec = {1: ('ch1', 'longin'), 5: ('ch5', 'longin'), 6: ('ch6', 'stringin'), 7: ('ch7', 'longout')}
        rcon_thread = threading.Timer(2, self.simulate_reconnect)
        rcon_thread.start()
        deferred = yield self.cfstore.commit(TR1)
        self.assertDictEqual(self.cfstore.client.cf,
                             {u'ch1': abbr('cf-engi', u'ch1', 'testhosta', 1111, 'Active'),
                              u'ch5': abbr('cf-engi', u'ch5', 'testhosta', 1111, 'Active'),
                              u'ch6': abbr('cf-engi', u'ch6', 'testhosta', 1111, 'Active'),
                              u'ch7': abbr('cf-engi', u'ch7', 'testhosta', 1111, 'Active')})

    @defer.inlineCallbacks
    def test_clean_service(self):
        self.cfclient = mock_client()
        self.cfclient.fail_find = True
        self.cfclient.cf = {u'ch1': abbr('cf-engi', u'ch1', 'testhostb', 2222, 'Active'),
                            u'ch5': abbr('cf-engi', u'ch5', 'testhostb', 2222, 'Active'),
                            u'ch6': abbr('cf-engi', u'ch6', 'testhostb', 2222, 'Active'),
                            u'ch7': abbr('cf-engi', u'ch7', 'testhostb', 2222, 'Active')}
        self.cfstore.client = self.cfclient
        rcon_thread = threading.Timer(2, self.simulate_reconnect)
        rcon_thread.start()
        self.cfstore.startService()
        self.assertDictEqual(self.cfstore.client.cf,
                             {u'ch1': abbr('cf-engi', u'ch1', 'testhostb', 2222, 'Inactive'),
                              u'ch5': abbr('cf-engi', u'ch5', 'testhostb', 2222, 'Inactive'),
                              u'ch6': abbr('cf-engi', u'ch6', 'testhostb', 2222, 'Inactive'),
                              u'ch7': abbr('cf-engi', u'ch7', 'testhostb', 2222, 'Inactive')})
        TR1 = mock_TR()
        TR1.src.host = 'testhosta'
        TR1.src.port = 1111
        TR1.addrec = {1: ('ch1', 'longin'), 5: ('ch5', 'longin'), 6: ('ch6', 'stringin'), 7: ('ch7', 'longout')}
        deferred = yield self.cfstore.commit(TR1)
        self.assertDictEqual(self.cfstore.client.cf,
                             {u'ch1': abbr('cf-engi', u'ch1', 'testhosta', 1111, 'Active'),
                              u'ch5': abbr('cf-engi', u'ch5', 'testhosta', 1111, 'Active'),
                              u'ch6': abbr('cf-engi', u'ch6', 'testhosta', 1111, 'Active'),
                              u'ch7': abbr('cf-engi', u'ch7', 'testhosta', 1111, 'Active')})

    def assertCommit(self, dict):
        self.assertDictEqual(self.cfstore.client.cf, dict)

    def simulate_reconnect(self):
        self.cfclient.connected = True  # There is the potential for a race condition here
        self.cfclient.fail_set = False  # This would cause a connection failure and re-polling at a different point
        self.cfclient.fail_find = False

def assertChannelsDictEqual(self, chs1, chs2, msg=None):
    """
    check equality ignoring the order of the properties
    """
    for ch in chs1:
        chs1[ch][u'properties'].sort()
    for ch in chs2:
        chs2[ch][u'properties'].sort()
    self.assertDictEqual(chs1, chs2, msg=msg)

def abbr(owner, name, hostname, iocname, status):
    return {u'owner': owner,
            u'name': name,
            u'properties': [
                {u'owner': 'cf-engi', u'name': 'hostName', u'value': hostname},
                {u'owner': 'cf-engi', u'name': 'iocName', u'value': iocname},
                {u'owner': 'cf-engi', u'name': 'iocid', u'value': hostname+":"+str(iocname)},
                {u'owner': 'cf-engi', u'name': 'pvStatus', u'value': status},
                {u'owner': 'cf-engi', u'name': 'time', u'value': '2016-08-18 12:33:09.953985'}]}


def getTime():
    return '2016-08-18 12:33:09.953985'

# if __name__ == '__main__':
#     unittest.main()
