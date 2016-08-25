from twisted.trial.unittest import TestCase
from twisted.internet import defer
from mock_client import mock_conf
from mock_client import mock_TR
from mock_client import mock_client
from cfstore import CFProcessor

import threading
import time


class MyTestCase(TestCase):
    def setUp(self):
        conf = mock_conf()
        self.cfstore = CFProcessor("cf", conf)
        self.cfstore.currentTime = getTime
        self.maxDiff = None

    @defer.inlineCallbacks
    def test_3_iocs(self):
        self.cfclient = mock_client()
        self.cfclient.connected = True
        self.cfstore.client = self.cfclient
        self.cfstore.startService()
        TR1 = mock_TR()
        TR1.src.host = 'testhosta'
        TR1.src.port = 1111
        TR1.addrec = {1: ('ch1', 'longin'), 5: ('ch5', 'longin'), 6: ('ch6', 'stringin'), 7: ('ch7', 'longout')}
        deferred = yield self.cfstore.commit(TR1)
        self.assertCommit([u'ch1', u'ch5', u'ch6', u'ch7'],
                          {u'ch1': abbr(u'ch1', 'testhosta', 1111, 'Active'),
                           u'ch5': abbr(u'ch5', 'testhosta', 1111, 'Active'),
                           u'ch6': abbr(u'ch6', 'testhosta', 1111, 'Active'),
                           u'ch7': abbr(u'ch7', 'testhosta', 1111, 'Active')})

        TR2 = mock_TR()
        TR2.src.host = 'testhostb'
        TR2.src.port = 2222
        TR2.addrec = {1: ('ch1', 'longin'), 2: ('ch2', 'longout'), 3: ('ch3', 'stringout'), 7: ('ch7', 'longout')}
        deferred = yield self.cfstore.commit(TR2)
        self.assertCommit([u'ch1', u'ch2', u'ch3', u'ch5', u'ch6', u'ch7'],
                          {u'ch1': abbr(u'ch1', 'testhostb', 2222, 'Active'),
                           u'ch2': abbr(u'ch2', 'testhostb', 2222, 'Active'),
                           u'ch3': abbr(u'ch3', 'testhostb', 2222, 'Active'),
                           u'ch5': abbr(u'ch5', 'testhosta', 1111, 'Active'),
                           u'ch6': abbr(u'ch6', 'testhosta', 1111, 'Active'),
                           u'ch7': abbr(u'ch7', 'testhostb', 2222, 'Active')})

        TR3 = mock_TR()
        TR3.src.host = 'testhostc'
        TR3.src.port = 3333
        TR3.addrec = {1: ('ch1', 'longin'), 2: ('ch2', 'longout'), 4: ('ch4', 'stringin'), 6: ('ch6', 'stringin')}
        deferred = yield self.cfstore.commit(TR3)
        self.assertCommit([u'ch1', u'ch2', u'ch3', u'ch4', u'ch5', u'ch6', u'ch7'],
                          {u'ch1': abbr(u'ch1', 'testhostc', 3333, 'Active'),
                           u'ch2': abbr(u'ch2', 'testhostc', 3333, 'Active'),
                           u'ch3': abbr(u'ch3', 'testhostb', 2222, 'Active'),
                           u'ch4': abbr(u'ch4', 'testhostc', 3333, 'Active'),
                           u'ch5': abbr(u'ch5', 'testhosta', 1111, 'Active'),
                           u'ch6': abbr(u'ch6', 'testhostc', 3333, 'Active'),
                           u'ch7': abbr(u'ch7', 'testhostb', 2222, 'Active')})

        TR4 = mock_TR()
        TR4.initial = False
        TR4.addrec = {}
        TR4.connected = False
        TR4.src.host = 'testhostc'
        TR4.src.port = 3333
        deferred = yield self.cfstore.commit(TR4)
        self.assertCommit([u'ch1', u'ch2', u'ch3', u'ch4', u'ch5', u'ch6', u'ch7'],
                          {u'ch1': abbr(u'ch1', 'testhostb', 2222, 'Active'),
                           u'ch2': abbr(u'ch2', 'testhostb', 2222, 'Active'),
                           u'ch3': abbr(u'ch3', 'testhostb', 2222, 'Active'),
                           u'ch4': abbr(u'ch4', 'testhostc', 3333, 'Inactive'),
                           u'ch5': abbr(u'ch5', 'testhosta', 1111, 'Active'),
                           u'ch6': abbr(u'ch6', 'testhosta', 1111, 'Active'),
                           u'ch7': abbr(u'ch7', 'testhostb', 2222, 'Active')})

        TR5 = mock_TR()
        TR5.src.host = 'testhostc'
        TR5.src.port = 3333
        TR5.addrec = {1: ('ch1', 'longin'), 2: ('ch2', 'longout'), 4: ('ch4', 'stringin'), 6: ('ch6', 'stringin')}
        deferred = yield self.cfstore.commit(TR5)
        self.assertCommit([u'ch1', u'ch2', u'ch3', u'ch4', u'ch5', u'ch6', u'ch7'],
                          {u'ch1': abbr(u'ch1', 'testhostc', 3333, 'Active'),
                           u'ch2': abbr(u'ch2', 'testhostc', 3333, 'Active'),
                           u'ch3': abbr(u'ch3', 'testhostb', 2222, 'Active'),
                           u'ch4': abbr(u'ch4', 'testhostc', 3333, 'Active'),
                           u'ch5': abbr(u'ch5', 'testhosta', 1111, 'Active'),
                           u'ch6': abbr(u'ch6', 'testhostc', 3333, 'Active'),
                           u'ch7': abbr(u'ch7', 'testhostb', 2222, 'Active')})

        TR6 = mock_TR()
        TR6.initial = False
        TR6.addrec = {}
        TR6.connected = False
        TR6.src.host = 'testhostb'
        TR6.src.port = 2222
        deferred = yield self.cfstore.commit(TR6)
        self.assertCommit([u'ch1', u'ch2', u'ch3', u'ch4', u'ch5', u'ch6', u'ch7'],
                          {u'ch1': abbr(u'ch1', 'testhostc', 3333, 'Active'),
                           u'ch2': abbr(u'ch2', 'testhostc', 3333, 'Active'),
                           u'ch3': abbr(u'ch3', 'testhostb', 2222, 'Inactive'),
                           u'ch4': abbr(u'ch4', 'testhostc', 3333, 'Active'),
                           u'ch5': abbr(u'ch5', 'testhosta', 1111, 'Active'),
                           u'ch6': abbr(u'ch6', 'testhostc', 3333, 'Active'),
                           u'ch7': abbr(u'ch7', 'testhosta', 1111, 'Active')})

        TR7 = mock_TR()
        TR7.initial = False
        TR7.addrec = {}
        TR7.connected = False
        TR7.src.host = 'testhosta'
        TR7.src.port = 1111
        deferred = yield self.cfstore.commit(TR7)
        self.assertCommit([u'ch1', u'ch2', u'ch3', u'ch4', u'ch5', u'ch6', u'ch7'],
                          {u'ch1': abbr(u'ch1', 'testhostc', 3333, 'Active'),
                           u'ch2': abbr(u'ch2', 'testhostc', 3333, 'Active'),
                           u'ch3': abbr(u'ch3', 'testhostb', 2222, 'Inactive'),
                           u'ch4': abbr(u'ch4', 'testhostc', 3333, 'Active'),
                           u'ch5': abbr(u'ch5', 'testhosta', 1111, 'Inactive'),
                           u'ch6': abbr(u'ch6', 'testhostc', 3333, 'Active'),
                           u'ch7': abbr(u'ch7', 'testhosta', 1111, 'Inactive')})

        TR8 = mock_TR()
        TR8.initial = False
        TR8.addrec = {}
        TR8.connected = False
        TR8.src.host = 'testhostc'
        TR8.src.port = 3333
        deferred = yield self.cfstore.commit(TR8)
        self.assertCommit([u'ch1', u'ch2', u'ch3', u'ch4', u'ch5', u'ch6', u'ch7'],
                          {u'ch1': abbr(u'ch1', 'testhostc', 3333, 'Inactive'),
                           u'ch2': abbr(u'ch2', 'testhostc', 3333, 'Inactive'),
                           u'ch3': abbr(u'ch3', 'testhostb', 2222, 'Inactive'),
                           u'ch4': abbr(u'ch4', 'testhostc', 3333, 'Inactive'),
                           u'ch5': abbr(u'ch5', 'testhosta', 1111, 'Inactive'),
                           u'ch6': abbr(u'ch6', 'testhostc', 3333, 'Inactive'),
                           u'ch7': abbr(u'ch7', 'testhosta', 1111, 'Inactive')})

        TR9 = mock_TR()
        TR9.src.host = 'testhostb'
        TR9.src.port = 2222
        TR9.addrec = {1: ('ch1', 'longin'), 2: ('ch2', 'longout'), 3: ('ch3', 'stringout'), 7: ('ch7', 'longout')}
        deferred = yield self.cfstore.commit(TR9)
        self.assertCommit([u'ch1', u'ch2', u'ch3', u'ch4', u'ch5', u'ch6', u'ch7'],
                          {u'ch1': abbr(u'ch1', 'testhostb', 2222, 'Active'),
                           u'ch2': abbr(u'ch2', 'testhostb', 2222, 'Active'),
                           u'ch3': abbr(u'ch3', 'testhostb', 2222, 'Active'),
                           u'ch4': abbr(u'ch4', 'testhostc', 3333, 'Inactive'),
                           u'ch5': abbr(u'ch5', 'testhosta', 1111, 'Inactive'),
                           u'ch6': abbr(u'ch6', 'testhostc', 3333, 'Inactive'),
                           u'ch7': abbr(u'ch7', 'testhostb', 2222, 'Active')})

        TR10 = mock_TR()
        TR10.src.host = 'testhosta'
        TR10.src.port = 1111
        TR10.addrec = {1: ('ch1', 'longin'), 2: ('ch2', 'longout'), 3: ('ch3', 'stringout'), 4: ('ch4', 'stringin'), 5: ('ch5', 'longin'), 6: ('ch6', 'stringin'), 7: ('ch7', 'longout')}
        deferred = yield self.cfstore.commit(TR10)
        self.assertCommit([u'ch1', u'ch2', u'ch3', u'ch4', u'ch5', u'ch6', u'ch7'],
                          {u'ch1': abbr(u'ch1', 'testhosta', 1111, 'Active'),
                           u'ch2': abbr(u'ch2', 'testhosta', 1111, 'Active'),
                           u'ch3': abbr(u'ch3', 'testhosta', 1111, 'Active'),
                           u'ch4': abbr(u'ch4', 'testhosta', 1111, 'Active'),
                           u'ch5': abbr(u'ch5', 'testhosta', 1111, 'Active'),
                           u'ch6': abbr(u'ch6', 'testhosta', 1111, 'Active'),
                           u'ch7': abbr(u'ch7', 'testhosta', 1111, 'Active')})

    @defer.inlineCallbacks
    def test_no_CFS(self):
        self.cfclient = mock_client()
        self.cfclient.connected = False
        self.cfstore.client = self.cfclient
        self.cfstore.startService()
        TR1 = mock_TR()
        TR1.src.host = 'testhosta'
        TR1.src.port = 1111
        TR1.addrec = {1: ('ch1', 'longin'), 5: ('ch5', 'longin'), 6: ('ch6', 'stringin'), 7: ('ch7', 'longout')}
        threading._start_new_thread(self.simulate_reconnect, ())
        deferred = yield self.cfstore.commit(TR1)
        self.assertCommit([u'ch1', u'ch5', u'ch6', u'ch7'],
                          {u'ch1': abbr(u'ch1', 'testhosta', 1111, 'Active'),
                           u'ch5': abbr(u'ch5', 'testhosta', 1111, 'Active'),
                           u'ch6': abbr(u'ch6', 'testhosta', 1111, 'Active'),
                           u'ch7': abbr(u'ch7', 'testhosta', 1111, 'Active')})

    @defer.inlineCallbacks
    def test_set_fail(self):
        self.cfclient = mock_client()
        self.cfclient.connected = True
        self.cfclient.fail_set = True
        self.cfstore.client = self.cfclient
        self.cfstore.startService()
        TR1 = mock_TR()
        TR1.src.host = 'testhosta'
        TR1.src.port = 1111
        TR1.addrec = {1: ('ch1', 'longin'), 5: ('ch5', 'longin'), 6: ('ch6', 'stringin'), 7: ('ch7', 'longout')}
        threading._start_new_thread(self.simulate_reconnect, ())
        deferred = yield self.cfstore.commit(TR1)
        self.assertCommit([u'ch1', u'ch5', u'ch6', u'ch7'],
                          {u'ch1': abbr(u'ch1', 'testhosta', 1111, 'Active'),
                           u'ch5': abbr(u'ch5', 'testhosta', 1111, 'Active'),
                           u'ch6': abbr(u'ch6', 'testhosta', 1111, 'Active'),
                           u'ch7': abbr(u'ch7', 'testhosta', 1111, 'Active')})


    def assertCommit(self, keys, dict):
        self.assertSequenceEqual(self.cfstore.client.cf.keys(), keys)
        self.assertDictEqual(self.cfstore.client.cf, dict)

    def simulate_reconnect(self):
        time.sleep(3)
        self.cfclient.connected = True
        self.cfclient.fail_set = False

def abbr(name, hostname, iocname, status):
    return {u'owner': 'cf-update',
            u'name': name,
            u'properties': [
                {u'owner': 'cf-update', u'name': 'hostName',
                 u'value': hostname},
                {u'owner': 'cf-update', u'name': 'iocName',
                 u'value': iocname},
                {u'owner': 'cf-update', u'name': 'pvStatus',
                 u'value': status},
                {u'owner': 'cf-update', u'name': 'time',
                 u'value': '2016-08-18 12:33:09.953985'}]}


def getTime():
    return '2016-08-18 12:33:09.953985'

# if __name__ == '__main__':
#     unittest.main()
