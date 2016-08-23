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

        TRFinal = mock_TR()
        TRFinal.initial = False
        TRFinal.addrec = {}
        TRFinal.connected = False
        TRFinal.src.host = 'testhostc'
        TRFinal.src.port = 3333
        deferred = yield self.cfstore.commit(TRFinal)
        self.assertCommit([u'ch1', u'ch2', u'ch3', u'ch4', u'ch5', u'ch6', u'ch7'],
                          {u'ch1': abbr(u'ch1', 'testhostb', 2222, 'Active'),
                           u'ch2': abbr(u'ch2', 'testhostb', 2222, 'Active'),
                           u'ch3': abbr(u'ch3', 'testhostb', 2222, 'Active'),
                           u'ch4': abbr(u'ch4', 'testhostc', 3333, 'Inactive'),
                           u'ch5': abbr(u'ch5', 'testhosta', 1111, 'Active'),
                           u'ch6': abbr(u'ch6', 'testhosta', 1111, 'Active'),
                           u'ch7': abbr(u'ch7', 'testhostb', 2222, 'Active')})

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
