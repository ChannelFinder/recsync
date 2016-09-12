from channelfinder import ChannelFinderClient

'''
Simple script for adding active channels to Channel Finder Service for testing cf-store clean
If it gives a 500 error, run it again. Glassfish and CFS must be set up and running.
'''


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

client = ChannelFinderClient()
client.set(channels=[abbr(u'ch1', 'testhosta', 1111, 'Active'), abbr(u'test_channel', 'testhosta', 1111, 'Active')])
