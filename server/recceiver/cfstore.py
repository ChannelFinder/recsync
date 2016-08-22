# -*- coding: utf-8 -*-

import logging
_log = logging.getLogger(__name__)

from zope.interface import implements
from twisted.application import service
from twisted.internet.threads import deferToThread
import time
import interfaces
import datetime

# ITRANSACTION FORMAT:
#
# src = source address
# addrec = records ein added ( recname, rectype, {key:val})
# delrec = a set() of records which are being removed
# infos = dictionary of client infos
# recinfos = additional infos being added to existing records 
# "recid: {key:value}"
#


__all__ = ['CFProcessor']

class CFProcessor(service.Service):
    implements(interfaces.IProcessor)

    def __init__(self, name, conf):
        _log.info("CF_INIT %s", name)
        self.name, self.conf = name, conf
        self.channel_dict = dict()
        self.iocs = dict()
        self.client = None
        self.currentTime = getCurrentTime

    def startService(self):
        service.Service.startService(self)
        self.running = 1
        _log.info("CF_START")
        from channelfinder import ChannelFinderClient
        # Using the default python cf-client.
        # The usr, username, and password are provided by the channelfinder._conf module.
        if self.client is None:  # For setting up mock test client
            self.client = ChannelFinderClient()

    def stopService(self):
        service.Service.stopService(self)
        #Set channels to inactive and close connection to client
        self.running = 0
        _log.info("CF_STOP")

    def commit(self, TR):
        if _log.isEnabledFor(logging.DEBUG):
            _log.debug("CF_COMMIT %s", TR.infos.items())
        pvNames = [unicode(rname, "utf-8") for rid, (rname, rtype) in TR.addrec.iteritems()]
        delrec = TR.delrec
        iocName = TR.src.port
        hostName = TR.src.host
        iocid = hostName + str(iocName)
        owner = TR.infos.get('CF_USERNAME') or TR.infos.get('ENGINEER') or self.conf.get('username', 'cfstore')
        time = self.currentTime()
        if TR.initial:
            self.iocs[iocid] = {"iocname": iocName, "hostname": hostName, "channelcount": 0}  # add IOC to source list
        if TR.connected:
            for pv in pvNames:
                if pv in self.channel_dict:
                    if iocid not in self.channel_dict[pv]:
                        self.channel_dict[pv].append(iocid)  # add iocname to pvName in dict
                        self.iocs[iocid]["channelcount"] += 1
                    # else:  # info already in dictionary, possibly recovering from ioc disconnect
                else:
                    self.channel_dict[pv] = [iocid]  # add pvName with [iocname] in dict
                    self.iocs[iocid]["channelcount"] += 1
            for pv in delrec:
                if iocid in self.channel_dict[pv]:
                    self.channel_dict[pv].remove(iocid)
                    self.iocs[iocid]["channelcount"] -= 1
                    if len(self.channel_dict[pv]) <= 0:  # case: channel has no more iocs
                        del self.channel_dict[pv]
                #pvNames.remove(pv)
        else:  # CASE: IOC Disconnected
            keys = self.channel_dict.keys()
            for ch in keys:
                if iocid in self.channel_dict[ch]:
                    self.channel_dict[ch].remove(iocid)
                    if len(self.channel_dict[ch]) <= 0:  # case: channel has no more iocs
                        del self.channel_dict[ch]
                    self.iocs[iocid]['channelcount'] -= 1
                # else:
                #     pass  # ch not connected to ioc?

        if iocName and hostName and owner:
            #print "channels_dict:\n", self.channel_dict
            return deferToThread(poll, __updateCF__, self.client, pvNames, delrec, self.channel_dict, self.iocs, hostName, iocName, time, owner)
        else:
            _log.error('failed to initialize one or more of the following properties' +
                       'hostname: %s iocname: %s owner: %s', hostName, iocName, owner)

def __updateCF__(client, new, delrec, channels_dict, iocs, hostName, iocName, time, owner):
    if hostName is None or iocName is None:
        raise Exception('missing hostName or iocName')
    channels = []
    checkPropertiesExist(client, owner)
    old = client.findByArgs([('hostName', hostName), ('iocName', iocName)])
    if old is not None:
        for ch in old:
            if new == [] or ch[u'name'] in delrec:  # case: empty commit/del, remove all reference to ioc
                if ch[u'name'] in channels_dict:
                    channels.append(updateChannel(ch,
                                                  owner=owner,
                                                  hostName=iocs[channels_dict[ch[u'name']][-1]]["hostname"],
                                                  iocName=iocs[channels_dict[ch[u'name']][-1]]["iocname"],
                                                  pvStatus='Active',
                                                  time=time))
                else:
                    '''Orphan the channel : mark as inactive, keep the old hostName and iocName'''
                    oldHostName = hostName
                    oldIocName = iocName
                    oldTime = time
                    for prop in ch[u'properties']:
                        if prop[u'name'] == u'hostName':
                            oldHostName = prop[u'value']
                        if prop[u'name'] == u'iocName':
                            oldIocName = prop[u'value']
                        if prop[u'name'] == u'time':
                            oldTime = prop[u'value']
                    channels.append(updateChannel(ch,
                                                  owner=owner,
                                                  hostName=oldHostName,
                                                  iocName=oldIocName,
                                                  pvStatus='Inactive',
                                                  time=oldTime))
            else:
                if ch in new:  # case: channel in old and new
                    channels.append(updateChannel(ch,
                                                  owner=owner,
                                                  hostName=iocs[channels_dict[ch[u'name']][-1]]["hostname"],
                                                  iocName=iocs[channels_dict[ch[u'name']][-1]]["iocname"],
                                                  pvStatus='Active',
                                                  time=time))
                    new.remove(ch[u'name'])

    # now pvNames contains a list of pv's new on this host/ioc
    for pv in new:
        ch = client.findByArgs([('~name', pv)])
        if not ch:
            '''New channel'''
            channels.append(createChannel(pv,
                                          chOwner=owner,
                                          hostName=hostName,
                                          iocName=iocName,
                                          pvStatus='Active',
                                          time=time))
        else:
            '''update existing channel: exists but with a different hostName and/or iocName'''
            channels.append(updateChannel(ch[0],
                                          owner=owner,
                                          hostName=hostName,
                                          iocName=iocName,
                                          pvStatus='Active',
                                          time=time))
    client.set(channels=channels)

def updateChannel(channel, owner, hostName=None, iocName=None, pvStatus='InActive', time=None):
    '''
    Helper to update a channel object so as to not affect the existing properties
    '''
    # properties list devoid of hostName and iocName properties
    if channel[u'properties']:
        channel[u'properties'] = [property for property in channel[u'properties']
                         if property[u'name'] != 'hostName'
                         and property[u'name'] != 'iocName'
                         and property[u'name'] != 'pvStatus'
                         and property[u'name'] != 'time']
    else:
        channel[u'properties'] = []
    if hostName is not None:
        channel[u'properties'].append({u'name': 'hostName', u'owner': owner, u'value': hostName})
    if iocName is not None:
        channel[u'properties'].append({u'name': 'iocName', u'owner': owner, u'value': iocName})
    if pvStatus:
        channel[u'properties'].append({u'name': 'pvStatus', u'owner': owner, u'value': pvStatus})
    if time:
        channel[u'properties'].append({u'name': 'time', u'owner': owner, u'value': time})
    return channel

def createChannel(chName, chOwner, hostName=None, iocName=None, pvStatus='InActive', time=None):
    '''
    Helper to create a channel object with the required properties
    '''
    ch = {u'name': chName, u'owner': chOwner, u'properties': []}
    if hostName is not None:
        ch[u'properties'].append({u'name': 'hostName', u'owner': chOwner, u'value': hostName})
    if iocName is not None:
        ch[u'properties'].append({u'name': 'iocName', u'owner': chOwner, u'value': iocName})
    if pvStatus:
        ch[u'properties'].append({u'name': 'pvStatus', u'owner': chOwner, u'value': pvStatus})
    if time:
        ch[u'properties'].append({u'name': 'time', u'owner': chOwner, u'value': time})
    return ch

def checkPropertiesExist(client, propOwner):
    '''
    Checks if the properties used by dbUpdate are present if not it creates them
    '''
    requiredProperties = ['hostName', 'iocName', 'pvStatus', 'time']
    for propName in requiredProperties:
        if client.findProperty(propName) is None:
            try:
                client.set(property={u'name': propName, u'owner': propOwner})
            except Exception as e:
                _log.error('Failed to create the property %s: %s', propName, e)

def getCurrentTime():
    return str(datetime.datetime.now())

def poll(update, client, new, delrec, channels_dict, iocs, hostName, iocName, times, owner):
    sleep = 1
    while 1:
        try:
            update(client, new, delrec, channels_dict, iocs, hostName, iocName, times, owner)
            print "-------------------\nTRUE\n-------------------"
            return True
        except Exception as e:  # should catch only network errors
            print "SLEEP: ", sleep, "\n-------------------------\n",
            print "", channels_dict, "\n-------------------------\n"
            time.sleep(sleep)
            if sleep >= 60:
                sleep = 60
            else:
                sleep *= 1.5

