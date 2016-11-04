# -*- coding: utf-8 -*-

import logging
_log = logging.getLogger(__name__)
from requests import RequestException
from zope.interface import implements
from twisted.application import service
from twisted.internet.threads import deferToThread
from twisted.internet.defer import DeferredLock
from twisted.internet import defer
from operator import itemgetter
from collections import defaultdict
import time
import interfaces
import datetime
import os
import json

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
        self.channel_dict = defaultdict(list)
        self.iocs = dict()
        self.client = None
        self.currentTime = getCurrentTime
        self.lock = DeferredLock()

    def startService(self):
        service.Service.startService(self)
        self.running = 1
        _log.info("CF_START")
        from channelfinder import ChannelFinderClient
        """
        Using the default python cf-client.
        The url, username, and password are provided by the channelfinder._conf module.
        """
        if self.client is None:  # For setting up mock test client
            self.client = ChannelFinderClient()
        self.clean_service()

    def stopService(self):
        service.Service.stopService(self)
        # Set channels to inactive and close connection to client
        self.running = 0
        self.clean_service()
        _log.info("CF_STOP")

    @defer.inlineCallbacks
    def commit(self, transaction_record):
        yield self.lock.acquire()
        try:
            yield deferToThread(self.__commit__, transaction_record)
        finally:
            self.lock.release()

    def __commit__(self, TR):
        _log.debug("CF_COMMIT %s", TR.infos.items())
        """
        a dictionary with a list of records with their associated property info  
        pvInfo 
        {rid: { "pvName":"recordName",
                "infoProperties":{propName:value, ...}}}
        """
                
        iocName = TR.infos.get('IOCNAME') or TR.src.port
        hostName = TR.infos.get('HOSTNAME') or TR.src.host
        owner = TR.infos.get('ENGINEER') or TR.infos.get('CF_USERNAME') or self.conf.get('username', 'cfstore')
        time = self.currentTime()
        
        pvInfo = {}
        for rid, (rname, rtype) in TR.addrec.iteritems():
            pvInfo[rid] = {"pvName":rname}            
        for rid, (recinfos) in TR.recinfos.iteritems():
            if "properties" in  recinfos:
                if rid in pvInfo:
                    recProperties = recinfos["properties"]
                    properties = []
                    for prop in recProperties.split(","):
                        p = prop.strip().split("=")
                        properties.append({u'name': p[0], u'owner': owner, u'value': p[1]})
                    pvInfo[rid]["infoProperties"] = properties
                else:
                    _log.error("could not find the associated record for properties")
        _log.debug(pvInfo)        
            
        pvNames = [info["pvName"] for rid, (info) in pvInfo.iteritems()]
        
        delrec = list(TR.delrec)
        _log.info("DELETED records " + str(delrec))
        
        host = TR.src.host
        port = TR.src.port
        
        """The unique identifier for a particular IOC"""
        iocid = host + ":" + str(port)
        _log.info("CF_COMMIT: "+iocid)
        
        if TR.initial:
            """Add IOC to source list """
            self.iocs[iocid] = {"iocname": iocName, "hostname": hostName, "owner": owner, "time": time, "channelcount": 0} 
        if not TR.connected:
            delrec.extend(self.channel_dict.keys())
        for pv in pvNames:
            self.channel_dict[pv].append(iocid)  # add iocname to pvName in dict
            self.iocs[iocid]["channelcount"] += 1
        for pv in delrec:
            if iocid in self.channel_dict[pv]:
                self.channel_dict[pv].remove(iocid)
                if iocid in self.iocs:
                    self.iocs[iocid]["channelcount"] -= 1
                if self.iocs[iocid]['channelcount'] == 0:
                    self.iocs.pop(iocid, None)
                elif self.iocs[iocid]['channelcount'] < 0:
                    _log.error("channel count negative!")
                if len(self.channel_dict[pv]) <= 0:  # case: channel has no more iocs
                    del self.channel_dict[pv]
        poll(__updateCF__, self.client, pvInfo, delrec, self.channel_dict, self.iocs, hostName, iocName, iocid, owner, time)
        dict_to_file(self.channel_dict, self.iocs, self.conf)

    def clean_service(self):
        """
        Marks all channels as "Inactive" until the recsync server is back up
        """
        sleep = 1
        retry_limit = 5
        owner = self.conf.get('username', 'cfstore')
        while 1:
            try:
                _log.debug("Cleaning service...")
                channels = self.client.findByArgs([('pvStatus', 'Active')])
                if channels is not None:
                    new_channels = []
                    for ch in channels or []:
                        new_channels.append(ch[u'name'])
                    if len(new_channels) > 0:
                        self.client.update(property={u'name': 'pvStatus', u'owner': owner, u'value': "Inactive"},
                                           channelNames=new_channels)
                    _log.debug("Service clean.")
                    return
            except RequestException:
                _log.exception("cleaning failed, retrying: ")

            time.sleep(min(60, sleep))
            sleep *= 1.5
            if self.running == 0 and sleep >= retry_limit:
                _log.debug("Abandoning clean.")
                return


def dict_to_file(dict, iocs, conf):
    filename = conf.get('debug_file_loc', None)
    if filename:
        if os.path.isfile(filename):
            os.remove(filename)
        list = []
        for key in dict:
            list.append([key, iocs[dict[key][-1]]['hostname'], iocs[dict[key][-1]]['iocname']])

        list.sort(key=itemgetter(0))

        with open(filename, 'w+') as f:
            json.dump(list, f)


def __updateCF__(client, pvInfo, delrec, channels_dict, iocs, hostName, iocName, iocid, owner, iocTime):
    new = [info["pvName"] for rid, (info) in pvInfo.iteritems()]
    
    if hostName is None or iocName is None:
        raise Exception('missing hostName or iocName')
    
    channels = []
    checkPropertiesExist(client, owner)
    """A list of channels in channelfinder with the associated hostName and iocName"""
    old = client.findByArgs([('iocid', iocid)])
    if old is not None:
        for ch in old:
            if new == [] or ch[u'name'] in delrec:  # case: empty commit/del, remove all reference to ioc
                if ch[u'name'] in channels_dict:
                    ch[u'owner'] = iocs[channels_dict[ch[u'name']][-1]]["owner"]
                    ch[u'properties'] = __merge_property_lists([
                        {u'name': 'hostName', u'owner': owner, u'value': iocs[channels_dict[ch[u'name']][-1]]["hostname"]},
                        {u'name': 'iocName', u'owner': owner, u'value': iocs[channels_dict[ch[u'name']][-1]]["iocname"]},                        
                        {u'name': 'iocid', u'owner': owner, u'value': channels_dict[ch[u'name']][-1]},
                        {u'name': 'pvStatus', u'owner': owner, u'value': 'Active'},
                        {u'name': 'time', u'owner': owner, u'value': iocTime}],
                                                               ch[u'properties'])
                    channels.append(ch)
                else:
                    """Orphan the channel : mark as inactive, keep the old hostName and iocName"""
                    ch[u'properties'] = __merge_property_lists([{u'name': 'pvStatus', u'owner': owner, u'value': 'Inactive'},
                                                                {u'name': 'time', u'owner': owner, u'value': iocTime}],
                                                               ch[u'properties'])
                    channels.append(ch)
            else:
                if ch in new:  # case: channel in old and new
                    """
                    Channel exists in Channelfinder with same hostname and iocname.
                    Update the status to ensure it is marked active and update the time. 
                    """
                    ch[u'properties'] = __merge_property_lists([{u'name': 'pvStatus', u'owner': owner, u'value': 'Active'},
                                                                {u'name': 'time', u'owner': owner, u'value': iocTime}],
                                                               ch[u'properties'])
                    channels.append(ch)
                    new.remove(ch[u'name'])

    # now pvNames contains a list of pv's new on this host/ioc
    """A dictionary representing the current channelfinder information associated with the pvNames"""
    existingChannels = {}

    """
    The list of pv's is searched keeping in mind the limitations on the URL length
    The search is split into groups to ensure that the size does not exceed 600 characters
    """
    searchStrings = []
    searchString = ''
    for pv in new:
        if not searchString:
            searchString = pv
        elif (len(searchString) + len(pv) < 600):
            searchString = searchString + '|' + pv
        else:
            searchStrings.append(searchString)
            searchString=pv
    if searchString:
        searchStrings.append(searchString)        
    
    for eachSearchString in searchStrings:
        for ch in client.findByArgs([('~name', eachSearchString)]):
            existingChannels[ch["name"]] = ch
    
    for pv in new:        
        newProps = [{u'name': 'hostName', u'owner': owner, u'value': hostName},
                     {u'name': 'iocName', u'owner': owner, u'value': iocName},
                     {u'name': 'iocid', u'owner': owner, u'value': iocid},
                     {u'name': 'pvStatus', u'owner': owner, u'value': "Active"},
                     {u'name': 'time', u'owner': owner, u'value': iocTime}]
        infoProperties = [info["infoProperties"] for rid, (info) in pvInfo.iteritems() if info["pvName"] == pv and "infoProperties" in info ]
        _log.debug("InfoProperties: " + str(infoProperties))
        if len(infoProperties) == 1:
            newProps = newProps + infoProperties[0]
        _log.debug(newProps)
        if pv in existingChannels:
            """update existing channel: exists but with a different hostName and/or iocName"""            
            existingChannel = existingChannels[pv]            
            existingChannel["properties"] = __merge_property_lists(newProps, existingChannel["properties"])
            channels.append(existingChannel)
        else:
            """New channel"""
            channels.append({u'name': pv,
                             u'owner': owner,
                             u'properties': newProps})
    _log.debug("CHANNELS: %s", json.dumps(str(channels), indent=4))
    if len(channels) != 0:
        client.set(channels=channels)
    else:
        if old and len(old) != 0:
            client.set(channels=channels)

def __merge_property_lists(newProperties, oldProperties):
    """
    Merges two lists of properties ensuring that there are no 2 properties with the same name
    In case of overlap between the new and old property lists the new property list wins out
    """
    newPropNames = [ p[u'name'] for p in newProperties ]
    for oldProperty in oldProperties:
        if oldProperty[u'name'] not in newPropNames:
            newProperties = newProperties + [oldProperty]
    return newProperties

def checkPropertiesExist(client, propOwner):
    """
    Checks if the properties used by dbUpdate are present if not it creates them
    """
    requiredProperties = ['hostName', 'iocName', 'pvStatus', 'time', "iocid"]
    for propName in requiredProperties:
        if client.findProperty(propName) is None:
            try:
                client.set(property={u'name': propName, u'owner': propOwner})
            except Exception:
                _log.exception('Failed to create the property %s', propName)
                raise


def getCurrentTime():
    return str(datetime.datetime.now())


def poll(update, client, pvInfo, delrec, channels_dict, iocs, hostName, iocName, iocid, owner, iocTime):
    _log.debug("Polling begin: ")
    sleep = 1
    success = False
    while not success:
        try:
            update(client, pvInfo, delrec, channels_dict, iocs, hostName, iocName, iocid, owner, iocTime)
            success = True
            return success
        except RequestException as e:
            _log.debug("error: " + str(e.message))
            _log.debug("SLEEP: " + str(min(60, sleep)))
            _log.debug(str(channels_dict))
            time.sleep(min(60, sleep))
            sleep *= 1.5

