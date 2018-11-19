# -*- coding: utf-8 -*-

import logging
_log = logging.getLogger(__name__)

from zope.interface import implementer

from requests import RequestException, ConnectionError
from twisted.application import service
from twisted.internet.threads import deferToThread
from twisted.internet.defer import DeferredLock
from twisted.internet import defer
from operator import itemgetter
from collections import defaultdict
import time
from . import interfaces
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


@implementer(interfaces.IProcessor)
class CFProcessor(service.Service):
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

        if self.client is None:  # For setting up mock test client
            """
            Using the default python cf-client.  The url, username, and
            password are provided by the channelfinder._conf module.
            """
            from channelfinder import ChannelFinderClient
            self.client = ChannelFinderClient()
            try:
                cf_props = [prop['name'] for prop in self.client.getAllProperties()]
                if (self.conf.get('alias', 'default') == 'on'):
                    reqd_props = {'hostName', 'iocName', 'pvStatus', 'time', 'iocid', 'alias'}
                else:
                    reqd_props = {'hostName', 'iocName', 'pvStatus', 'time', 'iocid'}
                wl = self.conf.get('infotags', list())
                whitelist = [s.strip(', ') for s in wl.split()] \
                    if wl else wl
                # Are any required properties not already present on CF?
                properties = reqd_props - set(cf_props)
                # Are any whitelisted properties not already present on CF?
                # If so, add them too.
                properties.update(set(whitelist) - set(cf_props))

                owner = self.conf.get('username', 'cfstore')
                for prop in properties:
                    self.client.set(property={u'name': prop, u'owner': owner})

                self.whitelist = set(whitelist)
                _log.debug('WHITELIST = {}'.format(self.whitelist))
            except ConnectionError:
                _log.exception("Cannot connect to Channelfinder service")
                raise
            else:
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
        for rid, (rname, rtype) in TR.addrec.items():
            pvInfo[rid] = {"pvName": rname}
        for rid, (recinfos) in TR.recinfos.items():
            # find intersection of these sets
            recinfo_wl = [p for p in self.whitelist if p in recinfos.keys()]
            if recinfo_wl:
                pvInfo[rid]['infoProperties'] = list()
                for infotag in recinfo_wl:
                    _log.debug('INFOTAG = {}'.format(infotag))
                    property = {u'name': infotag, u'owner': owner,
                                u'value': recinfos[infotag]}
                    pvInfo[rid]['infoProperties'].append(property)
        for rid, alias in TR.aliases.items():
            pvInfo[rid]['aliases'] = alias
        _log.debug(pvInfo)

        pvNames = [info["pvName"] for rid, (info) in pvInfo.items()]

        delrec = list(TR.delrec)
        _log.info("DELETED records " + str(delrec))

        host = TR.src.host
        port = TR.src.port

        """The unique identifier for a particular IOC"""
        iocid = host + ":" + str(port)
        _log.info("CF_COMMIT: " + iocid)

        if TR.initial:
            """Add IOC to source list """
            self.iocs[iocid] = {"iocname": iocName, "hostname": hostName, "owner": owner, "time": time,
                                "channelcount": 0}
        if not TR.connected:
            delrec.extend(self.channel_dict.keys())
        for pv in pvNames:
            self.channel_dict[pv].append(iocid)  # add iocname to pvName in dict
            self.iocs[iocid]["channelcount"] += 1
            """In case, alias exists"""
            if (self.conf.get('alias', 'default' == 'on')):
                al = [info["aliases"] for rid, (info) in pvInfo.items() if info["pvName"] == pv and "aliases" in info ]
                if len(al) == 1:
                    ali = al[0]
                    for a in ali:
                        self.channel_dict[a].append(iocid)  # add iocname to pvName in dict
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
                """In case, alias exists"""
                if (self.conf.get('alias', 'default' == 'on')):
                    al = [info["aliases"] for rid, (info) in pvInfo.items() if info["pvName"] == pv and "aliases" in info ]
                    if len(al) == 1:
                        ali = al[0]
                        for a in ali:
                            self.channel_dict[a].remove(iocid)
                            if iocid in self.iocs:
                                self.iocs[iocid]["channelcount"] -= 1
                            if self.iocs[iocid]['channelcount'] == 0:
                                self.iocs.pop(iocid, None)
                            elif self.iocs[iocid]['channelcount'] < 0:
                                _log.error("channel count negative!")
                            if len(self.channel_dict[a]) <= 0:  # case: channel has no more iocs
                                del self.channel_dict[a]
        poll(__updateCF__, self.client, pvInfo, delrec, self.channel_dict, self.iocs, self.conf, hostName, iocName, iocid,
             owner, time)
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
                        _log.debug("TANVI new channels %s", new_channels)
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


def __updateCF__(client, pvInfo, delrec, channels_dict, iocs, conf, hostName, iocName, iocid, owner, iocTime):
    new = [info["pvName"] for rid, (info) in pvInfo.items()]
    _log.debug("Here 1")
    if hostName is None or iocName is None:
        raise Exception('missing hostName or iocName')
    _log.debug("Here 2")
    channels = []
    """A list of channels in channelfinder with the associated hostName and iocName"""
    old = client.findByArgs([('iocid', iocid)])
    if old is not None:
        _log.debug("Here 3")
        for ch in old:
            _log.debug("Here 4")
            if new == [] or ch[u'name'] in delrec:  # case: empty commit/del, remove all reference to ioc
                _log.debug("Here 5")
                if ch[u'name'] in channels_dict:
                    _log.debug("Here 6")
                    ch[u'owner'] = iocs[channels_dict[ch[u'name']][-1]]["owner"]
                    ch[u'properties'] = __merge_property_lists([
                        {u'name': 'hostName', u'owner': owner, u'value': iocs[channels_dict[ch[u'name']][-1]]["hostname"]},
                        {u'name': 'iocName', u'owner': owner, u'value': iocs[channels_dict[ch[u'name']][-1]]["iocname"]},                        
                        {u'name': 'iocid', u'owner': owner, u'value': channels_dict[ch[u'name']][-1]},
                        {u'name': 'pvStatus', u'owner': owner, u'value': 'Active'},
                        {u'name': 'time', u'owner': owner, u'value': iocTime}],
                                                               ch[u'properties'])
                    channels.append(ch)
                    """In case alias exist, also delete them"""
                    if (conf.get('alias', 'default') == 'on'):
                        al = [info["aliases"] for rid, (info) in pvInfo.items() if info["pvName"] == ch and "aliases" in info ]
                        if len(al) == 1:
                            ali = al[0]
                            for a in ali:
                                if a[u'name'] in channels_dict:
                                    a[u'owner'] = iocs[channels_dict[a[u'name']][-1]]["owner"]
                                    a[u'properties'] = __merge_property_lists([
                                        {u'name': 'hostName', u'owner': owner, u'value': iocs[channels_dict[a[u'name']][-1]]["hostname"]},
                                        {u'name': 'iocName', u'owner': owner, u'value': iocs[channels_dict[a[u'name']][-1]]["iocname"]},                        
                                        {u'name': 'iocid', u'owner': owner, u'value': channels_dict[a[u'name']][-1]},
                                        {u'name': 'pvStatus', u'owner': owner, u'value': 'Active'},
                                        {u'name': 'time', u'owner': owner, u'value': iocTime}],
                                                                            a[u'properties'])
                                    channels.append(a)

                else:
                    _log.debug("Here 7")
                    """Orphan the channel : mark as inactive, keep the old hostName and iocName"""
                    ch[u'properties'] = __merge_property_lists([{u'name': 'pvStatus', u'owner': owner, u'value': 'Inactive'},
                                                                {u'name': 'time', u'owner': owner, u'value': iocTime}],
                                                               ch[u'properties'])
                    channels.append(ch)
                    """Also orphan any alias"""
                    if (conf.get('alias', 'default') == 'on'):
                        al = [info["aliases"] for rid, (info) in pvInfo.items() if info["pvName"] == ch and "aliases" in info ]
                        if len(al) == 1:
                            _log.debug("Here 8")
                            ali = al[0]
                            for a in ali:
                                a[u'properties'] = __merge_property_lists([{u'name': 'pvStatus', u'owner': owner, u'value': 'Inactive'},
                                                                    {u'name': 'time', u'owner': owner, u'value': iocTime}],
                                                                a[u'properties'])
                                channels.append(a)
            else:
                _log.debug("Here 9")
                if ch in new:  # case: channel in old and new
                    _log.debug("Here 10")
                    """
                    Channel exists in Channelfinder with same hostname and iocname.
                    Update the status to ensure it is marked active and update the time. 
                    """
                    ch[u'properties'] = __merge_property_lists([{u'name': 'pvStatus', u'owner': owner, u'value': 'Active'},
                                                                {u'name': 'time', u'owner': owner, u'value': iocTime}],
                                                               ch[u'properties'])
                    channels.append(ch)
                    new.remove(ch[u'name'])
                    """In case, alias exist"""
                    if (conf.get('alias', 'default') == 'on'):
                        al = [info["aliases"] for rid, (info) in pvInfo.items() if info["pvName"] == ch and "aliases" in info ]
                        _log.debug("TANVI aliasProperties: " + str(al))
                        if len(al) == 1:
                            _log.debug("Here 11")
                            ali = al[0]
                            for a in ali:
                                _log.debug("Here 12")
                                if a in old:
                                    _log.debug("Here 13")
                                    """alias exists in old list"""
                                    a[u'properties'] = __merge_property_lists([{u'name': 'pvStatus', u'owner': owner, u'value': 'Active'},
                                                                    {u'name': 'time', u'owner': owner, u'value': iocTime}],
                                                                    a[u'properties'])
                                    channels.append(a)
                                    new.remove(a[u'name'])
                                else:
                                    _log.debug("Here 14")
                                    """alias exists but not part of old list"""
                                    aprops = __merge_property_lists([{u'name': 'pvStatus', u'owner': owner, u'value': 'Active'},
                                                                    {u'name': 'time', u'owner': owner, u'value': iocTime},
                                                                    {u'name': 'alias', u'owner': owner, u'value': ch}],
                                                                ch[u'properties'])
                                    channels.append({u'name': a,
                                                    u'owner': owner,
                                                    u'properties': aprops})
                                    new.remove(a[u'name'])
    # now pvNames contains a list of pv's new on this host/ioc
    """A dictionary representing the current channelfinder information associated with the pvNames"""
    existingChannels = {}
    _log.debug("Here 15")
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
    _log.debug("Here 16")
    for pv in new:
        _log.debug("Here 17")        
        newProps = [{u'name': 'hostName', u'owner': owner, u'value': hostName},
                     {u'name': 'iocName', u'owner': owner, u'value': iocName},
                     {u'name': 'iocid', u'owner': owner, u'value': iocid},
                     {u'name': 'pvStatus', u'owner': owner, u'value': "Active"},
                     {u'name': 'time', u'owner': owner, u'value': iocTime}]
        infoProperties = [info["infoProperties"] for rid, (info) in pvInfo.items() if info["pvName"] == pv and "infoProperties" in info ]
        _log.debug("InfoProperties: " + str(infoProperties))
        if len(infoProperties) == 1:
            newProps = newProps + infoProperties[0]
        _log.debug(newProps)
        aliasProperties = [info["aliases"] for rid, (info) in pvInfo.items() if info["pvName"] == pv and "aliases" in info ]
        _log.debug("aliasProperties: " + str(aliasProperties))
        if pv in existingChannels:
            _log.debug("Here 18")
            """update existing channel: exists but with a different hostName and/or iocName"""            
            existingChannel = existingChannels[pv]            
            existingChannel["properties"] = __merge_property_lists(newProps, existingChannel["properties"])
            channels.append(existingChannel)
            """in case, alias exists, update their properties too"""
            if (conf.get('alias', 'default') == 'on'):
                if len(aliasProperties) == 1:
                    _log.debug("Here 18.1")
                    alist = aliasProperties[0]
                    parentRec = {u'name': 'alias', u'owner': owner, u'value': pv}
                    newProps.append(parentRec)
                    for a in alist:
                        _log.debug("Here 18.2")
                        if a in existingChannels:
                            _log.debug("Here 18.3")
                            ach = existingChannels[a]
                            ach["properties"] = __merge_property_lists(newProps, ach["properties"])
                            channels.append(ach)
                        else:
                            _log.debug("Here 18.4")
                            channels.append({u'name': a,
                                    u'owner': owner,
                                    u'properties': newProps})

        else:
            _log.debug("Here 19")
            """New channel"""
            channels.append({u'name': pv,
                             u'owner': owner,
                             u'properties': newProps})
            if (conf.get('alias', 'default') == 'on'):
                if len(aliasProperties) == 1:
                    _log.debug("Here 20")
                    parentRec = {u'name': 'alias', u'owner': owner, u'value': pv}
                    newProps.append(parentRec)
                    alist = aliasProperties[0]
                    for a in alist:
                        channels.append({u'name': a,
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
    Merges two lists of properties ensuring that there are no 2 properties with
    the same name In case of overlap between the new and old property lists the
    new property list wins out
    """
    newPropNames = [ p[u'name'] for p in newProperties ]
    for oldProperty in oldProperties:
        if oldProperty[u'name'] not in newPropNames:
            newProperties = newProperties + [oldProperty]
    return newProperties


def getCurrentTime():
    return str(datetime.datetime.now())


def poll(update, client, pvInfo, delrec, channels_dict, iocs, conf, hostName, iocName, iocid, owner, iocTime):
    _log.debug("Polling begin: ")
    sleep = 1
    success = False
    while not success:
        try:
            update(client, pvInfo, delrec, channels_dict, iocs, conf, hostName, iocName, iocid, owner, iocTime)
            success = True
            return success
        except RequestException as e:
            _log.debug("error: " + str(e.message))
            _log.debug("SLEEP: " + str(min(60, sleep)))
            _log.debug(str(channels_dict))
            time.sleep(min(60, sleep))
            sleep *= 1.5

