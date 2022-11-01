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
        # Returning a Deferred is not supported by startService(),
        # so instead attempt to acquire the lock synchonously!
        d = self.lock.acquire()
        if not d.called:
            d.cancel()
            service.Service.stopService(self)
            raise RuntimeError('Failed to acquired CF Processor lock for service start')

        try:
            self._startServiceWithLock()
        except:
            service.Service.stopService(self)
            raise
        finally:
            self.lock.release()

    def _startServiceWithLock(self):
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
                reqd_props = {'hostName', 'iocName', 'pvStatus', 'time', 'iocid'}
                if (self.conf.get('alias', 'default') == 'on'):
                    reqd_props.add('alias')
                if (self.conf.get('recordType', 'default') == 'on'):
                    reqd_props.add('recordType')
                env_vars_setting = self.conf.get('environment_vars')
                self.env_vars = {}
                if env_vars_setting != "" and env_vars_setting is not None:
                    env_vars_dict = dict(item.strip().split(":") for item in env_vars_setting.split(","))
                    self.env_vars = { k.strip():v.strip() for k, v in env_vars_dict.items()}
                    for epics_env_var_name, cf_prop_name in self.env_vars.items():
                        reqd_props.add(cf_prop_name)
                wl = self.conf.get('infotags', list())
                whitelist = [s.strip(', ') for s in wl.split()] \
                    if wl else wl
                if (self.conf.get('recordDesc', 'default') == 'on'):
                    whitelist.append('recordDesc')
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
                if self.conf.getboolean('cleanOnStart', True):
                    self.clean_service()

    def stopService(self):
        service.Service.stopService(self)
        return self.lock.run(self._stopServiceWithLock)

    def _stopServiceWithLock(self):
        # Set channels to inactive and close connection to client
        if self.conf.getboolean('cleanOnStop', True):
            self.clean_service()
        _log.info("CF_STOP")

    # @defer.inlineCallbacks # Twisted v16 does not support cancellation!
    def commit(self, transaction_record):
        return self.lock.run(self._commitWithLock, transaction_record)

    def _commitWithLock(self, TR):
        self.cancelled = False

        t = deferToThread(self._commitWithThread, TR)

        def cancelCommit(d):
            self.cancelled = True
            d.callback(None)

        d = defer.Deferred(cancelCommit)

        def waitForThread(_ignored):
            if self.cancelled:
                return t

        d.addCallback(waitForThread)

        def chainError(err):
            if not err.check(defer.CancelledError):
                _log.error("CF_COMMIT FAILURE: %s", err)
            if self.cancelled:
                if not err.check(defer.CancelledError):
                    raise defer.CancelledError()
                return err
            else:
                d.callback(None)

        def chainResult(_ignored):
            if self.cancelled:
                raise defer.CancelledError()
            else:
                d.callback(None)

        t.addCallbacks(chainResult, chainError)
        return d

    def _commitWithThread(self, TR):
        if not self.running:
            raise defer.CancelledError('CF Processor is not running (TR: %s:%s)', TR.src.host, TR.src.port)

        _log.info("CF_COMMIT: %s", TR)
        """
        a dictionary with a list of records with their associated property info  
        pvInfo 
        {rid: { "pvName":"recordName",
                "infoProperties":{propName:value, ...}}}
        """

        host = TR.src.host
        port = TR.src.port
        iocName = TR.infos.get('IOCNAME') or TR.src.port
        hostName = TR.infos.get('HOSTNAME') or TR.src.host
        owner = TR.infos.get('ENGINEER') or TR.infos.get('CF_USERNAME') or self.conf.get('username', 'cfstore')
        time = self.currentTime()

        """The unique identifier for a particular IOC"""
        iocid = host + ":" + str(port)

        pvInfo = {}
        for rid, (rname, rtype) in TR.addrec.items():
            pvInfo[rid] = {"pvName": rname}
            if (self.conf.get('recordType', 'default' == 'on')):
                pvInfo[rid]['recordType'] = rtype
        for rid, (recinfos) in TR.recinfos.items():
            # find intersection of these sets
            if rid not in pvInfo:
                _log.warn('IOC: %s: PV not found for recinfo with RID: %s', iocid, rid)
                continue
            recinfo_wl = [p for p in self.whitelist if p in recinfos.keys()]
            if recinfo_wl:
                pvInfo[rid]['infoProperties'] = list()
                for infotag in recinfo_wl:
                    property = {u'name': infotag, u'owner': owner,
                                u'value': recinfos[infotag]}
                    pvInfo[rid]['infoProperties'].append(property)

        for rid, alias in TR.aliases.items():
            if rid not in pvInfo:
                _log.warn('IOC: %s: PV not found for alias with RID: %s', iocid, rid)
                continue
            pvInfo[rid]['aliases'] = alias

        for rid in pvInfo:
            for epics_env_var_name, cf_prop_name in self.env_vars.items():
                if TR.infos.get(epics_env_var_name) is not None:
                    property = {u'name': cf_prop_name, u'owner': owner,
                                u'value': TR.infos.get(epics_env_var_name)}
                    if "infoProperties" not in pvInfo[rid]:
                        pvInfo[rid]['infoProperties'] = list()
                    pvInfo[rid]['infoProperties'].append(property)
                else:
                    _log.debug('EPICS environment var %s listed in environment_vars setting list not found in this IOC: %s', epics_env_var_name, iocName)

        delrec = list(TR.delrec)
        _log.debug("Delete records: %s", delrec)


        pvInfoByName = {}
        for rid, (info) in pvInfo.items():
            if info["pvName"] in pvInfoByName:
                _log.warn("Commit contains multiple records with PV name: %s (%s)", pv, iocid)
                continue
            pvInfoByName[info["pvName"]] = info
            _log.debug("Add record: %s: %s", rid, info)

        if TR.initial:
            """Add IOC to source list """
            self.iocs[iocid] = {"iocname": iocName, "hostname": hostName, "owner": owner, "time": time,
                                "channelcount": 0}
        if not TR.connected:
            delrec.extend(self.channel_dict.keys())
        for pv in pvInfoByName.keys():
            self.channel_dict[pv].append(iocid)  # add iocname to pvName in dict
            self.iocs[iocid]["channelcount"] += 1
            """In case, alias exists"""
            if (self.conf.get('alias', 'default' == 'on')):
                if pv in pvInfoByName and "aliases" in pvInfoByName[pv]:
                    for a in pvInfoByName[pv]["aliases"]:
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
                    _log.error("Channel count negative: %s", iocid)
                if len(self.channel_dict[pv]) <= 0:  # case: channel has no more iocs
                    del self.channel_dict[pv]
                """In case, alias exists"""
                if (self.conf.get('alias', 'default' == 'on')):
                    if pv in pvInfoByName and "aliases" in pvInfoByName[pv]:
                        for a in pvInfoByName[pv]["aliases"]:
                            self.channel_dict[a].remove(iocid)
                            if iocid in self.iocs:
                                self.iocs[iocid]["channelcount"] -= 1
                            if self.iocs[iocid]['channelcount'] == 0:
                                self.iocs.pop(iocid, None)
                            elif self.iocs[iocid]['channelcount'] < 0:
                                _log.error("Channel count negative: %s", iocid)
                            if len(self.channel_dict[a]) <= 0:  # case: channel has no more iocs
                                del self.channel_dict[a]
        poll(__updateCF__, self, pvInfoByName, delrec, hostName, iocName, iocid, owner, time)
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
                _log.info("CF Clean Started")
                channels = self.client.findByArgs(prepareFindArgs(self.conf, [('pvStatus', 'Active')]))
                if channels is not None:
                    new_channels = []
                    for ch in channels or []:
                        new_channels.append(ch[u'name'])
                    _log.info("Total channels to update: %s", len(new_channels))
                    while len(new_channels) > 0:
                        _log.debug('Update "pvStatus" property to "Inactive" for %s channels', min(len(new_channels), 10000))
                        self.client.update(property={u'name': 'pvStatus', u'owner': owner, u'value': "Inactive"},
                                           channelNames=new_channels[:10000])
                        new_channels = new_channels[10000:]
                    _log.info("CF Clean Completed")
                    return
            except RequestException as e:
                _log.error("Clean service failed: %s", e)

            _log.info("Clean service retry in %s seconds", min(60, sleep))
            time.sleep(min(60, sleep))
            sleep *= 1.5
            if self.running == 0 and sleep >= retry_limit:
                _log.info("Abandoning clean after %s seconds", retry_limit)
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


def __updateCF__(proc, pvInfoByName, delrec, hostName, iocName, iocid, owner, iocTime):
    # Consider making this function a class methed then 'proc' simply becomes 'self'
    client = proc.client
    channels_dict = proc.channel_dict
    iocs = proc.iocs
    conf = proc.conf
    new = set(pvInfoByName.keys())

    if iocid in iocs:
        hostName = iocs[iocid]["hostname"]
        iocName = iocs[iocid]["iocname"]
        owner = iocs[iocid]["owner"]
        iocTime = iocs[iocid]["time"]
    else:
        _log.warn('IOC Env Info not found: %s', iocid)

    if hostName is None or iocName is None:
        raise Exception('missing hostName or iocName')

    if proc.cancelled:
        raise defer.CancelledError()

    channels = []
    """A list of channels in channelfinder with the associated hostName and iocName"""
    _log.debug('Find existing channels by IOCID: %s', iocid)
    old = client.findByArgs(prepareFindArgs(conf, [('iocid', iocid)]))
    if proc.cancelled:
        raise defer.CancelledError()

    if old is not None:
        for ch in old:
            if len(new) == 0 or ch[u'name'] in delrec:  # case: empty commit/del, remove all reference to ioc
                if ch[u'name'] in channels_dict:
                    ch[u'owner'] = iocs[channels_dict[ch[u'name']][-1]]["owner"]
                    ch[u'properties'] = __merge_property_lists([
                        {u'name': 'hostName', u'owner': owner, u'value': iocs[channels_dict[ch[u'name']][-1]]["hostname"]},
                        {u'name': 'iocName', u'owner': owner, u'value': iocs[channels_dict[ch[u'name']][-1]]["iocname"]},                        
                        {u'name': 'iocid', u'owner': owner, u'value': channels_dict[ch[u'name']][-1]},
                        {u'name': 'pvStatus', u'owner': owner, u'value': 'Active'},
                        {u'name': 'time', u'owner': owner, u'value': iocTime}],
                                                               ch[u'properties'])
                    if (conf.get('recordType', 'default') == 'on'):
                        ch[u'properties'] = __merge_property_lists(ch[u'properties'].append({u'name': 'recordType', u'owner': owner, u'value': iocs[channels_dict[ch[u'name']][-1]]["recordType"]}), ch[u'properties'])
                    channels.append(ch)
                    _log.debug("Add existing channel to previous IOC: %s", channels[-1])
                    """In case alias exist, also delete them"""
                    if (conf.get('alias', 'default') == 'on'):
                        if ch[u'name'] in pvInfoByName and "aliases" in pvInfoByName[ch[u'name']]:
                            for a in pvInfoByName[ch[u'name']]["aliases"]:
                                if a[u'name'] in channels_dict:
                                    a[u'owner'] = iocs[channels_dict[a[u'name']][-1]]["owner"]
                                    a[u'properties'] = __merge_property_lists([
                                        {u'name': 'hostName', u'owner': owner, u'value': iocs[channels_dict[a[u'name']][-1]]["hostname"]},
                                        {u'name': 'iocName', u'owner': owner, u'value': iocs[channels_dict[a[u'name']][-1]]["iocname"]},                        
                                        {u'name': 'iocid', u'owner': owner, u'value': channels_dict[a[u'name']][-1]},
                                        {u'name': 'pvStatus', u'owner': owner, u'value': 'Active'},
                                        {u'name': 'time', u'owner': owner, u'value': iocTime}],
                                                                            a[u'properties'])
                                    if (conf.get('recordType', 'default') == 'on'):
                                        ch[u'properties'] = __merge_property_lists(ch[u'properties'].append({u'name': 'recordType', u'owner': owner, u'value': iocs[channels_dict[a[u'name']][-1]]["recordType"]}), ch[u'properties'])
                                    channels.append(a)
                                    _log.debug("Add existing alias to previous IOC: %s", channels[-1])

                else:
                    """Orphan the channel : mark as inactive, keep the old hostName and iocName"""
                    ch[u'properties'] = __merge_property_lists([{u'name': 'pvStatus', u'owner': owner, u'value': 'Inactive'},
                                                                {u'name': 'time', u'owner': owner, u'value': iocTime}],
                                                               ch[u'properties'])
                    channels.append(ch)
                    _log.debug("Add orphaned channel with no IOC: %s", channels[-1])
                    """Also orphan any alias"""
                    if (conf.get('alias', 'default') == 'on'):
                        if ch[u'name'] in pvInfoByName and "aliases" in pvInfoByName[ch[u'name']]:
                            for a in pvInfoByName[ch[u'name']]["aliases"]:
                                a[u'properties'] = __merge_property_lists([{u'name': 'pvStatus', u'owner': owner, u'value': 'Inactive'},
                                                                    {u'name': 'time', u'owner': owner, u'value': iocTime}],
                                                                    a[u'properties'])
                                channels.append(a)
                                _log.debug("Add orphaned alias with no IOC: %s", channels[-1])
            else:
                if ch[u'name'] in new:  # case: channel in old and new
                    """
                    Channel exists in Channelfinder with same hostname and iocname.
                    Update the status to ensure it is marked active and update the time. 
                    """
                    ch[u'properties'] = __merge_property_lists([{u'name': 'pvStatus', u'owner': owner, u'value': 'Active'},
                                                                {u'name': 'time', u'owner': owner, u'value': iocTime}],
                                                               ch[u'properties'])
                    channels.append(ch)
                    _log.debug("Add existing channel with same IOC: %s", channels[-1])
                    new.remove(ch[u'name'])

                    """In case, alias exist"""
                    if (conf.get('alias', 'default') == 'on'):
                        if ch[u'name'] in pvInfoByName and "aliases" in pvInfoByName[ch[u'name']]:
                            for a in pvInfoByName[ch[u'name']]["aliases"]:
                                if a in old:
                                    """alias exists in old list"""
                                    a[u'properties'] = __merge_property_lists([{u'name': 'pvStatus', u'owner': owner, u'value': 'Active'},
                                                                    {u'name': 'time', u'owner': owner, u'value': iocTime}],
                                                                    a[u'properties'])
                                    channels.append(a)
                                    new.remove(a[u'name'])
                                else:
                                    """alias exists but not part of old list"""
                                    aprops = __merge_property_lists([{u'name': 'pvStatus', u'owner': owner, u'value': 'Active'},
                                                                    {u'name': 'time', u'owner': owner, u'value': iocTime},
                                                                    {u'name': 'alias', u'owner': owner, u'value': ch[u'name']}],
                                                                ch[u'properties'])
                                    channels.append({u'name': a[u'name'],
                                                    u'owner': owner,
                                                    u'properties': aprops})
                                    new.remove(a[u'name'])
                                _log.debug("Add existing alias with same IOC: %s", channels[-1])
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
        _log.debug('Find existing channels by name: %s', eachSearchString)
        for ch in client.findByArgs(prepareFindArgs(conf, [('~name', eachSearchString)])):
            existingChannels[ch["name"]] = ch
        if proc.cancelled:
            raise defer.CancelledError()

    for pv in new:  
        newProps = [{u'name': 'hostName', u'owner': owner, u'value': hostName},
                     {u'name': 'iocName', u'owner': owner, u'value': iocName},
                     {u'name': 'iocid', u'owner': owner, u'value': iocid},
                     {u'name': 'pvStatus', u'owner': owner, u'value': "Active"},
                     {u'name': 'time', u'owner': owner, u'value': iocTime}]
        if (conf.get('recordType', 'default') == 'on'):
            newProps.append({u'name': 'recordType', u'owner': owner, u'value': pvInfoByName[pv]['recordType']})
        if pv in pvInfoByName and "infoProperties" in pvInfoByName[pv]:
            newProps = newProps + pvInfoByName[pv]["infoProperties"]

        if pv in existingChannels:
            """update existing channel: exists but with a different hostName and/or iocName"""            
            existingChannel = existingChannels[pv]            
            existingChannel["properties"] = __merge_property_lists(newProps, existingChannel["properties"])
            channels.append(existingChannel)
            _log.debug("Add existing channel with different IOC: %s", channels[-1])
            """in case, alias exists, update their properties too"""
            if (conf.get('alias', 'default') == 'on'):
                if pv in pvInfoByName and "aliases" in pvInfoByName[pv]:
                    alProps = [{u'name': 'alias', u'owner': owner, u'value': pv}]
                    for p in newProps:
                        alProps.append(p)
                    for a in pvInfoByName[pv]["aliases"]:
                        if a in existingChannels:
                            ach = existingChannels[a]
                            ach["properties"] = __merge_property_lists(alProps, ach["properties"])
                            channels.append(ach)
                        else:
                            channels.append({u'name': a,
                                    u'owner': owner,
                                    u'properties': alProps})
                        _log.debug("Add existing alias with different IOC: %s", channels[-1])

        else:
            """New channel"""
            channels.append({u'name': pv,
                             u'owner': owner,
                             u'properties': newProps})
            _log.debug("Add new channel: %s", channels[-1])
            if (conf.get('alias', 'default') == 'on'):
                if pv in pvInfoByName and "aliases" in pvInfoByName[pv]:
                    alProps = [{u'name': 'alias', u'owner': owner, u'value': pv}]
                    for p in newProps:
                        alProps.append(p)
                    for a in pvInfoByName[pv]["aliases"]:
                        channels.append({u'name': a,
                                    u'owner': owner,
                                    u'properties': alProps})
                        _log.debug("Add new alias: %s", channels[-1])
    _log.info("Total channels to update: %s", len(channels))
    if len(channels) != 0:
        client.set(channels=channels)
    else:
        if old and len(old) != 0:
            client.set(channels=channels)
    if proc.cancelled:
        raise defer.CancelledError()

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


def prepareFindArgs(conf, args):
    size = conf.get('findSizeLimit', 0)
    if size > 0:
        args.append(('~size', size))
    return args


def poll(update, proc, pvInfoByName, delrec, hostName, iocName, iocid, owner, iocTime):
    _log.debug("Polling begins...")
    sleep = 1
    success = False
    while not success:
        try:
            update(proc, pvInfoByName, delrec, hostName, iocName, iocid, owner, iocTime)
            success = True
            return success
        except RequestException as e:
            _log.error("ChannelFinder update failed: %s", e)
            _log.info("ChannelFinder update retry in %s seconds", min(60, sleep))
            #_log.debug(str(channels_dict))
            time.sleep(min(60, sleep))
            sleep *= 1.5
    _log.debug("Polling complete")
