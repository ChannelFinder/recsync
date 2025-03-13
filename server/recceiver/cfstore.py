# -*- coding: utf-8 -*-

import datetime
import json
import logging
import os
import socket
import time
from collections import defaultdict
from operator import itemgetter

from channelfinder import ChannelFinderClient
from requests import ConnectionError, RequestException
from zope.interface import implementer

from twisted.application import service
from twisted.internet import defer
from twisted.internet.defer import DeferredLock
from twisted.internet.threads import deferToThread

from . import interfaces

_log = logging.getLogger(__name__)

# ITRANSACTION FORMAT:
#
# source_address = source address
# records_to_add = records ein added ( recname, rectype, {key:val})
# records_to_delete = a set() of records which are being removed
# client_infos = dictionary of client client_infos
# record_infos_to_add = additional client_infos being added to existing records
# "recid: {key:value}"
#

__all__ = ["CFProcessor"]

RECCEIVERID_KEY = "recceiverID"
RECCEIVERID_DEFAULT = socket.gethostname()


@implementer(interfaces.IProcessor)
class CFProcessor(service.Service):
    def __init__(self, name, conf):
        _log.info("CF_INIT {name}".format(name=name))
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
            raise RuntimeError("Failed to acquired CF Processor lock for service start")

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
            self.client = ChannelFinderClient()
            try:
                cf_properties = [cf_property["name"] for cf_property in self.client.getAllProperties()]
                required_properties = {
                    "hostName",
                    "iocName",
                    "pvStatus",
                    "time",
                    "iocid",
                    "iocIP",
                    RECCEIVERID_KEY,
                }

                if self.conf.get("alias"):
                    required_properties.add("alias")
                if self.conf.get("recordType"):
                    required_properties.add("recordType")
                env_vars_setting = self.conf.get("environment_vars")
                self.env_vars = {}
                if env_vars_setting != "" and env_vars_setting is not None:
                    env_vars_dict = dict(item.strip().split(":") for item in env_vars_setting.split(","))
                    self.env_vars = {k.strip(): v.strip() for k, v in env_vars_dict.items()}
                    for epics_env_var_name, cf_prop_name in self.env_vars.items():
                        required_properties.add(cf_prop_name)
                # Standard property names for CA/PVA name server connections. These are
                # environment variables from reccaster so take advantage of env_vars
                if self.conf.get("iocConnectionInfo"):
                    self.env_vars["RSRV_SERVER_PORT"] = "caPort"
                    self.env_vars["PVAS_SERVER_PORT"] = "pvaPort"
                    required_properties.add("caPort")
                    required_properties.add("pvaPort")
                infotags_whitelist = self.conf.get("infotags", list())
                if infotags_whitelist:
                    record_property_names_list = [s.strip(", ") for s in infotags_whitelist.split()]
                else:
                    record_property_names_list = []
                if self.conf.get("recordDesc"):
                    record_property_names_list.append("recordDesc")
                # Are any required properties not already present on CF?
                properties = required_properties - set(cf_properties)
                # Are any whitelisted properties not already present on CF?
                # If so, add them too.
                properties.update(set(record_property_names_list) - set(cf_properties))

                owner = self.conf.get("username", "cfstore")
                for cf_property in properties:
                    self.client.set(property={"name": cf_property, "owner": owner})

                self.record_property_names_list = set(record_property_names_list)
                _log.debug("record_property_names_list = {}".format(self.record_property_names_list))
            except ConnectionError:
                _log.exception("Cannot connect to Channelfinder service")
                raise
            else:
                if self.conf.getboolean("cleanOnStart", True):
                    self.clean_service()

    def stopService(self):
        _log.info("CF_STOP")
        service.Service.stopService(self)
        return self.lock.run(self._stopServiceWithLock)

    def _stopServiceWithLock(self):
        # Set channels to inactive and close connection to client
        if self.conf.getboolean("cleanOnStop", True):
            self.clean_service()
        _log.info("CF_STOP with lock")

    # @defer.inlineCallbacks # Twisted v16 does not support cancellation!
    def commit(self, transaction_record):
        return self.lock.run(self._commitWithLock, transaction_record)

    def _commitWithLock(self, transaction):
        self.cancelled = False

        t = deferToThread(self._commitWithThread, transaction)

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
                _log.error("CF_COMMIT FAILURE: {s}".format(s=err))
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

    def _commitWithThread(self, transaction):
        if not self.running:
            raise defer.CancelledError(
                "CF Processor is not running (transaction: {host}:{port})",
                host=transaction.source_address.host,
                port=transaction.source_address.port,
            )

        _log.info("CF_COMMIT: {transaction}".format(transaction=transaction))
        """
        a dictionary with a list of records with their associated property info
        pvInfo
        {record_id: { "pvName":"recordName",
                "infoProperties":{propName:value, ...}}}
        """

        host = transaction.source_address.host
        port = transaction.source_address.port
        iocName = transaction.client_infos.get("IOCNAME") or transaction.source_address.port
        hostName = transaction.client_infos.get("HOSTNAME") or transaction.source_address.host
        owner = (
            transaction.client_infos.get("ENGINEER")
            or transaction.client_infos.get("CF_USERNAME")
            or self.conf.get("username", "cfstore")
        )
        time = self.currentTime(timezone=self.conf.get("timezone"))

        """The unique identifier for a particular IOC"""
        iocid = host + ":" + str(port)

        recordInfo = {}
        for record_id, (record_name, record_type) in transaction.records_to_add.items():
            recordInfo[record_id] = {"pvName": record_name}
            if self.conf.get("recordType"):
                recordInfo[record_id]["recordType"] = record_type
        for record_id, (record_infos_to_add) in transaction.record_infos_to_add.items():
            # find intersection of these sets
            if record_id not in recordInfo:
                _log.warning(
                    "IOC: {iocid}: PV not found for recinfo with RID: {record_id}".format(
                        iocid=iocid, record_id=record_id
                    )
                )
                continue
            recinfo_wl = [p for p in self.record_property_names_list if p in record_infos_to_add.keys()]
            if recinfo_wl:
                recordInfo[record_id]["infoProperties"] = list()
                for infotag in recinfo_wl:
                    recordInfo[record_id]["infoProperties"].append(
                        create_property(owner, infotag, record_infos_to_add[infotag])
                    )

        for record_id, alias in transaction.aliases.items():
            if record_id not in recordInfo:
                _log.warning(
                    "IOC: {iocid}: PV not found for alias with RID: {record_id}".format(
                        iocid=iocid, record_id=record_id
                    )
                )
                continue
            recordInfo[record_id]["aliases"] = alias

        for record_id in recordInfo:
            for epics_env_var_name, cf_prop_name in self.env_vars.items():
                if transaction.client_infos.get(epics_env_var_name) is not None:
                    if "infoProperties" not in recordInfo[record_id]:
                        recordInfo[record_id]["infoProperties"] = list()
                    recordInfo[record_id]["infoProperties"].append(
                        create_property(owner, cf_prop_name, transaction.client_infos.get(epics_env_var_name))
                    )
                else:
                    _log.debug(
                        "EPICS environment var %s listed in environment_vars setting list not found in this IOC: %s",
                        epics_env_var_name,
                        iocName,
                    )

        records_to_delete = list(transaction.records_to_delete)
        _log.debug("Delete records: {s}".format(s=records_to_delete))

        recordInfoByName = {}
        for record_id, (info) in recordInfo.items():
            if info["pvName"] in recordInfoByName:
                _log.warning(
                    "Commit contains multiple records with PV name: {pv} ({iocid})".format(
                        pv=info["pvName"], iocid=iocid
                    )
                )
                continue
            recordInfoByName[info["pvName"]] = info
            _log.debug("Add record: {record_id}: {info}".format(record_id=record_id, info=info))
            _log.debug("Add record: {record_id}: {info}".format(record_id=record_id, info=info))

        if transaction.initial:
            """Add IOC to source list """
            self.iocs[iocid] = {
                "iocname": iocName,
                "hostname": hostName,
                "iocIP": host,
                "owner": owner,
                "time": time,
                "channelcount": 0,
            }
        if not transaction.connected:
            records_to_delete.extend(self.channel_dict.keys())
        for record_name in recordInfoByName.keys():
            self.channel_dict[record_name].append(iocid)
            self.iocs[iocid]["channelcount"] += 1
            """In case, alias exists"""
            if self.conf.get("alias"):
                if record_name in recordInfoByName and "aliases" in recordInfoByName[record_name]:
                    for alias in recordInfoByName[record_name]["aliases"]:
                        self.channel_dict[alias].append(iocid)  # add iocname to pvName in dict
                        self.iocs[iocid]["channelcount"] += 1
        for record_name in records_to_delete:
            if iocid in self.channel_dict[record_name]:
                self.remove_channel(record_name, iocid)
                """In case, alias exists"""
                if self.conf.get("alias"):
                    if record_name in recordInfoByName and "aliases" in recordInfoByName[record_name]:
                        for alias in recordInfoByName[record_name]["aliases"]:
                            self.remove_channel(alias, iocid)
        poll(
            __updateCF__,
            self,
            recordInfoByName,
            records_to_delete,
            hostName,
            iocName,
            host,
            iocid,
            owner,
            time,
        )
        dict_to_file(self.channel_dict, self.iocs, self.conf)

    def remove_channel(self, recordName, iocid):
        self.channel_dict[recordName].remove(iocid)
        if iocid in self.iocs:
            self.iocs[iocid]["channelcount"] -= 1
        if self.iocs[iocid]["channelcount"] == 0:
            self.iocs.pop(iocid, None)
        elif self.iocs[iocid]["channelcount"] < 0:
            _log.error("Channel count negative: {s}", s=iocid)
        if len(self.channel_dict[recordName]) <= 0:  # case: channel has no more iocs
            del self.channel_dict[recordName]

    def clean_service(self):
        """
        Marks all channels as "Inactive" until the recsync server is back up
        """
        sleep = 1
        retry_limit = 5
        owner = self.conf.get("username", "cfstore")
        recceiverid = self.conf.get(RECCEIVERID_KEY, RECCEIVERID_DEFAULT)
        while 1:
            try:
                _log.info("CF Clean Started")
                channels = self.get_active_channels(recceiverid)
                if channels is not None:
                    while channels is not None and len(channels) > 0:
                        self.clean_channels(owner, channels)
                        channels = self.get_active_channels(recceiverid)
                    _log.info("CF Clean Completed")
                    return
                else:
                    _log.info("CF Clean Completed")
                    return
            except RequestException as e:
                _log.error("Clean service failed: {s}".format(s=e))
            retry_seconds = min(60, sleep)
            _log.info("Clean service retry in {retry_seconds} seconds".format(retry_seconds=retry_seconds))
            time.sleep(retry_seconds)
            sleep *= 1.5
            if self.running == 0 and sleep >= retry_limit:
                _log.info("Abandoning clean after {retry_limit} seconds".format(retry_limit=retry_limit))
                return

    def get_active_channels(self, recceiverid):
        return self.client.findByArgs(
            prepareFindArgs(self.conf, [("pvStatus", "Active"), (RECCEIVERID_KEY, recceiverid)])
        )

    def clean_channels(self, owner, channels):
        new_channels = []
        for cf_channel in channels or []:
            new_channels.append(cf_channel["name"])
        _log.info("Total channels to update: {nChannels}".format(nChannels=len(new_channels)))
        _log.debug(
            'Update "pvStatus" property to "Inactive" for {n_channels} channels'.format(n_channels=len(new_channels))
        )
        self.client.update(
            property=create_inactive_property(owner),
            channelNames=new_channels,
        )


def dict_to_file(dict, iocs, conf):
    filename = conf.get("debug_file_loc", None)
    if filename:
        if os.path.isfile(filename):
            os.remove(filename)
        list = []
        for key in dict:
            list.append([key, iocs[dict[key][-1]]["hostname"], iocs[dict[key][-1]]["iocname"]])

        list.sort(key=itemgetter(0))

        with open(filename, "w+") as f:
            json.dump(list, f)


def create_channel(name: str, owner: str, properties: list[dict[str, str]]):
    return {
        "name": name,
        "owner": owner,
        "properties": properties,
    }


def create_property(owner: str, name: str, value: str):
    return {
        "name": name,
        "owner": owner,
        "value": value,
    }


def create_recordType_property(owner: str, recordType: str):
    return create_property(owner, "recordType", recordType)


def create_alias_property(owner: str, alias: str):
    return create_property(owner, "alias", alias)


def create_pvStatus_property(owner: str, pvStatus: str):
    return create_property(owner, "pvStatus", pvStatus)


def create_active_property(owner: str):
    return create_pvStatus_property(owner, "Active")


def create_inactive_property(owner: str):
    return create_pvStatus_property(owner, "Inactive")


def create_time_property(owner: str, time: str):
    return create_property(owner, "time", time)


def __updateCF__(
    processor,
    recordInfoByName,
    records_to_delete,
    hostName,
    iocName,
    iocIP,
    iocid,
    owner,
    iocTime,
):
    _log.info("CF Update IOC: {iocid}".format(iocid=iocid))

    # Consider making this function a class methed then 'processor' simply becomes 'self'
    client = processor.client
    channels_dict = processor.channel_dict
    iocs = processor.iocs
    conf = processor.conf
    recceiverid = conf.get(RECCEIVERID_KEY, RECCEIVERID_DEFAULT)
    new_channels = set(recordInfoByName.keys())

    if iocid in iocs:
        hostName = iocs[iocid]["hostname"]
        iocName = iocs[iocid]["iocname"]
        owner = iocs[iocid]["owner"]
        iocTime = iocs[iocid]["time"]
        iocIP = iocs[iocid]["iocIP"]
    else:
        _log.warning("IOC Env Info not found: {iocid}".format(iocid=iocid))

    if hostName is None or iocName is None:
        raise Exception("missing hostName or iocName")

    if processor.cancelled:
        raise defer.CancelledError()

    channels = []
    """A list of channels in channelfinder with the associated hostName and iocName"""
    _log.debug("Find existing channels by IOCID: {iocid}".format(iocid=iocid))
    old_channels = client.findByArgs(prepareFindArgs(conf, [("iocid", iocid)]))
    if processor.cancelled:
        raise defer.CancelledError()

    if old_channels is not None:
        for cf_channel in old_channels:
            if (
                len(new_channels) == 0 or cf_channel["name"] in records_to_delete
            ):  # case: empty commit/del, remove all reference to ioc
                if cf_channel["name"] in channels_dict:
                    cf_channel["owner"] = iocs[channels_dict[cf_channel["name"]][-1]]["owner"]
                    cf_channel["properties"] = __merge_property_lists(
                        create_default_properties(owner, iocTime, recceiverid, channels_dict, iocs, cf_channel),
                        cf_channel,
                    )
                    if conf.get("recordType"):
                        cf_channel["properties"] = __merge_property_lists(
                            cf_channel["properties"].append(
                                create_recordType_property(
                                    owner, iocs[channels_dict[cf_channel["name"]][-1]]["recordType"]
                                )
                            ),
                            cf_channel,
                        )
                    channels.append(cf_channel)
                    _log.debug("Add existing channel to previous IOC: {s}".format(s=channels[-1]))
                    """In case alias exist, also delete them"""
                    if conf.get("alias"):
                        if cf_channel["name"] in recordInfoByName and "aliases" in recordInfoByName[cf_channel["name"]]:
                            for alias in recordInfoByName[cf_channel["name"]]["aliases"]:
                                if alias["name"] in channels_dict:
                                    alias["owner"] = iocs[channels_dict[alias["name"]][-1]]["owner"]
                                    alias["properties"] = __merge_property_lists(
                                        create_default_properties(
                                            owner,
                                            iocTime,
                                            recceiverid,
                                            channels_dict,
                                            iocs,
                                            cf_channel,
                                        ),
                                        alias,
                                    )
                                    if conf.get("recordType", "default") == "on":
                                        cf_channel["properties"] = __merge_property_lists(
                                            cf_channel["properties"].append(
                                                create_recordType_property(
                                                    owner,
                                                    iocs[channels_dict[alias["name"]][-1]]["recordType"],
                                                )
                                            ),
                                            cf_channel,
                                        )
                                    channels.append(alias)
                                    _log.debug("Add existing alias to previous IOC: {s}".format(s=channels[-1]))

                else:
                    """Orphan the channel : mark as inactive, keep the old hostName and iocName"""
                    cf_channel["properties"] = __merge_property_lists(
                        [
                            create_inactive_property(owner),
                            create_time_property(owner, iocTime),
                        ],
                        cf_channel,
                    )
                    channels.append(cf_channel)
                    _log.debug("Add orphaned channel with no IOC: {s}".format(s=channels[-1]))
                    """Also orphan any alias"""
                    if conf.get("alias", "default") == "on":
                        if cf_channel["name"] in recordInfoByName and "aliases" in recordInfoByName[cf_channel["name"]]:
                            for alias in recordInfoByName[cf_channel["name"]]["aliases"]:
                                alias["properties"] = __merge_property_lists(
                                    [
                                        create_inactive_property(owner),
                                        create_time_property(owner, iocTime),
                                    ],
                                    alias,
                                )
                                channels.append(alias)
                                _log.debug("Add orphaned alias with no IOC: {s}".format(s=channels[-1]))
            else:
                if cf_channel["name"] in new_channels:  # case: channel in old and new
                    """
                    Channel exists in Channelfinder with same hostname and iocname.
                    Update the status to ensure it is marked active and update the time.
                    """
                    cf_channel["properties"] = __merge_property_lists(
                        [
                            create_active_property(owner),
                            create_time_property(owner, iocTime),
                        ],
                        cf_channel,
                    )
                    channels.append(cf_channel)
                    _log.debug("Add existing channel with same IOC: {s}".format(s=channels[-1]))
                    new_channels.remove(cf_channel["name"])

                    """In case, alias exist"""
                    if conf.get("alias", "default") == "on":
                        if cf_channel["name"] in recordInfoByName and "aliases" in recordInfoByName[cf_channel["name"]]:
                            for alias in recordInfoByName[cf_channel["name"]]["aliases"]:
                                if alias in old_channels:
                                    """alias exists in old list"""
                                    alias["properties"] = __merge_property_lists(
                                        [
                                            create_active_property(owner),
                                            create_time_property(owner, iocTime),
                                        ],
                                        alias,
                                    )
                                    channels.append(alias)
                                    new_channels.remove(alias["name"])
                                else:
                                    """alias exists but not part of old list"""
                                    aprops = __merge_property_lists(
                                        [
                                            create_active_property(owner),
                                            create_time_property(owner, iocTime),
                                            create_alias_property(
                                                owner,
                                                cf_channel["name"],
                                            ),
                                        ],
                                        cf_channel,
                                    )
                                    channels.append(
                                        create_channel(
                                            alias["name"],
                                            owner,
                                            aprops,
                                        )
                                    )
                                    new_channels.remove(alias["name"])
                                _log.debug("Add existing alias with same IOC: {s}".format(s=channels[-1]))
    # now pvNames contains a list of pv's new on this host/ioc
    """A dictionary representing the current channelfinder information associated with the pvNames"""
    existingChannels = {}

    """
    The list of pv's is searched keeping in mind the limitations on the URL length
    The search is split into groups to ensure that the size does not exceed 600 characters
    """
    searchStrings = []
    searchString = ""
    for channel_name in new_channels:
        if not searchString:
            searchString = channel_name
        elif len(searchString) + len(channel_name) < 600:
            searchString = searchString + "|" + channel_name
        else:
            searchStrings.append(searchString)
            searchString = channel_name
    if searchString:
        searchStrings.append(searchString)

    for eachSearchString in searchStrings:
        _log.debug("Find existing channels by name: {search}".format(search=eachSearchString))
        for cf_channel in client.findByArgs(prepareFindArgs(conf, [("~name", eachSearchString)])):
            existingChannels[cf_channel["name"]] = cf_channel
        if processor.cancelled:
            raise defer.CancelledError()

    for channel_name in new_channels:
        newProps = create_properties(owner, iocTime, recceiverid, hostName, iocName, iocIP, iocid)
        if conf.get("recordType", "default") == "on":
            newProps.append(create_recordType_property(owner, recordInfoByName[channel_name]["recordType"]))
        if channel_name in recordInfoByName and "infoProperties" in recordInfoByName[channel_name]:
            newProps = newProps + recordInfoByName[channel_name]["infoProperties"]

        if channel_name in existingChannels:
            """update existing channel: exists but with a different hostName and/or iocName"""
            existingChannel = existingChannels[channel_name]
            existingChannel["properties"] = __merge_property_lists(newProps, existingChannel)
            channels.append(existingChannel)
            _log.debug("Add existing channel with different IOC: {s}".format(s=channels[-1]))
            """in case, alias exists, update their properties too"""
            if conf.get("alias", "default") == "on":
                if channel_name in recordInfoByName and "aliases" in recordInfoByName[channel_name]:
                    alProps = [create_alias_property(owner, channel_name)]
                    for p in newProps:
                        alProps.append(p)
                    for alias in recordInfoByName[channel_name]["aliases"]:
                        if alias in existingChannels:
                            ach = existingChannels[alias]
                            ach["properties"] = __merge_property_lists(alProps, ach)
                            channels.append(ach)
                        else:
                            channels.append(create_channel(alias, owner, alProps))
                        _log.debug("Add existing alias with different IOC: {s}".format(s=channels[-1]))

        else:
            """New channel"""
            channels.append({"name": channel_name, "owner": owner, "properties": newProps})
            _log.debug("Add new channel: {s}".format(s=channels[-1]))
            if conf.get("alias", "default") == "on":
                if channel_name in recordInfoByName and "aliases" in recordInfoByName[channel_name]:
                    alProps = [create_alias_property(owner, channel_name)]
                    for p in newProps:
                        alProps.append(p)
                    for alias in recordInfoByName[channel_name]["aliases"]:
                        channels.append({"name": alias, "owner": owner, "properties": alProps})
                        _log.debug("Add new alias: {s}".format(s=channels[-1]))
    _log.info("Total channels to update: {nChannels} {iocName}".format(nChannels=len(channels), iocName=iocName))
    if len(channels) != 0:
        client.set(channels=channels)
    else:
        if old_channels and len(old_channels) != 0:
            client.set(channels=channels)
    if processor.cancelled:
        raise defer.CancelledError()


def create_properties(owner, iocTime, recceiverid, hostName, iocName, iocIP, iocid):
    return [
        create_property(owner, "hostName", hostName),
        create_property(owner, "iocName", iocName),
        create_property(owner, "iocid", iocid),
        create_property(owner, "iocIP", iocIP),
        create_active_property(owner),
        create_time_property(owner, iocTime),
        create_property(owner, RECCEIVERID_KEY, recceiverid),
    ]


def create_default_properties(owner, iocTime, recceiverid, channels_dict, iocs, cf_channel):
    return create_properties(
        owner,
        iocTime,
        recceiverid,
        iocs[channels_dict[cf_channel["name"]][-1]]["hostname"],
        iocs[channels_dict[cf_channel["name"]][-1]]["iocname"],
        iocs[channels_dict[cf_channel["name"]][-1]]["iocIP"],
        channels_dict[cf_channel["name"]][-1],
    )


def __merge_property_lists(newProperties: list[dict[str, str]], channel: dict[str, list[dict[str, str]]]):
    """
    Merges two lists of properties ensuring that there are no 2 properties with
    the same name In case of overlap between the new and old property lists the
    new property list wins out
    """
    newPropNames = [p["name"] for p in newProperties]
    for oldProperty in channel["properties"]:
        if oldProperty["name"] not in newPropNames:
            newProperties = newProperties + [oldProperty]
    return newProperties


def getCurrentTime(timezone=False):
    if timezone:
        return str(datetime.datetime.now().astimezone())
    return str(datetime.datetime.now())


def prepareFindArgs(conf, args, size=0):
    size_limit = int(conf.get("findSizeLimit", size))
    if size_limit > 0:
        args.append(("~size", size_limit))
    return args


def poll(
    update_method,
    processor,
    recordInfoByName,
    records_to_delete,
    hostName,
    iocName,
    iocIP,
    iocid,
    owner,
    iocTime,
):
    _log.info("Polling {iocName} begins...".format(iocName=iocName))
    sleep = 1
    success = False
    while not success:
        try:
            update_method(
                processor,
                recordInfoByName,
                records_to_delete,
                hostName,
                iocName,
                iocIP,
                iocid,
                owner,
                iocTime,
            )
            success = True
            return success
        except RequestException as e:
            _log.error("ChannelFinder update failed: {s}".format(s=e))
            retry_seconds = min(60, sleep)
            _log.info("ChannelFinder update retry in {retry_seconds} seconds".format(retry_seconds=retry_seconds))
            time.sleep(retry_seconds)
            sleep *= 1.5
    _log.info("Polling {iocName} complete".format(iocName=iocName))
