# -*- coding: utf-8 -*-

import datetime
import logging
import socket
import time
from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, List, Optional, Set

from channelfinder import ChannelFinderClient
from requests import ConnectionError, RequestException
from twisted.application import service
from twisted.internet import defer
from twisted.internet.defer import DeferredLock
from twisted.internet.threads import deferToThread
from zope.interface import implementer

from . import interfaces
from .interfaces import CommitTransaction
from .processors import ConfigAdapter

_log = logging.getLogger(__name__)

__all__ = ["CFProcessor"]

RECCEIVERID_KEY = "recceiverID"
RECCEIVERID_DEFAULT = socket.gethostname()


@dataclass
class CFConfig:
    alias_enabled: bool = False
    record_type_enabled: bool = False
    environment_variables: str = ""
    info_tags: str = ""
    ioc_connection_info: bool = True
    record_description_enabled: bool = False
    clean_on_start: bool = True
    clean_on_stop: bool = True
    username: str = "cfstore"
    recceiver_id: str = RECCEIVERID_DEFAULT
    timezone: str = ""
    cf_query_limit: int = 10000

    @staticmethod
    def from_config_adapter(conf: ConfigAdapter) -> "CFConfig":
        return CFConfig(
            alias_enabled=conf.get("alias", False),
            record_type_enabled=conf.get("recordType", False),
            environment_variables=conf.get("environment_vars", ""),
            info_tags=conf.get("infotags", ""),
            ioc_connection_info=conf.get("iocConnectionInfo", True),
            record_description_enabled=conf.get("recordDesc", False),
            clean_on_start=conf.get("cleanOnStart", True),
            clean_on_stop=conf.get("cleanOnStop", True),
            username=conf.get("username", "cfstore"),
            recceiver_id=conf.get("recceiverId", RECCEIVERID_DEFAULT),
            timezone=conf.get("timezone", ""),
            cf_query_limit=conf.get("findSizeLimit", 10000),
        )


@dataclass
class CFProperty:
    name: str
    owner: str
    value: Optional[str] = None

    def as_dict(self) -> Dict[str, str]:
        return {"name": self.name, "owner": self.owner, "value": str(self.value)}

    @staticmethod
    def from_channelfinder_dict(prop_dict: Dict[str, str]) -> "CFProperty":
        return CFProperty(
            name=prop_dict.get("name", ""),
            owner=prop_dict.get("owner", ""),
            value=prop_dict.get("value"),
        )


@implementer(interfaces.IProcessor)
class CFProcessor(service.Service):
    def __init__(self, name, conf):
        _log.info("CF_INIT {name}".format(name=name))
        self.name = name
        self.channel_dict = defaultdict(list)
        self.iocs = dict()
        self.client = None
        self.currentTime = getCurrentTime
        self.lock = DeferredLock()
        self.cf_config = CFConfig.from_config_adapter(conf)

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
                cf_properties = {cf_property["name"] for cf_property in self.client.getAllProperties()}
                required_properties = {
                    "hostName",
                    "iocName",
                    "pvStatus",
                    "time",
                    "iocid",
                    "iocIP",
                    RECCEIVERID_KEY,
                }

                if self.cf_config.alias_enabled:
                    required_properties.add("alias")
                if self.cf_config.record_type_enabled:
                    required_properties.add("recordType")
                env_vars_setting = self.cf_config.environment_variables
                self.env_vars = {}
                if env_vars_setting != "" and env_vars_setting is not None:
                    env_vars_dict = dict(item.strip().split(":") for item in env_vars_setting.split(","))
                    self.env_vars = {k.strip(): v.strip() for k, v in env_vars_dict.items()}
                    for epics_env_var_name, cf_prop_name in self.env_vars.items():
                        required_properties.add(cf_prop_name)
                # Standard property names for CA/PVA name server connections. These are
                # environment variables from reccaster so take advantage of env_vars
                # iocConnectionInfo enabled by default
                if self.cf_config.ioc_connection_info:
                    self.env_vars["RSRV_SERVER_PORT"] = "caPort"
                    self.env_vars["PVAS_SERVER_PORT"] = "pvaPort"
                    required_properties.add("caPort")
                    required_properties.add("pvaPort")

                record_property_names_list = [s.strip(", ") for s in self.cf_config.info_tags.split()]
                if self.cf_config.record_description_enabled:
                    record_property_names_list.append("recordDesc")
                # Are any required properties not already present on CF?
                properties = required_properties - cf_properties
                # Are any whitelisted properties not already present on CF?
                # If so, add them too.
                properties.update(set(record_property_names_list) - cf_properties)

                owner = self.cf_config.username
                for cf_property_name in properties:
                    self.client.set(property={"name": cf_property_name, "owner": owner})

                self.record_property_names_list = set(record_property_names_list)
                self.managed_properties = required_properties.union(record_property_names_list)
                _log.debug("record_property_names_list = {}".format(self.record_property_names_list))
            except ConnectionError:
                _log.exception("Cannot connect to Channelfinder service")
                raise
            else:
                if self.cf_config.clean_on_start:
                    self.clean_service()

    def stopService(self):
        _log.info("CF_STOP")
        service.Service.stopService(self)
        return self.lock.run(self._stopServiceWithLock)

    def _stopServiceWithLock(self):
        # Set channels to inactive and close connection to client
        if self.cf_config.clean_on_stop:
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

    def _commitWithThread(self, transaction: CommitTransaction):
        if not self.running:
            raise defer.CancelledError(
                "CF Processor is not running (transaction: {host}:{port})",
                host=transaction.source_address.host,
                port=transaction.source_address.port,
            )

        _log.info("CF_COMMIT: {transaction}".format(transaction=transaction))
        _log.debug("CF_COMMIT: transaction: {s}".format(s=repr(transaction)))
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
            or self.cf_config.username
        )
        time = self.currentTime(timezone=self.cf_config.timezone)

        """The unique identifier for a particular IOC"""
        iocid = host + ":" + str(port)
        _log.debug("transaction: {s}".format(s=repr(transaction)))

        recordInfo = {}
        for record_id, (record_name, record_type) in transaction.records_to_add.items():
            recordInfo[record_id] = {"pvName": record_name}
            if self.cf_config.record_type_enabled:
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
                        CFProperty(infotag, owner, record_infos_to_add[infotag])
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
                        CFProperty(cf_prop_name, owner, transaction.client_infos.get(epics_env_var_name))
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
            if self.cf_config.alias_enabled:
                if record_name in recordInfoByName and "aliases" in recordInfoByName[record_name]:
                    for alias in recordInfoByName[record_name]["aliases"]:
                        self.channel_dict[alias].append(iocid)  # add iocname to pvName in dict
                        self.iocs[iocid]["channelcount"] += 1
        for record_name in records_to_delete:
            if iocid in self.channel_dict[record_name]:
                self.remove_channel(record_name, iocid)
                """In case, alias exists"""
                if self.cf_config.alias_enabled:
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
        owner = self.cf_config.username
        recceiverid = self.cf_config.recceiver_id
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
            prepareFindArgs(self.cf_config, [("pvStatus", "Active"), (RECCEIVERID_KEY, recceiverid)])
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
            property=create_inactive_property(owner).as_dict(),
            channelNames=new_channels,
        )


def create_channel(name: str, owner: str, properties: List[CFProperty]):
    return {
        "name": name,
        "owner": owner,
        "properties": properties,
    }


def create_recordType_property(owner: str, recordType: str) -> CFProperty:
    return CFProperty("recordType", owner, recordType)


def create_alias_property(owner: str, alias: str) -> CFProperty:
    return CFProperty("alias", owner, alias)


def create_pvStatus_property(owner: str, pvStatus: str) -> CFProperty:
    return CFProperty("pvStatus", owner, pvStatus)


def create_active_property(owner: str) -> CFProperty:
    return create_pvStatus_property(owner, "Active")


def create_inactive_property(owner: str) -> CFProperty:
    return create_pvStatus_property(owner, "Inactive")


def create_time_property(owner: str, time: str) -> CFProperty:
    return CFProperty("time", owner, time)


def __updateCF__(
    processor: CFProcessor,
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
    _log.debug(
        "CF Update IOC: {iocid} recordInfoByName {recordInfoByName}".format(
            iocid=iocid, recordInfoByName=recordInfoByName
        )
    )
    # Consider making this function a class methed then 'processor' simply becomes 'self'
    client = processor.client
    channels_dict = processor.channel_dict
    iocs = processor.iocs
    cf_config = processor.cf_config
    recceiverid = processor.cf_config.recceiver_id
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
    old_channels = client.findByArgs(prepareFindArgs(cf_config, [("iocid", iocid)]))
    old_channels = [
        {
            "name": ch["name"],
            "owner": ch["owner"],
            "properties": [CFProperty.from_channelfinder_dict(prop) for prop in ch["properties"]],
        }
        for ch in old_channels
    ]

    if processor.cancelled:
        raise defer.CancelledError()

    if old_channels is not None:
        for cf_channel in old_channels:
            if (
                len(new_channels) == 0 or cf_channel["name"] in records_to_delete
            ):  # case: empty commit/del, remove all reference to ioc
                _log.debug("Channel {s} exists in Channelfinder not in new_channels".format(s=cf_channel["name"]))
                if cf_channel["name"] in channels_dict:
                    cf_channel["owner"] = iocs[channels_dict[cf_channel["name"]][-1]]["owner"]
                    cf_channel["properties"] = __merge_property_lists(
                        create_default_properties(owner, iocTime, recceiverid, channels_dict, iocs, cf_channel),
                        cf_channel,
                        processor.managed_properties,
                    )
                    if cf_config.record_type_enabled:
                        cf_channel["properties"] = __merge_property_lists(
                            cf_channel["properties"].append(
                                create_recordType_property(
                                    owner, iocs[channels_dict[cf_channel["name"]][-1]]["recordType"]
                                )
                            ),
                            cf_channel,
                            processor.managed_properties,
                        )
                    channels.append(cf_channel)
                    _log.debug("Add existing channel to previous IOC: {s}".format(s=channels[-1]))
                    """In case alias exist, also delete them"""
                    if cf_config.alias_enabled:
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
                                        processor.managed_properties,
                                    )
                                    if cf_config.record_type_enabled:
                                        cf_channel["properties"] = __merge_property_lists(
                                            cf_channel["properties"].append(
                                                create_recordType_property(
                                                    owner,
                                                    iocs[channels_dict[alias["name"]][-1]]["recordType"],
                                                )
                                            ),
                                            cf_channel,
                                            processor.managed_properties,
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
                    if cf_config.alias_enabled:
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
                    _log.debug(
                        "Channel {s} exists in Channelfinder with same hostname and iocname".format(
                            s=cf_channel["name"]
                        )
                    )
                    cf_channel["properties"] = __merge_property_lists(
                        [
                            create_active_property(owner),
                            create_time_property(owner, iocTime),
                        ],
                        cf_channel,
                        processor.managed_properties,
                    )
                    channels.append(cf_channel)
                    _log.debug("Add existing channel with same IOC: {s}".format(s=channels[-1]))
                    new_channels.remove(cf_channel["name"])

                    """In case, alias exist"""
                    if cf_config.alias_enabled:
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
                                        processor.managed_properties,
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
                                        processor.managed_properties,
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
        for cf_channel in client.findByArgs(prepareFindArgs(cf_config, [("~name", eachSearchString)])):
            existingChannels[cf_channel["name"]] = {
                "name": cf_channel["name"],
                "owner": cf_channel["owner"],
                "properties": [CFProperty.from_channelfinder_dict(prop) for prop in cf_channel["properties"]],
            }
        if processor.cancelled:
            raise defer.CancelledError()

    for channel_name in new_channels:
        newProps = create_properties(owner, iocTime, recceiverid, hostName, iocName, iocIP, iocid)
        if cf_config.record_type_enabled:
            newProps.append(create_recordType_property(owner, recordInfoByName[channel_name]["recordType"]))
        if channel_name in recordInfoByName and "infoProperties" in recordInfoByName[channel_name]:
            newProps = newProps + recordInfoByName[channel_name]["infoProperties"]

        if channel_name in existingChannels:
            _log.debug(
                f"""update existing channel{channel_name}: exists but with a different hostName and/or iocName"""
            )

            existingChannel = existingChannels[channel_name]
            existingChannel["properties"] = __merge_property_lists(
                newProps,
                existingChannel,
                processor.managed_properties,
            )
            channels.append(existingChannel)
            _log.debug("Add existing channel with different IOC: {s}".format(s=channels[-1]))
            """in case, alias exists, update their properties too"""
            if cf_config.alias_enabled:
                if channel_name in recordInfoByName and "aliases" in recordInfoByName[channel_name]:
                    alProps = [create_alias_property(owner, channel_name)]
                    for p in newProps:
                        alProps.append(p)
                    for alias in recordInfoByName[channel_name]["aliases"]:
                        if alias in existingChannels:
                            ach = existingChannels[alias]
                            ach["properties"] = __merge_property_lists(
                                alProps,
                                ach,
                                processor.managed_properties,
                            )
                            channels.append(ach)
                        else:
                            channels.append(create_channel(alias, owner, alProps))
                        _log.debug("Add existing alias with different IOC: {s}".format(s=channels[-1]))

        else:
            """New channel"""
            channels.append({"name": channel_name, "owner": owner, "properties": newProps})
            _log.debug("Add new channel: {s}".format(s=channels[-1]))
            if cf_config.alias_enabled:
                if channel_name in recordInfoByName and "aliases" in recordInfoByName[channel_name]:
                    alProps = [create_alias_property(owner, channel_name)]
                    for p in newProps:
                        alProps.append(p)
                    for alias in recordInfoByName[channel_name]["aliases"]:
                        channels.append({"name": alias, "owner": owner, "properties": alProps})
                        _log.debug("Add new alias: {s}".format(s=channels[-1]))
    _log.info("Total channels to update: {nChannels} {iocName}".format(nChannels=len(channels), iocName=iocName))
    if len(channels) != 0:
        cf_set_chunked(client, channels, cf_config.cf_query_limit)
    else:
        if old_channels and len(old_channels) != 0:
            cf_set_chunked(client, channels, cf_config.cf_query_limit)
    if processor.cancelled:
        raise defer.CancelledError()


def cf_set_chunked(client, channels, chunk_size=10000):
    for i in range(0, len(channels), chunk_size):
        chunk = [
            {"name": ch["name"], "owner": ch["owner"], "properties": [prop.as_dict() for prop in ch["properties"]]}
            for ch in channels[i : i + chunk_size]
        ]
        _log.debug("Updating chunk %s", chunk)
        client.set(channels=chunk)


def create_properties(owner, iocTime, recceiverid, hostName, iocName, iocIP, iocid):
    return [
        CFProperty("hostName", owner, hostName),
        CFProperty("iocName", owner, iocName),
        CFProperty("iocid", owner, iocid),
        CFProperty("iocIP", owner, iocIP),
        create_active_property(owner),
        create_time_property(owner, iocTime),
        CFProperty(RECCEIVERID_KEY, owner, recceiverid),
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


def __merge_property_lists(
    newProperties: List[CFProperty], channel: Dict[str, List[CFProperty]], managed_properties: Set[str] = set()
) -> List[CFProperty]:
    """
    Merges two lists of properties ensuring that there are no 2 properties with
    the same name In case of overlap between the new and old property lists the
    new property list wins out
    """
    newPropNames = [p.name for p in newProperties]
    for oldProperty in channel["properties"]:
        if oldProperty.name not in newPropNames and (oldProperty.name not in managed_properties):
            newProperties = newProperties + [oldProperty]
    return newProperties


def getCurrentTime(timezone=False):
    if timezone:
        return str(datetime.datetime.now().astimezone())
    return str(datetime.datetime.now())


def prepareFindArgs(cf_config: CFConfig, args, size=0):
    size_limit = int(cf_config.cf_query_limit)
    if size_limit > 0:
        args.append(("~size", size_limit))
    return args


def poll(
    update_method,
    processor: CFProcessor,
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
