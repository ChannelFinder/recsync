# -*- coding: utf-8 -*-

import datetime
import enum
import logging
import socket
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set

from channelfinder import ChannelFinderClient
from requests import ConnectionError, RequestException
from zope.interface import implementer

from twisted.application import service
from twisted.internet import defer
from twisted.internet.defer import DeferredLock
from twisted.internet.threads import deferToThread

from . import interfaces
from .interfaces import CommitTransaction
from .processors import ConfigAdapter

_log = logging.getLogger(__name__)

__all__ = ["CFProcessor"]

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

    @staticmethod
    def recordType(owner: str, recordType: str) -> "CFProperty":
        """Create a Channelfinder recordType property.

        Args:
            owner: The owner of the property.
            recordType: The recordType of the property.
        """
        return CFProperty(CFPropertyName.recordType.name, owner, recordType)

    @staticmethod
    def alias(owner: str, alias: str) -> "CFProperty":
        """Create a Channelfinder alias property.

        Args:
            owner: The owner of the property.
            alias: The alias of the property.
        """
        return CFProperty(CFPropertyName.alias.name, owner, alias)

    @staticmethod
    def pvStatus(owner: str, pvStatus: str) -> "CFProperty":
        """Create a Channelfinder pvStatus property.

        Args:
            owner: The owner of the property.
            pvStatus: The pvStatus of the property.
        """
        return CFProperty(CFPropertyName.pvStatus.name, owner, pvStatus)

    @staticmethod
    def active(owner: str) -> "CFProperty":
        """Create a Channelfinder active property.

        Args:
            owner: The owner of the property.
        """
        return CFProperty.pvStatus(owner, PVStatus.Active.name)

    @staticmethod
    def inactive(owner: str) -> "CFProperty":
        """Create a Channelfinder inactive property.

        Args:
            owner: The owner of the property.
        """
        return CFProperty.pvStatus(owner, PVStatus.Inactive.name)

    @staticmethod
    def time(owner: str, time: str) -> "CFProperty":
        """Create a Channelfinder time property.

        Args:
            owner: The owner of the property.
            time: The time of the property.
        """
        return CFProperty(CFPropertyName.time.name, owner, time)


@dataclass
class RecordInfo:
    pvName: str
    recordType: Optional[str] = None
    infoProperties: List[CFProperty] = field(default_factory=list)
    aliases: List[str] = field(default_factory=list)


class CFPropertyName(enum.Enum):
    hostName = enum.auto()
    iocName = enum.auto()
    iocid = enum.auto()
    iocIP = enum.auto()
    pvStatus = enum.auto()
    time = enum.auto()
    recceiverID = enum.auto()
    alias = enum.auto()
    recordType = enum.auto()
    recordDesc = enum.auto()
    caPort = enum.auto()
    pvaPort = enum.auto()


class PVStatus(enum.Enum):
    Active = enum.auto()
    Inactive = enum.auto()


@dataclass
class IocInfo:
    host: str
    hostname: str
    ioc_name: str
    ioc_IP: str
    owner: str
    time: str
    channelcount: int
    port: int

    @property
    def ioc_id(self):
        return self.host + ":" + str(self.port)


@dataclass
class CFChannel:
    name: str
    owner: str
    properties: List[CFProperty]

    def as_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "owner": self.owner,
            "properties": [p.as_dict() for p in self.properties],
        }

    def from_channelfinder_dict(channel_dict: Dict[str, Any]) -> "CFChannel":
        return CFChannel(
            name=channel_dict.get("name", ""),
            owner=channel_dict.get("owner", ""),
            properties=[CFProperty.from_channelfinder_dict(p) for p in channel_dict.get("properties", [])],
        )


@implementer(interfaces.IProcessor)
class CFProcessor(service.Service):
    def __init__(self, name, conf):
        self.cf_config = CFConfig.from_config_adapter(conf)
        _log.info("CF_INIT %s", self.cf_config)
        self.name = name
        self.channel_ioc_ids = defaultdict(list)
        self.iocs: Dict[str, IocInfo] = dict()
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
                cf_properties = {cf_property["name"] for cf_property in self.client.getAllProperties()}
                required_properties = {
                    CFPropertyName.hostName.name,
                    CFPropertyName.iocName.name,
                    CFPropertyName.iocid.name,
                    CFPropertyName.iocIP.name,
                    CFPropertyName.pvStatus.name,
                    CFPropertyName.time.name,
                    CFPropertyName.recceiverID.name,
                }

                if self.cf_config.alias_enabled:
                    required_properties.add(CFPropertyName.alias.name)
                if self.cf_config.record_type_enabled:
                    required_properties.add(CFPropertyName.recordType.name)
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
                    required_properties.add(CFPropertyName.caPort.name)
                    required_properties.add(CFPropertyName.pvaPort.name)

                record_property_names_list = [s.strip(", ") for s in self.cf_config.info_tags.split()]
                if self.cf_config.record_description_enabled:
                    record_property_names_list.append(CFPropertyName.recordDesc.name)
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
                _log.debug("record_property_names_list = %s", self.record_property_names_list)
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

        def chainResult(result):
            if self.cancelled:
                raise defer.CancelledError(f"CF Processor is cancelled, due to {result}")
            else:
                d.callback(None)

        t.addCallbacks(chainResult, chainError)
        return d

    def transaction_to_recordInfo(self, ioc_info: IocInfo, transaction: CommitTransaction) -> Dict[str, RecordInfo]:
        recordInfo: Dict[str, RecordInfo] = {}
        for record_id, (record_name, record_type) in transaction.records_to_add.items():
            recordInfo[record_id] = RecordInfo(pvName=record_name, recordType=None, infoProperties=[], aliases=[])
            if self.cf_config.record_type_enabled:
                recordInfo[record_id].recordType = record_type

        for record_id, (record_infos_to_add) in transaction.record_infos_to_add.items():
            # find intersection of these sets
            if record_id not in recordInfo:
                _log.warning("IOC: %s: PV not found for recinfo with RID: {record_id}", ioc_info, record_id)
                continue
            recinfo_wl = [p for p in self.record_property_names_list if p in record_infos_to_add.keys()]
            if recinfo_wl:
                for infotag in recinfo_wl:
                    recordInfo[record_id].infoProperties.append(
                        CFProperty(infotag, ioc_info.owner, record_infos_to_add[infotag])
                    )

        for record_id, record_aliases in transaction.aliases.items():
            if record_id not in recordInfo:
                _log.warning("IOC: %s: PV not found for alias with RID: %s", ioc_info, record_id)
                continue
            recordInfo[record_id].aliases = record_aliases

        for record_id in recordInfo:
            for epics_env_var_name, cf_prop_name in self.env_vars.items():
                if transaction.client_infos.get(epics_env_var_name) is not None:
                    recordInfo[record_id].infoProperties.append(
                        CFProperty(cf_prop_name, ioc_info.owner, transaction.client_infos.get(epics_env_var_name))
                    )
                else:
                    _log.debug(
                        "EPICS environment var %s listed in environment_vars setting list not found in this IOC: %s",
                        epics_env_var_name,
                        ioc_info,
                    )
        return recordInfo

    def record_info_by_name(self, recordInfo, ioc_info) -> Dict[str, RecordInfo]:
        recordInfoByName = {}
        for record_id, (info) in recordInfo.items():
            if info.pvName in recordInfoByName:
                _log.warning("Commit contains multiple records with PV name: %s (%s)", info.pvName, ioc_info)
                continue
            recordInfoByName[info.pvName] = info
        return recordInfoByName

    def update_ioc_infos(
        self,
        transaction: CommitTransaction,
        ioc_info: IocInfo,
        records_to_delete: List[str],
        recordInfoByName: Dict[str, RecordInfo],
    ):
        iocid = ioc_info.ioc_id
        if transaction.initial:
            """Add IOC to source list """
            self.iocs[iocid] = ioc_info
        if not transaction.connected:
            records_to_delete.extend(self.channel_ioc_ids.keys())
        for record_name in recordInfoByName.keys():
            self.channel_ioc_ids[record_name].append(iocid)
            self.iocs[iocid].channelcount += 1
            """In case, alias exists"""
            if self.cf_config.alias_enabled:
                if record_name in recordInfoByName:
                    for record_aliases in recordInfoByName[record_name].aliases:
                        self.channel_ioc_ids[record_aliases].append(iocid)  # add iocname to pvName in dict
                        self.iocs[iocid].channelcount += 1
        for record_name in records_to_delete:
            if iocid in self.channel_ioc_ids[record_name]:
                self.remove_channel(record_name, iocid)
                """In case, alias exists"""
                if self.cf_config.alias_enabled:
                    if record_name in recordInfoByName:
                        for record_aliases in recordInfoByName[record_name].aliases:
                            self.remove_channel(record_aliases, iocid)

    def _commitWithThread(self, transaction: CommitTransaction):
        if not self.running:
            host = transaction.source_address.host
            port = transaction.source_address.port
            raise defer.CancelledError(f"CF Processor is not running (transaction: {host}:{port})")

        _log.info("CF_COMMIT: %s", transaction)
        _log.debug("CF_COMMIT: transaction: %s", repr(transaction))

        ioc_info = IocInfo(
            host=transaction.source_address.host,
            hostname=transaction.client_infos.get("HOSTNAME") or transaction.source_address.host,
            ioc_name=transaction.client_infos.get("IOCNAME") or str(transaction.source_address.port),
            ioc_IP=transaction.source_address.host,
            owner=(
                transaction.client_infos.get("ENGINEER")
                or transaction.client_infos.get("CF_USERNAME")
                or self.cf_config.username
            ),
            time=self.currentTime(timezone=self.cf_config.timezone),
            port=transaction.source_address.port,
            channelcount=0,
        )

        recordInfo = self.transaction_to_recordInfo(ioc_info, transaction)

        records_to_delete = list(transaction.records_to_delete)
        _log.debug("Delete records: %s", records_to_delete)

        recordInfoByName = self.record_info_by_name(recordInfo, ioc_info)
        self.update_ioc_infos(transaction, ioc_info, records_to_delete, recordInfoByName)
        poll(__updateCF__, self, recordInfoByName, records_to_delete, ioc_info)

    def remove_channel(self, recordName: str, iocid: str):
        self.channel_ioc_ids[recordName].remove(iocid)
        if iocid in self.iocs:
            self.iocs[iocid].channelcount -= 1
        if self.iocs[iocid].channelcount == 0:
            self.iocs.pop(iocid, None)
        elif self.iocs[iocid].channelcount < 0:
            _log.error("Channel count negative: %s", iocid)
        if len(self.channel_ioc_ids[recordName]) <= 0:  # case: channel has no more iocs
            del self.channel_ioc_ids[recordName]

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
            _log.info("Clean service retry in %s seconds", retry_seconds)
            time.sleep(retry_seconds)
            sleep *= 1.5
            if self.running == 0 and sleep >= retry_limit:
                _log.info("Abandoning clean after %s seconds", retry_limit)
                return

    def get_active_channels(self, recceiverid) -> List[CFChannel]:
        return [
            CFChannel.from_channelfinder_dict(ch)
            for ch in self.client.findByArgs(
                prepareFindArgs(
                    self.cf_config,
                    [
                        (CFPropertyName.pvStatus.name, PVStatus.Active.name),
                        (CFPropertyName.recceiverID.name, recceiverid),
                    ],
                )
            )
        ]

    def clean_channels(self, owner: str, channels: List[CFChannel]):
        new_channels = []
        for cf_channel in channels or []:
            new_channels.append(cf_channel.name)
        _log.info("Cleaning %s channels.", len(new_channels))
        _log.debug('Update "pvStatus" property to "Inactive" for %s channels', len(new_channels))
        self.client.update(
            property=CFProperty.inactive(owner).as_dict(),
            channelNames=new_channels,
        )


def handle_channel_is_old(
    channel_ioc_ids: Dict[str, List[str]],
    cf_channel: CFChannel,
    iocs: Dict[str, IocInfo],
    ioc_info: IocInfo,
    recceiverid: str,
    processor: CFProcessor,
    cf_config: CFConfig,
    channels: List[CFChannel],
    recordInfoByName: Dict[str, RecordInfo],
):
    last_ioc_id = channel_ioc_ids[cf_channel.name][-1]
    cf_channel.owner = iocs[last_ioc_id].owner
    cf_channel.properties = __merge_property_lists(
        create_default_properties(ioc_info, recceiverid, channel_ioc_ids, iocs, cf_channel),
        cf_channel,
        processor.managed_properties,
    )
    if cf_config.record_type_enabled:
        cf_channel.properties = __merge_property_lists(
            cf_channel.properties.append(CFProperty.recordType(ioc_info.owner, iocs[last_ioc_id]["recordType"])),
            cf_channel,
            processor.managed_properties,
        )
    channels.append(cf_channel)
    _log.debug("Add existing channel %s to previous IOC %s", cf_channel, last_ioc_id)
    """In case alias exist, also delete them"""
    if cf_config.alias_enabled:
        if cf_channel.name in recordInfoByName:
            for alias_name in recordInfoByName[cf_channel.name].aliases:
                # TODO Remove? This code couldn't have been working....
                alias_channel = CFChannel(alias_name, "", [])
                if alias_name in channel_ioc_ids:
                    last_alias_ioc_id = channel_ioc_ids[alias_name][-1]
                    alias_channel.owner = iocs[last_alias_ioc_id].owner
                    alias_channel.properties = __merge_property_lists(
                        create_default_properties(
                            ioc_info,
                            recceiverid,
                            channel_ioc_ids,
                            iocs,
                            cf_channel,
                        ),
                        alias_channel,
                        processor.managed_properties,
                    )
                    if cf_config.record_type_enabled:
                        cf_channel.properties = __merge_property_lists(
                            cf_channel.properties.append(
                                CFProperty.recordType(
                                    ioc_info.owner,
                                    iocs[last_alias_ioc_id]["recordType"],
                                )
                            ),
                            cf_channel,
                            processor.managed_properties,
                        )
                    channels.append(alias_channel)
                    _log.debug("Add existing alias %s to previous IOC: %s", alias_channel, last_alias_ioc_id)


def orphan_channel(
    cf_channel: CFChannel,
    ioc_info: IocInfo,
    channels: List[CFChannel],
    cf_config: CFConfig,
    recordInfoByName: Dict[str, RecordInfo],
):
    cf_channel.properties = __merge_property_lists(
        [
            CFProperty.inactive(ioc_info.owner),
            CFProperty.time(ioc_info.owner, ioc_info.time),
        ],
        cf_channel,
    )
    channels.append(cf_channel)
    _log.debug("Add orphaned channel %s with no IOC: %s", cf_channel, ioc_info)
    """Also orphan any alias"""
    if cf_config.alias_enabled:
        if cf_channel.name in recordInfoByName:
            for alias_name in recordInfoByName[cf_channel.name].aliases:
                alias_channel = CFChannel(alias_name, "", [])
                alias_channel.properties = __merge_property_lists(
                    [
                        CFProperty.inactive(ioc_info.owner),
                        CFProperty.time(ioc_info.owner, ioc_info.time),
                    ],
                    alias_channel,
                )
                channels.append(alias_channel)
                _log.debug("Add orphaned alias %s with no IOC: %s", alias_channel, ioc_info)


def handle_channel_old_and_new(
    cf_channel: CFChannel,
    iocid: str,
    ioc_info: IocInfo,
    processor: CFProcessor,
    channels: List[CFChannel],
    new_channels: Set[str],
    cf_config: CFConfig,
    recordInfoByName: Dict[str, RecordInfo],
    old_channels: List[CFChannel],
):
    """
    Channel exists in Channelfinder with same iocid.
    Update the status to ensure it is marked active and update the time.
    """
    _log.debug("Channel %s exists in Channelfinder with same iocid %s", cf_channel.name, iocid)
    cf_channel.properties = __merge_property_lists(
        [
            CFProperty.active(ioc_info.owner),
            CFProperty.time(ioc_info.owner, ioc_info.time),
        ],
        cf_channel,
        processor.managed_properties,
    )
    channels.append(cf_channel)
    _log.debug("Add existing channel with same IOC: %s", cf_channel)
    new_channels.remove(cf_channel.name)

    """In case, alias exist"""
    if cf_config.alias_enabled:
        if cf_channel.name in recordInfoByName:
            for alias_name in recordInfoByName[cf_channel.name].aliases:
                if alias_name in old_channels:
                    """alias exists in old list"""
                    alias_channel = CFChannel(alias_name, "", [])
                    alias_channel.properties = __merge_property_lists(
                        [
                            CFProperty.active(ioc_info.owner),
                            CFProperty.time(ioc_info.owner, ioc_info.time),
                        ],
                        alias_channel,
                        processor.managed_properties,
                    )
                    channels.append(alias_channel)
                    new_channels.remove(alias_name)
                else:
                    """alias exists but not part of old list"""
                    aprops = __merge_property_lists(
                        [
                            CFProperty.active(ioc_info.owner),
                            CFProperty.time(ioc_info.owner, ioc_info.time),
                            CFProperty.alias(
                                ioc_info.owner,
                                cf_channel.name,
                            ),
                        ],
                        cf_channel,
                        processor.managed_properties,
                    )
                    channels.append(
                        CFChannel(
                            alias_name,
                            ioc_info.owner,
                            aprops,
                        )
                    )
                    new_channels.remove(alias_name)
                _log.debug("Add existing alias with same IOC: %s", cf_channel)


def get_existing_channels(
    new_channels: Set[str], client: ChannelFinderClient, cf_config: CFConfig, processor: CFProcessor
) -> Dict[str, CFChannel]:
    """A dictionary representing the current channelfinder information associated with the pvNames"""
    existingChannels: Dict[str, CFChannel] = {}

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
        _log.debug("Find existing channels by name: %s", eachSearchString)
        for found_channel in client.findByArgs(prepareFindArgs(cf_config, [("~name", eachSearchString)])):
            existingChannels[found_channel["name"]] = CFChannel.from_channelfinder_dict(found_channel)
        if processor.cancelled:
            raise defer.CancelledError()
    return existingChannels


def handle_old_channels(
    old_channels: List[CFChannel],
    new_channels: Set[str],
    records_to_delete: List[str],
    channel_ioc_ids: Dict[str, List[str]],
    iocs: Dict[str, IocInfo],
    ioc_info: IocInfo,
    recceiverid: str,
    processor: CFProcessor,
    cf_config: CFConfig,
    channels: List[CFChannel],
    recordInfoByName: Dict[str, RecordInfo],
    iocid: str,
):
    for cf_channel in old_channels:
        if (
            len(new_channels) == 0 or cf_channel.name in records_to_delete
        ):  # case: empty commit/del, remove all reference to ioc
            _log.debug("Channel %s exists in Channelfinder not in new_channels", cf_channel)
            if cf_channel.name in channel_ioc_ids:
                handle_channel_is_old(
                    channel_ioc_ids,
                    cf_channel,
                    iocs,
                    ioc_info,
                    recceiverid,
                    processor,
                    cf_config,
                    channels,
                    recordInfoByName,
                )
            else:
                """Orphan the channel : mark as inactive, keep the old hostName and iocName"""
                orphan_channel(cf_channel, ioc_info, channels, cf_config, recordInfoByName)
        else:
            if cf_channel.name in new_channels:  # case: channel in old and new
                handle_channel_old_and_new(
                    cf_channel,
                    iocid,
                    ioc_info,
                    processor,
                    channels,
                    new_channels,
                    cf_config,
                    recordInfoByName,
                    old_channels,
                )
    return cf_channel


def update_existing_channel_diff_iocid(
    existingChannels: Dict[str, CFChannel],
    channel_name: str,
    newProps: List[CFProperty],
    processor: CFProcessor,
    channels: List[CFChannel],
    cf_config: CFConfig,
    recordInfoByName: Dict[str, RecordInfo],
    ioc_info: IocInfo,
    iocid: str,
):
    existingChannel = existingChannels[channel_name]
    existingChannel.properties = __merge_property_lists(
        newProps,
        existingChannel,
        processor.managed_properties,
    )
    channels.append(existingChannel)
    _log.debug("Add existing channel with different IOC: %s", existingChannel)
    """in case, alias exists, update their properties too"""
    if cf_config.alias_enabled:
        if channel_name in recordInfoByName:
            alProps = [CFProperty.alias(ioc_info.owner, channel_name)]
            for p in newProps:
                alProps.append(p)
            for alias_name in recordInfoByName[channel_name].aliases:
                if alias_name in existingChannels:
                    ach = existingChannels[alias_name]
                    ach.properties = __merge_property_lists(
                        alProps,
                        ach,
                        processor.managed_properties,
                    )
                    channels.append(ach)
                else:
                    channels.append(CFChannel(alias_name, ioc_info.owner, alProps))
                _log.debug("Add existing alias %s of %s with different IOC from %s", alias_name, channel_name, iocid)


def __updateCF__(processor: CFProcessor, recordInfoByName: Dict[str, RecordInfo], records_to_delete, ioc_info: IocInfo):
    _log.info("CF Update IOC: %s", ioc_info)
    _log.debug("CF Update IOC: %s recordInfoByName %s", ioc_info, recordInfoByName)
    # Consider making this function a class methed then 'processor' simply becomes 'self'
    client = processor.client
    channel_ioc_ids = processor.channel_ioc_ids
    iocs = processor.iocs
    cf_config = processor.cf_config
    recceiverid = processor.cf_config.recceiver_id
    new_channels = set(recordInfoByName.keys())
    iocid = ioc_info.ioc_id

    if iocid not in iocs:
        _log.warning("IOC Env Info %s not found in ioc list: %s", ioc_info, iocs)

    if ioc_info.hostname is None or ioc_info.ioc_name is None:
        raise Exception(f"Missing hostName {ioc_info.hostname} or iocName {ioc_info.ioc_name}")

    if processor.cancelled:
        raise defer.CancelledError("Processor cancelled in __updateCF__")

    channels: List[CFChannel] = []
    """A list of channels in channelfinder with the associated hostName and iocName"""
    _log.debug("Find existing channels by IOCID: %s", ioc_info)
    old_channels: List[CFChannel] = [
        CFChannel.from_channelfinder_dict(ch)
        for ch in client.findByArgs(prepareFindArgs(cf_config, [("iocid", iocid)]))
    ]

    if old_channels is not None:
        handle_old_channels(
            old_channels,
            new_channels,
            records_to_delete,
            channel_ioc_ids,
            iocs,
            ioc_info,
            recceiverid,
            processor,
            cf_config,
            channels,
            recordInfoByName,
            iocid,
        )
    # now pvNames contains a list of pv's new on this host/ioc
    existingChannels = get_existing_channels(new_channels, client, cf_config, processor)

    for channel_name in new_channels:
        newProps = create_properties(
            ioc_info.owner,
            ioc_info.time,
            recceiverid,
            ioc_info.hostname,
            ioc_info.ioc_name,
            ioc_info.ioc_IP,
            ioc_info.ioc_id,
        )
        if (
            cf_config.record_type_enabled
            and channel_name in recordInfoByName
            and recordInfoByName[channel_name].recordType
        ):
            newProps.append(CFProperty.recordType(ioc_info.owner, recordInfoByName[channel_name].recordType))
        if channel_name in recordInfoByName:
            newProps = newProps + recordInfoByName[channel_name].infoProperties

        if channel_name in existingChannels:
            _log.debug("update existing channel %s: exists but with a different iocid from %s", channel_name, iocid)
            update_existing_channel_diff_iocid(
                existingChannels,
                channel_name,
                newProps,
                processor,
                channels,
                cf_config,
                recordInfoByName,
                ioc_info,
                iocid,
            )
        else:
            """New channel"""
            channels.append(CFChannel(channel_name, ioc_info.owner, newProps))
            _log.debug("Add new channel: %s", channel_name)
            if cf_config.alias_enabled:
                if channel_name in recordInfoByName:
                    alProps = [CFProperty.alias(ioc_info.owner, channel_name)]
                    for p in newProps:
                        alProps.append(p)
                    for alias in recordInfoByName[channel_name].aliases:
                        channels.append(CFChannel(alias, ioc_info.owner, alProps))
                        _log.debug("Add new alias: %s from %s", alias, channel_name)
    _log.info("Total channels to update: %s for ioc: %s", len(channels), ioc_info)

    if len(channels) != 0:
        cf_set_chunked(client, channels, cf_config.cf_query_limit)
    else:
        if old_channels and len(old_channels) != 0:
            cf_set_chunked(client, channels, cf_config.cf_query_limit)
    if processor.cancelled:
        raise defer.CancelledError()


def cf_set_chunked(client, channels: List[CFChannel], chunk_size=10000):
    for i in range(0, len(channels), chunk_size):
        chunk = [ch.as_dict() for ch in channels[i : i + chunk_size]]
        client.set(channels=chunk)


def create_properties(owner: str, iocTime: str, recceiverid: str, hostName: str, iocName: str, iocIP: str, iocid: str):
    return [
        CFProperty(CFPropertyName.hostName.name, owner, hostName),
        CFProperty(CFPropertyName.iocName.name, owner, iocName),
        CFProperty(CFPropertyName.iocid.name, owner, iocid),
        CFProperty(CFPropertyName.iocIP.name, owner, iocIP),
        CFProperty.active(owner),
        CFProperty.time(owner, iocTime),
        CFProperty(CFPropertyName.recceiverID.name, owner, recceiverid),
    ]


def create_default_properties(
    ioc_info: IocInfo, recceiverid: str, channels_iocs: Dict[str, List[str]], iocs: Dict[str, IocInfo], cf_channel
):
    channel_name = cf_channel.name
    last_ioc_info = iocs[channels_iocs[channel_name][-1]]
    return create_properties(
        ioc_info.owner,
        ioc_info.time,
        recceiverid,
        last_ioc_info.hostname,
        last_ioc_info.ioc_name,
        last_ioc_info.ioc_IP,
        last_ioc_info.ioc_id,
    )


def __merge_property_lists(
    newProperties: List[CFProperty], channel: CFChannel, managed_properties: Set[str] = set()
) -> List[CFProperty]:
    """
    Merges two lists of properties ensuring that there are no 2 properties with
    the same name In case of overlap between the new and old property lists the
    new property list wins out
    """
    newPropNames = [p.name for p in newProperties]
    for oldProperty in channel.properties:
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
    recordInfoByName: Dict[str, RecordInfo],
    records_to_delete,
    ioc_info: IocInfo,
):
    _log.info("Polling for %s begins...", ioc_info)
    sleep = 1.0
    success = False
    while not success:
        try:
            update_method(processor, recordInfoByName, records_to_delete, ioc_info)
            success = True
            return success
        except RequestException as e:
            _log.error("ChannelFinder update failed: {s}".format(s=e))
            retry_seconds = min(60, sleep)
            _log.info("ChannelFinder update retry in {retry_seconds} seconds".format(retry_seconds=retry_seconds))
            time.sleep(retry_seconds)
            sleep *= 1.5
    _log.info("Polling %s complete", ioc_info)
