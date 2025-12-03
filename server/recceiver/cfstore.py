# -*- coding: utf-8 -*-

import datetime
import enum
import logging
import socket
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

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
DEFAULT_MAX_CHANNEL_NAME_QUERY_LENGTH = 600
DEFAULT_QUERY_LIMIT = 10_000


class PVStatus(enum.Enum):
    """PV Status values."""

    Active = enum.auto()
    Inactive = enum.auto()


@dataclass
class CFConfig:
    """Configuration options for the CF Processor"""

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
    timezone: Optional[str] = None
    cf_query_limit: int = DEFAULT_QUERY_LIMIT

    @classmethod
    def loads(cls, conf: ConfigAdapter) -> "CFConfig":
        """Load configuration from a ConfigAdapter instance.

        Args:
            conf: ConfigAdapter instance containing configuration data.
        """
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
            cf_query_limit=conf.get("findSizeLimit", DEFAULT_QUERY_LIMIT),
        )


@dataclass
class CFProperty:
    name: str
    owner: str
    value: Optional[str] = None

    def as_dict(self) -> Dict[str, str]:
        """Convert to dictionary for Channelfinder API."""
        return {"name": self.name, "owner": self.owner, "value": self.value or ""}

    @classmethod
    def from_channelfinder_dict(cls, prop_dict: Dict[str, str]) -> "CFProperty":
        """Create CFProperty from Channelfinder json output.

        Args:
            prop_dict: Dictionary representing a property from Channelfinder.
        """
        return cls(
            name=prop_dict.get("name", ""),
            owner=prop_dict.get("owner", ""),
            value=prop_dict.get("value"),
        )

    @classmethod
    def record_type(cls, owner: str, record_type: str) -> "CFProperty":
        """Create a Channelfinder recordType property.

        Args:
            owner: The owner of the property.
            recordType: The recordType of the property.
        """
        return cls(CFPropertyName.recordType.name, owner, record_type)

    @classmethod
    def alias(cls, owner: str, alias: str) -> "CFProperty":
        """Create a Channelfinder alias property.

        Args:
            owner: The owner of the property.
            alias: The alias of the property.
        """
        return cls(CFPropertyName.alias.name, owner, alias)

    @classmethod
    def pv_status(cls, owner: str, pv_status: PVStatus) -> "CFProperty":
        """Create a Channelfinder pvStatus property.

        Args:
            owner: The owner of the property.
            pvStatus: The pvStatus of the property.
        """
        return cls(CFPropertyName.pvStatus.name, owner, pv_status.name)

    @classmethod
    def active(cls, owner: str) -> "CFProperty":
        """Create a Channelfinder active property.

        Args:
            owner: The owner of the property.
        """
        return cls.pv_status(owner, PVStatus.Active)

    @classmethod
    def inactive(cls, owner: str) -> "CFProperty":
        """Create a Channelfinder inactive property.

        Args:
            owner: The owner of the property.
        """
        return cls.pv_status(owner, PVStatus.Inactive)

    @classmethod
    def time(cls, owner: str, time: str) -> "CFProperty":
        """Create a Channelfinder time property.

        Args:
            owner: The owner of the property.
            time: The time of the property.
        """
        return cls(CFPropertyName.time.name, owner, time)


@dataclass
class RecordInfo:
    """Information about a record to be stored in Channelfinder."""

    pv_name: str
    record_type: Optional[str] = None
    info_properties: List[CFProperty] = field(default_factory=list)
    aliases: List[str] = field(default_factory=list)


class CFPropertyName(enum.Enum):
    """Standard property names used in Channelfinder."""

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


@dataclass
class IocInfo:
    """Information about an IOC instance."""

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
        """Generate a unique IOC ID based on hostname and port."""
        return self.host + ":" + str(self.port)


@dataclass
class CFChannel:
    """Representation of a Channelfinder channel."""

    name: str
    owner: str
    properties: List[CFProperty]

    def as_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for conversion to json in Channelfinder API."""
        return {
            "name": self.name,
            "owner": self.owner,
            "properties": [p.as_dict() for p in self.properties],
        }

    @classmethod
    def from_channelfinder_dict(cls, channel_dict: Dict[str, Any]) -> "CFChannel":
        """Create CFChannel from Channelfinder json output.

        Args:
            channel_dict: Dictionary representing a channel from Channelfinder.
        """
        return cls(
            name=channel_dict.get("name", ""),
            owner=channel_dict.get("owner", ""),
            properties=[CFProperty.from_channelfinder_dict(p) for p in channel_dict.get("properties", [])],
        )


@implementer(interfaces.IProcessor)
class CFProcessor(service.Service):
    """Processor for committing IOC and Record information to Channelfinder."""

    def __init__(self, name: Optional[str], conf: ConfigAdapter):
        """Initialize the CFProcessor with configuration.

        Args:
            name: The name of the processor.
            conf: The configuration for the processor.
        """
        self.cf_config = CFConfig.loads(conf)
        _log.info("CF_INIT %s", self.cf_config)
        self.name = name
        self.channel_ioc_ids: Dict[str, List[str]] = defaultdict(list)
        self.iocs: Dict[str, IocInfo] = dict()
        self.client: Optional[ChannelFinderClient] = None
        self.current_time: Callable[[Optional[str]], str] = get_current_time
        self.lock: DeferredLock = DeferredLock()

    def startService(self):
        """Start the CFProcessor service."""
        service.Service.startService(self)
        # Returning a Deferred is not supported by startService(),
        # so instead attempt to acquire the lock synchonously!
        d = self.lock.acquire()
        if not d.called:
            d.cancel()
            service.Service.stopService(self)
            raise RuntimeError("Failed to acquired CF Processor lock for service start")

        try:
            self._start_service_with_lock()
        except:
            service.Service.stopService(self)
            raise
        finally:
            self.lock.release()

    def _start_service_with_lock(self):
        """Start the CFProcessor service with lock held.

        Using the default python cf-client.  The url, username, and
        password are provided by the channelfinder._conf module.
        """
        _log.info("CF_START")

        if self.client is None:  # For setting up mock test client
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
        """Stop the CFProcessor service."""
        _log.info("CF_STOP")
        service.Service.stopService(self)
        return self.lock.run(self._stop_service_with_lock)

    def _stop_service_with_lock(self):
        """Stop the CFProcessor service with lock held.

        If clean_on_stop is enabled, mark all channels as inactive.
        """
        if self.cf_config.clean_on_stop:
            self.clean_service()
        _log.info("CF_STOP with lock")

    # @defer.inlineCallbacks # Twisted v16 does not support cancellation!
    def commit(self, transaction_record: interfaces.ITransaction) -> defer.Deferred:
        """Commit a transaction to Channelfinder.

        Args:
            transaction_record: The transaction to commit.
        """
        return self.lock.run(self._commit_with_lock, transaction_record)

    def _commit_with_lock(self, transaction: interfaces.ITransaction) -> defer.Deferred:
        """Commit a transaction to Channelfinder with lock held.

        Args:
            transaction: The transaction to commit.
        """
        self.cancelled = False

        t = deferToThread(self._commitWithThread, transaction)

        def cancelCommit(d: defer.Deferred):
            """Cancel the commit operation."""
            self.cancelled = True
            d.callback(None)

        d: defer.Deferred = defer.Deferred(cancelCommit)

        def waitForThread(_ignored):
            """Wait for the commit thread to finish."""
            if self.cancelled:
                return t

        d.addCallback(waitForThread)

        def chainError(err):
            """Handle errors from the commit thread.

            Note this is not foolproof as the thread may still be running.
            """
            if not err.check(defer.CancelledError):
                _log.error("CF_COMMIT FAILURE: %s", err)
            if self.cancelled:
                if not err.check(defer.CancelledError):
                    raise defer.CancelledError()
                return err
            else:
                d.callback(None)

        def chainResult(result):
            """Handle successful completion of the commit thread.

            If the commit was cancelled, raise CancelledError.
            """
            if self.cancelled:
                raise defer.CancelledError(f"CF Processor is cancelled, due to {result}")
            else:
                d.callback(None)

        t.addCallbacks(chainResult, chainError)
        return d

    def transaction_to_record_infos(self, ioc_info: IocInfo, transaction: CommitTransaction) -> Dict[str, RecordInfo]:
        """Convert a CommitTransaction and IocInfo to a dictionary of RecordInfo objects.

        Combines record additions, info tags, aliases, and environment variables.

        Args:
            ioc_info: Information from the IOC
            transaction: transaction from reccaster
        """
        record_infos: Dict[str, RecordInfo] = {}
        for record_id, (record_name, record_type) in transaction.records_to_add.items():
            record_infos[record_id] = RecordInfo(pv_name=record_name, record_type=None, info_properties=[], aliases=[])
            if self.cf_config.record_type_enabled:
                record_infos[record_id].record_type = record_type

        for record_id, (record_infos_to_add) in transaction.record_infos_to_add.items():
            # find intersection of these sets
            if record_id not in record_infos:
                _log.warning("IOC: %s: PV not found for recinfo with RID: {record_id}", ioc_info, record_id)
                continue
            recinfo_wl = [p for p in self.record_property_names_list if p in record_infos_to_add.keys()]
            if recinfo_wl:
                for infotag in recinfo_wl:
                    record_infos[record_id].info_properties.append(
                        CFProperty(infotag, ioc_info.owner, record_infos_to_add[infotag])
                    )

        for record_id, record_aliases in transaction.aliases.items():
            if record_id not in record_infos:
                _log.warning("IOC: %s: PV not found for alias with RID: %s", ioc_info, record_id)
                continue
            record_infos[record_id].aliases = record_aliases

        for record_id in record_infos:
            for epics_env_var_name, cf_prop_name in self.env_vars.items():
                if transaction.client_infos.get(epics_env_var_name) is not None:
                    record_infos[record_id].info_properties.append(
                        CFProperty(cf_prop_name, ioc_info.owner, transaction.client_infos.get(epics_env_var_name))
                    )
                else:
                    _log.debug(
                        "EPICS environment var %s listed in environment_vars setting list not found in this IOC: %s",
                        epics_env_var_name,
                        ioc_info,
                    )
        return record_infos

    def record_info_by_name(self, record_infos: Dict[str, RecordInfo], ioc_info: IocInfo) -> Dict[str, RecordInfo]:
        """Create a dictionary of RecordInfo objects keyed by pvName.

        Args:
            record_infos: Dictionary of RecordInfo objects keyed by record_id.
            ioc_info: Information from the IOC.
        """
        record_info_by_name = {}
        for record_id, (info) in record_infos.items():
            if info.pv_name in record_info_by_name:
                _log.warning("Commit contains multiple records with PV name: %s (%s)", info.pv_name, ioc_info)
                continue
            record_info_by_name[info.pv_name] = info
        return record_info_by_name

    def update_ioc_infos(
        self,
        transaction: CommitTransaction,
        ioc_info: IocInfo,
        records_to_delete: List[str],
        record_info_by_name: Dict[str, RecordInfo],
    ) -> None:
        """Update the internal IOC information based on the transaction.

        Makes changed to self.iocs and self.channel_ioc_ids and records_to_delete.

        Args:
            transaction: The CommitTransaction being processed.
            ioc_info: The IocInfo for the IOC in the transaction.
            records_to_delete: List of record names to delete.
            record_info_by_name: Dictionary of RecordInfo objects keyed by pvName.
        """
        iocid = ioc_info.ioc_id
        if transaction.initial:
            # Add IOC to source list
            self.iocs[iocid] = ioc_info
        if not transaction.connected:
            records_to_delete.extend(self.channel_ioc_ids.keys())
        for record_name in record_info_by_name.keys():
            self.channel_ioc_ids[record_name].append(iocid)
            self.iocs[iocid].channelcount += 1
            # In case, alias exists
            if self.cf_config.alias_enabled:
                if record_name in record_info_by_name:
                    for record_aliases in record_info_by_name[record_name].aliases:
                        self.channel_ioc_ids[record_aliases].append(iocid)  # add iocname to pvName in dict
                        self.iocs[iocid].channelcount += 1
        for record_name in records_to_delete:
            if iocid in self.channel_ioc_ids[record_name]:
                self.remove_channel(record_name, iocid)
                # In case, alias exists
                if self.cf_config.alias_enabled:
                    if record_name in record_info_by_name:
                        for record_aliases in record_info_by_name[record_name].aliases:
                            self.remove_channel(record_aliases, iocid)

    def _commitWithThread(self, transaction: CommitTransaction):
        """Commit the transaction to Channelfinder.

        Collects the ioc info from the transaction.
        Collects the record infos from the transaction.
        Collects the records to delete from the transaction.
        Calculates the records by names.
        Updates the local IOC information.
        Polls Channelfinder with the required updates until it passes.

        Args:
            transaction: The transaction to commit.
        """
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
            time=self.current_time(self.cf_config.timezone),
            port=transaction.source_address.port,
            channelcount=0,
        )

        record_infos = self.transaction_to_record_infos(ioc_info, transaction)

        records_to_delete = list(transaction.records_to_delete)
        _log.debug("Delete records: %s", records_to_delete)

        record_info_by_name = self.record_info_by_name(record_infos, ioc_info)
        self.update_ioc_infos(transaction, ioc_info, records_to_delete, record_info_by_name)
        poll(_update_channelfinder, self, record_info_by_name, records_to_delete, ioc_info)

    def remove_channel(self, recordName: str, iocid: str) -> None:
        """Remove channel from self.iocs and self.channel_ioc_ids.

        Args:
            recordName: The name of the record to remove.
            iocid: The IOC ID of the record to remove from.
        """
        self.channel_ioc_ids[recordName].remove(iocid)
        if iocid in self.iocs:
            self.iocs[iocid].channelcount -= 1
        if self.iocs[iocid].channelcount == 0:
            self.iocs.pop(iocid, None)
        elif self.iocs[iocid].channelcount < 0:
            _log.error("Channel count negative: %s", iocid)
        if len(self.channel_ioc_ids[recordName]) <= 0:  # case: channel has no more iocs
            del self.channel_ioc_ids[recordName]

    def clean_service(self) -> None:
        """Marks all channels belonging to this recceiver (as found by the recceiver id) as 'Inactive'."""
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
                _log.error("Clean service failed: %s", e)
            retry_seconds = min(60, sleep)
            _log.info("Clean service retry in %s seconds", retry_seconds)
            time.sleep(retry_seconds)
            sleep *= 1.5
            if self.running == 0 and sleep >= retry_limit:
                _log.info("Abandoning clean after %s seconds", retry_limit)
                return

    def get_active_channels(self, recceiverid: str) -> List[CFChannel]:
        """Gets all the channels which are active for the given recceiver id.

        Args:
            recceiverid: The current recceiver id.
        """
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

    def clean_channels(self, owner: str, channels: List[CFChannel]) -> None:
        """Set the pvStatus property to 'Inactive' for the given channels.

        Args:
            owner: The owner of the channels.
            channels: The channels to set to 'Inactive'.
        """
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
    record_info_by_name: Dict[str, RecordInfo],
) -> None:
    """Handle the case when the channel exists in channelfinder but not in the recceiver.

    Modifies:
        channels

    Args:
        channel_ioc_ids: mapping of channels to ioc ids
        cf_channel: The channel that is old
        iocs: List of all known iocs
        ioc_info: Current ioc
        recceiverid: id of current recceiver
        processor: Processor going through transaction
        cf_config: Configuration used for processor
        channels: list of the current channel changes
        record_info_by_name: Input information from the transaction
    """
    last_ioc_id = channel_ioc_ids[cf_channel.name][-1]
    cf_channel.owner = iocs[last_ioc_id].owner
    cf_channel.properties = __merge_property_lists(
        create_default_properties(ioc_info, recceiverid, channel_ioc_ids, iocs, cf_channel),
        cf_channel,
        processor.managed_properties,
    )
    channels.append(cf_channel)
    _log.debug("Add existing channel %s to previous IOC %s", cf_channel, last_ioc_id)
    # In case alias exist, also delete them
    if cf_config.alias_enabled:
        if cf_channel.name in record_info_by_name:
            for alias_name in record_info_by_name[cf_channel.name].aliases:
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
                    channels.append(alias_channel)
                    _log.debug("Add existing alias %s to previous IOC: %s", alias_channel, last_alias_ioc_id)


def orphan_channel(
    cf_channel: CFChannel,
    ioc_info: IocInfo,
    channels: List[CFChannel],
    cf_config: CFConfig,
    record_info_by_name: Dict[str, RecordInfo],
) -> None:
    """Handle a channel that exists in channelfinder but not on this recceiver.

    Modifies:
        channels

    Args:
        cf_channel: The channel to orphan
        ioc_info: Info of the current ioc
        channels: The current list of channel changes
        cf_config: Configuration of the proccessor
        record_info_by_name: information from the transaction
    """
    cf_channel.properties = __merge_property_lists(
        [
            CFProperty.inactive(ioc_info.owner),
            CFProperty.time(ioc_info.owner, ioc_info.time),
        ],
        cf_channel,
    )
    channels.append(cf_channel)
    _log.debug("Add orphaned channel %s with no IOC: %s", cf_channel, ioc_info)
    # Also orphan any alias
    if cf_config.alias_enabled:
        if cf_channel.name in record_info_by_name:
            for alias_name in record_info_by_name[cf_channel.name].aliases:
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
    record_info_by_name: Dict[str, RecordInfo],
    old_channels: List[CFChannel],
) -> None:
    """
    Channel exists in Channelfinder with same iocid.
    Update the status to ensure it is marked active and update the time.

    Modifies:
        channels
        new_channels

    Args:
        cf_channel: The channel to update
        iocid: The IOC ID of the channel
        ioc_info: Info of the current ioc
        processor: Processor going through transaction
        channels: The current list of channel changes
        new_channels: The list of new channels
        cf_config: Configuration of the processor
        record_info_by_name: information from the transaction
        old_channels: The list of old channels
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

    # In case, alias exist
    if cf_config.alias_enabled:
        if cf_channel.name in record_info_by_name:
            for alias_name in record_info_by_name[cf_channel.name].aliases:
                if alias_name in old_channels:
                    # alias exists in old list
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
                    # alias exists but not part of old list
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
    """Get the channels existing in channelfinder from the list of new channels.

    Args:
        new_channels: The list of new channels.
        client: The client to contact channelfinder
        cf_config: The configuration for the processor.
        processor: The processor.
    """
    existing_channels: Dict[str, CFChannel] = {}

    # The list of pv's is searched keeping in mind the limitations on the URL length
    search_strings = []
    search_string = ""
    for channel_name in new_channels:
        if not search_string:
            search_string = channel_name
        elif len(search_string) + len(channel_name) < 600:
            search_string = search_string + "|" + channel_name
        else:
            search_strings.append(search_string)
            search_string = channel_name
    if search_string:
        search_strings.append(search_string)

    for each_search_string in search_strings:
        _log.debug("Find existing channels by name: %s", each_search_string)
        for found_channel in client.findByArgs(prepareFindArgs(cf_config, [("~name", each_search_string)])):
            existing_channels[found_channel["name"]] = CFChannel.from_channelfinder_dict(found_channel)
        if processor.cancelled:
            raise defer.CancelledError(
                f"CF Processor is cancelled, while searching for existing channels: {each_search_string}"
            )
    return existing_channels


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
    record_info_by_name: Dict[str, RecordInfo],
    iocid: str,
) -> None:
    """Handle channels already present in Channelfinder for this IOC.

    Loops through all the old_channels,
        if it is on another ioc clean up reference to old ioc
        if it is not on another ioc set as Inactive
        if it is on current ioc update the properties

    Modifies:
        channels: The list of channels.
        iocs: The dictionary of IOCs.
        channel_ioc_ids: The dictionary of channel names to IOC IDs.
        new_channels: The list of new channels.

    Args:
        old_channels: The list of old channels.
        new_channels: The list of new channels.
        channel_ioc_ids: The dictionary of channel names to IOC IDs.
        recceiver_id: The recceiver ID.
        iocs: The dictionary of IOCs.
        records_to_delete: The list of records to delete.
        ioc_info: The IOC information.
        managed_properties: The properites managed by this recceiver.
        channels: The list of channels.
        alias_enabled: Whether aliases are enabled.
        record_type_enabled: Whether record types are enabled.
        record_info_by_name: The dictionary of record names to information.
        iocid: The IOC ID.
        cf_config: The configuration for the processor.
        processor: The processor.
    """
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
                    record_info_by_name,
                )
            else:
                orphan_channel(cf_channel, ioc_info, channels, cf_config, record_info_by_name)
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
                    record_info_by_name,
                    old_channels,
                )


def update_existing_channel_diff_iocid(
    existing_channels: Dict[str, CFChannel],
    channel_name: str,
    new_properties: List[CFProperty],
    processor: CFProcessor,
    channels: List[CFChannel],
    cf_config: CFConfig,
    record_info_by_name: Dict[str, RecordInfo],
    ioc_info: IocInfo,
    iocid: str,
) -> None:
    """Update existing channel with the changed properties.

    Modifies:
        channels

    Args:
        existing_channels: The dictionary of existing channels.
        channel_name: The name of the channel.
        new_properties: The new properties.
        processor: The processor.
        channels: The list of channels.
        cf_config: configuration of processor
        record_info_by_name: The dictionary of record names to information.
        ioc_info: The IOC information.
        iocid: The IOC ID.
    """
    existing_channel = existing_channels[channel_name]
    existing_channel.properties = __merge_property_lists(
        new_properties,
        existing_channel,
        processor.managed_properties,
    )
    channels.append(existing_channel)
    _log.debug("Add existing channel with different IOC: %s", existing_channel)
    # in case, alias exists, update their properties too
    if cf_config.alias_enabled:
        if channel_name in record_info_by_name:
            alias_properties = [CFProperty.alias(ioc_info.owner, channel_name)]
            for p in new_properties:
                alias_properties.append(p)
            for alias_name in record_info_by_name[channel_name].aliases:
                if alias_name in existing_channels:
                    ach = existing_channels[alias_name]
                    ach.properties = __merge_property_lists(
                        alias_properties,
                        ach,
                        processor.managed_properties,
                    )
                    channels.append(ach)
                else:
                    channels.append(CFChannel(alias_name, ioc_info.owner, alias_properties))
                _log.debug("Add existing alias %s of %s with different IOC from %s", alias_name, channel_name, iocid)


def create_new_channel(
    channels: List[CFChannel],
    channel_name: str,
    ioc_info: IocInfo,
    new_properties: List[CFProperty],
    cf_config: CFConfig,
    record_info_by_name: Dict[str, RecordInfo],
) -> None:
    """Create a new channel.

    Modifies:
        channels

    Args:
        channels: The list of channels.
        channel_name: The name of the channel.
        ioc_info: The IOC information.
        new_properties: The new properties.
        cf_config: configuration of processor
        record_info_by_name: The dictionary of record names to information.
    """

    channels.append(CFChannel(channel_name, ioc_info.owner, new_properties))
    _log.debug("Add new channel: %s", channel_name)
    if cf_config.alias_enabled:
        if channel_name in record_info_by_name:
            alias_properties = [CFProperty.alias(ioc_info.owner, channel_name)]
            for p in new_properties:
                alias_properties.append(p)
            for alias in record_info_by_name[channel_name].aliases:
                channels.append(CFChannel(alias, ioc_info.owner, alias_properties))
                _log.debug("Add new alias: %s from %s", alias, channel_name)


def _update_channelfinder(
    processor: CFProcessor, record_info_by_name: Dict[str, RecordInfo], records_to_delete, ioc_info: IocInfo
) -> None:
    """Update Channelfinder with the provided IOC and Record information.

    Calculates the changes required to the channels list and pushes the update the channelfinder.

    Args:
        processor: The processor.
        record_info_by_name: The dictionary of record names to information.
        records_to_delete: The list of records to delete.
        ioc_info: The IOC information.
    """
    _log.info("CF Update IOC: %s", ioc_info)
    _log.debug("CF Update IOC: %s record_info_by_name %s", ioc_info, record_info_by_name)
    # Consider making this function a class methed then 'processor' simply becomes 'self'
    client = processor.client
    channel_ioc_ids = processor.channel_ioc_ids
    iocs = processor.iocs
    cf_config = processor.cf_config
    recceiverid = processor.cf_config.recceiver_id
    new_channels = set(record_info_by_name.keys())
    iocid = ioc_info.ioc_id

    if iocid not in iocs:
        _log.warning("IOC Env Info %s not found in ioc list: %s", ioc_info, iocs)

    if ioc_info.hostname is None or ioc_info.ioc_name is None:
        raise Exception(f"Missing hostName {ioc_info.hostname} or iocName {ioc_info.ioc_name}")

    if processor.cancelled:
        raise defer.CancelledError(f"Processor cancelled in _update_channelfinder for {ioc_info}")

    channels: List[CFChannel] = []
    # A list of channels in channelfinder with the associated hostName and iocName
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
            record_info_by_name,
            iocid,
        )
    # now pvNames contains a list of pv's new on this host/ioc
    existing_channels = get_existing_channels(new_channels, client, cf_config, processor)

    for channel_name in new_channels:
        new_properties = create_ioc_properties(
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
            and channel_name in record_info_by_name
            and record_info_by_name[channel_name].record_type
        ):
            new_properties.append(CFProperty.record_type(ioc_info.owner, record_info_by_name[channel_name].record_type))
        if channel_name in record_info_by_name:
            new_properties = new_properties + record_info_by_name[channel_name].info_properties

        if channel_name in existing_channels:
            _log.debug("update existing channel %s: exists but with a different iocid from %s", channel_name, iocid)
            update_existing_channel_diff_iocid(
                existing_channels,
                channel_name,
                new_properties,
                processor,
                channels,
                cf_config,
                record_info_by_name,
                ioc_info,
                iocid,
            )
        else:
            create_new_channel(channels, channel_name, ioc_info, new_properties, cf_config, record_info_by_name)
    _log.info("Total channels to update: %s for ioc: %s", len(channels), ioc_info)

    if len(channels) != 0:
        cf_set_chunked(client, channels, cf_config.cf_query_limit)
    else:
        if old_channels and len(old_channels) != 0:
            cf_set_chunked(client, channels, cf_config.cf_query_limit)
    if processor.cancelled:
        raise defer.CancelledError(f"Processor cancelled in _update_channelfinder for {ioc_info}")


def cf_set_chunked(client: ChannelFinderClient, channels: List[CFChannel], chunk_size=DEFAULT_QUERY_LIMIT) -> None:
    """Submit a list of channels to channelfinder in a chunked way.

    Args:
        client: The channelfinder client.
        channels: The list of channels.
        chunk_size: The chunk size.
    """
    for i in range(0, len(channels), chunk_size):
        chunk = [ch.as_dict() for ch in channels[i : i + chunk_size]]
        client.set(channels=chunk)


def create_ioc_properties(
    owner: str, iocTime: str, recceiverid: str, hostName: str, iocName: str, iocIP: str, iocid: str
) -> List[CFProperty]:
    """Create the properties from an IOC.

    Args:
        owner: The owner of the properties.
        iocTime: The time of the properties.
        recceiverid: The recceiver ID of the properties.
        hostName: The host name of the properties.
        iocName: The IOC name of the properties.
        iocIP: The IOC IP of the properties.
        iocid: The IOC ID of the properties.
    """
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
) -> List[CFProperty]:
    """Create the default properties for an IOC.

    Args:
        ioc_info: The IOC information.
        recceiverid: The recceiver ID of the properties.
        channels_iocs: The dictionary of channel names to IOC IDs.
        iocs: The dictionary of IOCs.
        cf_channel: The Channelfinder channel.
    """
    channel_name = cf_channel.name
    last_ioc_info = iocs[channels_iocs[channel_name][-1]]
    return create_ioc_properties(
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
    """Merges two lists of properties.

    Ensures that there are no 2 properties with
    the same name In case of overlap between the new and old property lists the
    new property list wins out.

    Args:
        newProperties: The new properties.
        channel: The channel.
        managed_properties: The managed properties
    """
    newPropNames = [p.name for p in newProperties]
    for oldProperty in channel.properties:
        if oldProperty.name not in newPropNames and (oldProperty.name not in managed_properties):
            newProperties = newProperties + [oldProperty]
    return newProperties


def get_current_time(timezone: Optional[str] = None) -> str:
    """Get the current time.

    Args:
        timezone: The timezone.
    """
    if timezone:
        return str(datetime.datetime.now().astimezone())
    return str(datetime.datetime.now())


def prepareFindArgs(cf_config: CFConfig, args, size=0) -> List[Tuple[str, str]]:
    """Prepare the find arguments.

    Args:
        cf_config: The configuration.
        args: The arguments.
        size: The size.
    """
    size_limit = int(cf_config.cf_query_limit)
    if size_limit > 0:
        args.append(("~size", size_limit))
    return args


def poll(
    update_method: Callable[[CFProcessor, Dict[str, RecordInfo], List[str], IocInfo], None],
    processor: CFProcessor,
    record_info_by_name: Dict[str, RecordInfo],
    records_to_delete,
    ioc_info: IocInfo,
) -> bool:
    """Poll channelfinder with updates until it passes.

    Args:
        update_method: The update method.
        processor: The processor.
        record_info_by_name: The record information by name.
        records_to_delete: The records to delete.
        ioc_info: The IOC information.
    """
    _log.info("Polling for %s begins...", ioc_info)
    sleep = 1.0
    success = False
    while not success:
        try:
            update_method(processor, record_info_by_name, records_to_delete, ioc_info)
            success = True
            return success
        except RequestException as e:
            _log.error("ChannelFinder update failed: %s", e)
            retry_seconds = min(60, sleep)
            _log.info("ChannelFinder update retry in %s seconds", retry_seconds)
            time.sleep(retry_seconds)
            sleep *= 1.5
    _log.info("Polling %s complete", ioc_info)
    return success
