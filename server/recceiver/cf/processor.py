import datetime
import logging
import time
from collections import defaultdict
from typing import Callable, Dict, List, Optional, Set

from channelfinder import ChannelFinderClient
from requests import ConnectionError, RequestException
from twisted.application import service
from twisted.internet import defer
from twisted.internet.defer import DeferredLock
from twisted.internet.threads import deferToThread
from zope.interface import implementer

from recceiver import interfaces
from recceiver.cf.adapter import ChannelFinderAdapter, PyCFClientAdapter
from recceiver.cf.config import CFConfig
from recceiver.cf.model import (
    CFChannel,
    CFProperty,
    CFPropertyName,
    IOCInfo,
    IOCMissingInfoError,
    PVStatus,
    RecordInfo,
)
from recceiver.interfaces import CommitTransaction
from recceiver.processors import ConfigAdapter

_log = logging.getLogger(__name__)


@implementer(interfaces.IProcessor)
class CFProcessor(service.Service):
    """IProcessor plugin that synchronises IOC record data to Channelfinder.

    Maintains in-memory state (channel_ioc_ids, iocs) to reconcile the current
    snapshot with what CF holds, then pushes the minimal diff on each commit.
    """

    def __init__(self, name: Optional[str], conf: ConfigAdapter):
        self.cf_config = CFConfig.loads(conf)
        self.name = name  # Override name from service.Service
        self.channel_ioc_ids: Dict[str, List[str]] = defaultdict(list)
        self.iocs: Dict[str, IOCInfo] = {}
        self.client: Optional[ChannelFinderAdapter] = None
        self.current_time: Callable[[Optional[str]], str] = get_current_time
        self.lock: DeferredLock = DeferredLock()

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
            self._start_service_with_lock()
        except:
            service.Service.stopService(self)
            raise
        finally:
            self.lock.release()

    def _start_service_with_lock(self):
        _log.info("CF_START with configuration: %s", self.cf_config)

        if self.client is None:  # For setting up mock test client
            self.client = PyCFClientAdapter(
                ChannelFinderClient(
                    BaseURL=self.cf_config.base_url,
                    username=self.cf_config.cf_username,
                    password=self.cf_config.cf_password,
                    verify_ssl=self.cf_config.verify_ssl,
                ),
                size_limit=int(self.cf_config.cf_query_limit),
            )
            try:
                cf_properties = set(self.client.get_property_names())
                self._setup_cf_properties(cf_properties)
            except ConnectionError:
                _log.exception("Cannot connect to Channelfinder service")
                raise
            else:
                if self.cf_config.clean_on_start:
                    self.clean_service()

    def _setup_cf_properties(self, cf_properties: Set[str]) -> None:
        """Compute required CF properties, register any missing ones, and cache state.

        Sets self.env_vars, self.record_property_names_list, and self.managed_properties.
        """
        required_properties = {
            CFPropertyName.HOSTNAME.value,
            CFPropertyName.IOC_NAME.value,
            CFPropertyName.IOC_ID.value,
            CFPropertyName.IOC_IP.value,
            CFPropertyName.PV_STATUS.value,
            CFPropertyName.TIME.value,
            CFPropertyName.RECCEIVER_ID.value,
        }
        if self.cf_config.alias_enabled:
            required_properties.add(CFPropertyName.ALIAS.value)
        if self.cf_config.record_type_enabled:
            required_properties.add(CFPropertyName.RECORD_TYPE.value)

        env_vars_setting = self.cf_config.environment_variables
        self.env_vars = {}
        if env_vars_setting:
            self.env_vars = {
                k.strip(): v.strip() for item in env_vars_setting.split(",") for k, v in [item.split(":", 1)]
            }
            required_properties.update(self.env_vars.values())

        # CA/PVA port properties are sourced from reccaster env vars
        if self.cf_config.ioc_connection_info:
            self.env_vars["RSRV_SERVER_PORT"] = "caPort"
            self.env_vars["PVAS_SERVER_PORT"] = "pvaPort"
            required_properties.add(CFPropertyName.CA_PORT.value)
            required_properties.add(CFPropertyName.PVA_PORT.value)

        # Space or comma and space separated strings
        record_property_names_list = {s.strip(", ") for s in self.cf_config.info_tags.split()}
        if self.cf_config.record_description_enabled:
            record_property_names_list.add(CFPropertyName.RECORD_DESC.value)

        owner = self.cf_config.username
        for prop_name in (required_properties | record_property_names_list) - cf_properties:
            self.client.set_property(prop_name, owner)

        self.record_property_names_list = record_property_names_list
        self.managed_properties = required_properties | record_property_names_list
        _log.debug("record_property_names_list = %s", self.record_property_names_list)

    def stopService(self):
        _log.info("CF_STOP")
        service.Service.stopService(self)
        return self.lock.run(self._stop_service_with_lock)

    def _stop_service_with_lock(self):
        if self.cf_config.clean_on_stop:
            self.clean_service()
        _log.info("CF_STOP with lock")

    # @defer.inlineCallbacks # Twisted v16 does not support cancellation!
    def commit(self, transaction_record: interfaces.ITransaction) -> defer.Deferred:
        """Commit a transaction to Channelfinder."""
        return self.lock.run(self._commit_with_lock, transaction_record)

    def _commit_with_lock(self, transaction: interfaces.ITransaction) -> defer.Deferred:
        self.cancelled = False

        t = deferToThread(self._commit_with_thread, transaction)

        def cancel_commit(d: defer.Deferred):
            self.cancelled = True
            d.callback(None)

        d: defer.Deferred = defer.Deferred(cancel_commit)

        def wait_for_thread(_ignored):
            if self.cancelled:
                return t

        d.addCallback(wait_for_thread)

        def chain_error(err):
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

        def chain_result(result):
            if self.cancelled:
                raise defer.CancelledError(f"CF Processor is cancelled, due to {result}")
            else:
                d.callback(None)

        t.addCallbacks(chain_result, chain_error)
        return d

    def transaction_to_record_infos(self, ioc_info: IOCInfo, transaction: CommitTransaction) -> Dict[str, RecordInfo]:
        """Build a RecordInfo dict keyed by record_id from a transaction.

        Merges record types, info-tag properties, aliases, and mapped EPICS
        environment variables into each record. Only info tags on the
        record_property_names_list whitelist are included.
        """
        record_infos: Dict[str, RecordInfo] = {}
        for record_id, (record_name, record_type) in transaction.records_to_add.items():
            record_infos[record_id] = RecordInfo(pv_name=record_name, record_type=None, info_properties=[], aliases=[])
            if self.cf_config.record_type_enabled:
                record_infos[record_id].record_type = record_type

        for record_id, (record_infos_to_add) in transaction.record_infos_to_add.items():
            # find intersection of these sets
            if record_id not in record_infos:
                _log.warning("IOC: %s: PV not found for recinfo with RID: %s", ioc_info, record_id)
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

        self._apply_env_vars(record_infos, ioc_info, transaction)
        return record_infos

    def _apply_env_vars(
        self,
        record_infos: Dict[str, RecordInfo],
        ioc_info: IOCInfo,
        transaction: CommitTransaction,
    ) -> None:
        """Append mapped EPICS environment variable properties to every record."""
        for record_id in record_infos:
            for epics_env_var_name, cf_prop_name in self.env_vars.items():
                value = transaction.client_infos.get(epics_env_var_name)
                if value is not None:
                    record_infos[record_id].info_properties.append(CFProperty(cf_prop_name, ioc_info.owner, value))
                else:
                    _log.debug(
                        "EPICS environment var %s not found in IOC: %s",
                        epics_env_var_name,
                        ioc_info,
                    )

    @staticmethod
    def record_info_by_name(record_infos: Dict[str, RecordInfo], ioc_info: IOCInfo) -> Dict[str, RecordInfo]:
        """Re-key a record_id-to-RecordInfo dict by pv_name instead.

        Logs and skips duplicate PV names within the same commit.
        """
        record_info_by_name = {}
        for info in record_infos.values():
            if info.pv_name in record_info_by_name:
                _log.warning("Commit contains multiple records with PV name: %s (%s)", info.pv_name, ioc_info)
                continue
            record_info_by_name[info.pv_name] = info
        return record_info_by_name

    def update_ioc_infos(
        self,
        transaction: CommitTransaction,
        ioc_info: IOCInfo,
        records_to_delete: List[str],
        record_info_by_name: Dict[str, RecordInfo],
    ) -> None:
        """Reconcile channel_ioc_ids and iocs against the transaction.

        On initial transaction, registers the IOC. On disconnect, queues all
        its channels for deletion. Adds or removes channel-to-ioc mappings and
        updates channelcount, including aliases when enabled.
        """
        iocid = ioc_info.id
        if transaction.initial:
            self.iocs[iocid] = ioc_info
        if not transaction.connected:
            records_to_delete.extend(self.channel_ioc_ids.keys())
        for record_name in record_info_by_name:
            self.channel_ioc_ids[record_name].append(iocid)
            self.iocs[iocid].channelcount += 1
            if self.cf_config.alias_enabled:
                self._register_aliases(record_info_by_name[record_name].aliases, iocid)
        for record_name in records_to_delete:
            if iocid in self.channel_ioc_ids[record_name]:
                self.remove_channel(record_name, iocid)
                if self.cf_config.alias_enabled and record_name in record_info_by_name:
                    self._remove_aliases(record_info_by_name[record_name].aliases, iocid)

    def _register_aliases(self, aliases: List[str], iocid: str) -> None:
        for alias in aliases:
            self.channel_ioc_ids[alias].append(iocid)
            self.iocs[iocid].channelcount += 1

    def _remove_aliases(self, aliases: List[str], iocid: str) -> None:
        for alias in aliases:
            self.remove_channel(alias, iocid)

    def _commit_with_thread(self, transaction: CommitTransaction):
        host = transaction.source_address.host
        port = transaction.source_address.port

        if not self.running:
            raise defer.CancelledError(f"CF Processor is not running (transaction: {host}:{port})")

        _log.info("CF_COMMIT: %s", transaction)
        _log.debug("CF_COMMIT: transaction: %s", repr(transaction))

        ioc_name = transaction.client_infos.get("IOCNAME")
        if not ioc_name:
            ioc_name = str(port)
            _log.debug("IOC at %s:%d did not send IOCNAME; using port as iocName", host, port)

        owner = (
            transaction.client_infos.get(self.cf_config.env_owner_variable)
            or transaction.client_infos.get("CF_USERNAME")
            or self.cf_config.username
        )
        if owner == self.cf_config.username:
            _log.debug(
                "IOC at %s:%d did not send %s or CF_USERNAME; using service account as owner",
                host,
                port,
                self.cf_config.env_owner_variable,
            )

        ioc_info = IOCInfo(
            host=host,
            hostname=transaction.client_infos.get("HOSTNAME") or host,
            ioc_name=ioc_name,
            ioc_ip=host,
            owner=owner,
            time=self.current_time(self.cf_config.timezone),
            port=port,
        )

        record_infos = self.transaction_to_record_infos(ioc_info, transaction)

        records_to_delete = list(transaction.records_to_delete)
        _log.debug("Delete records: %s", records_to_delete)

        record_info_by_name = CFProcessor.record_info_by_name(record_infos, ioc_info)
        self.update_ioc_infos(transaction, ioc_info, records_to_delete, record_info_by_name)
        poll_success = self._push_to_cf(record_info_by_name, records_to_delete, ioc_info)
        if not poll_success:
            raise defer.CancelledError(f"Failed to commit transaction after polling retries: {transaction}")

    def remove_channel(self, record_name: str, iocid: str) -> None:
        """Unlink a channel from an IOC in channel_ioc_ids and decrement channelcount.

        Deletes the channel entry when the last IOC reference is removed,
        and deletes the IOC entry when its channelcount reaches zero.
        """
        self.channel_ioc_ids[record_name].remove(iocid)
        if iocid not in self.iocs:
            if len(self.channel_ioc_ids[record_name]) == 0:
                del self.channel_ioc_ids[record_name]
            return
        self.iocs[iocid].channelcount -= 1
        if self.iocs[iocid].channelcount <= 0:
            if self.iocs[iocid].channelcount < 0:
                _log.error("Channel count negative: %s", iocid)
            self.iocs.pop(iocid)
        if len(self.channel_ioc_ids[record_name]) == 0:
            del self.channel_ioc_ids[record_name]

    def clean_service(self) -> None:
        """Mark all channels belonging to this recceiver as 'Inactive'."""
        sleep = 1
        retry_limit = 5
        owner = self.cf_config.username
        recceiverid = self.cf_config.recceiver_id
        while 1:
            try:
                _log.info("CF Clean Started")
                channels = self.get_active_channels(recceiverid)
                while channels:
                    self.clean_channels(owner, channels)
                    channels = self.get_active_channels(recceiverid)
                _log.info("CF Clean Completed")
                return
            except RequestException as e:
                _log.exception("Clean service failed: %s", e)
            retry_seconds = min(60, sleep)
            _log.info("Clean service retry in %s seconds", retry_seconds)
            time.sleep(retry_seconds)
            sleep *= 1.5
            if self.running == 0 and sleep >= retry_limit:
                _log.info("Abandoning clean after %s seconds", retry_limit)
                return

    def get_active_channels(self, recceiverid: str) -> List[CFChannel]:
        """Return all CF channels currently marked Active for this recceiver."""
        return self.client.find_active_for_recceiver(recceiverid)

    def clean_channels(self, owner: str, channels: List[CFChannel]) -> None:
        """Mark the given channels Inactive in CF."""
        names = [ch.name for ch in channels or []]
        _log.info("Cleaning %s channels.", len(names))
        _log.debug('Update "pvStatus" property to "Inactive" for %s channels', len(names))
        self.client.update_property(CFProperty(CFPropertyName.PV_STATUS.value, owner, PVStatus.INACTIVE.value), names)

    def _push_to_cf(
        self,
        record_info_by_name: Dict[str, RecordInfo],
        records_to_delete: List[str],
        ioc_info: IOCInfo,
    ) -> bool:
        _log.info("Pushing updates for %s begins...", ioc_info)
        count = 0
        sleep = 1.0
        while self.cf_config.push_always_retry or count < self.cf_config.push_max_retries:
            count += 1
            try:
                self._update_channelfinder(record_info_by_name, records_to_delete, ioc_info)
                return True
            except RequestException as e:
                _log.exception("ChannelFinder update failed: %s", e)
                retry_seconds = min(60, sleep)
                _log.info("ChannelFinder update retry in %s seconds", retry_seconds)
                time.sleep(retry_seconds)
                sleep *= 1.5
        _log.error("Pushing updates for %s complete, failed after %d attempts", ioc_info, count)
        return False

    def _update_channelfinder(
        self,
        record_info_by_name: Dict[str, RecordInfo],
        records_to_delete: List[str],
        ioc_info: IOCInfo,
    ) -> None:
        _log.info("CF Update IOC: %s", ioc_info)
        _log.debug("CF Update IOC: %s record_info_by_name %s", ioc_info, record_info_by_name)
        recceiverid = self.cf_config.recceiver_id
        new_channels = set(record_info_by_name.keys())
        iocid = ioc_info.id

        if iocid not in self.iocs:
            _log.warning(
                "IOC %s did not send an initial transaction to join IOC list (%d IOCs known)",
                ioc_info,
                len(self.iocs),
            )

        if ioc_info.hostname is None or ioc_info.ioc_name is None:
            raise IOCMissingInfoError(ioc_info)

        if self.cancelled:
            raise defer.CancelledError(f"Processor cancelled in _update_channelfinder for {ioc_info}")

        channels: List[CFChannel] = []
        _log.debug("Find existing channels by IOCID: %s", ioc_info)
        old_channels: List[CFChannel] = self.client.find_by_ioc_id(iocid)

        if old_channels:
            self._handle_channels(
                old_channels,
                new_channels,
                records_to_delete,
                ioc_info,
                recceiverid,
                channels,
                record_info_by_name,
                iocid,
            )
        # now pvNames contains a list of pv's new on this host/ioc
        existing_channels = self._get_existing_channels(new_channels)

        if self.cancelled:
            raise defer.CancelledError(f"CF Processor is cancelled, after fetching existing channels for {ioc_info}")

        self._process_new_channels(
            new_channels, record_info_by_name, ioc_info, recceiverid, existing_channels, channels, iocid
        )
        _log.info("Total channels to update: %s for ioc: %s", len(channels), ioc_info)

        if len(channels) != 0:
            self._cf_set_chunked(channels)
        else:
            if old_channels and len(old_channels) != 0:
                self._cf_set_chunked(channels)
        if self.cancelled:
            raise defer.CancelledError(f"Processor cancelled in _update_channelfinder for {ioc_info}")

    def _process_new_channels(
        self,
        new_channels: Set[str],
        record_info_by_name: Dict[str, RecordInfo],
        ioc_info: IOCInfo,
        recceiverid: str,
        existing_channels: Dict[str, CFChannel],
        channels: List[CFChannel],
        iocid: str,
    ) -> None:
        for channel_name in new_channels:
            new_properties = create_ioc_properties(
                ioc_info.owner,
                ioc_info.time,
                recceiverid,
                ioc_info.hostname,
                ioc_info.ioc_name,
                ioc_info.ioc_ip,
                ioc_info.id,
            )
            record_info = record_info_by_name.get(channel_name)
            if record_info:
                if self.cf_config.record_type_enabled and record_info.record_type:
                    new_properties.append(
                        CFProperty(CFPropertyName.RECORD_TYPE.value, ioc_info.owner, record_info.record_type)
                    )
                new_properties = new_properties + record_info.info_properties
            if channel_name in existing_channels:
                _log.debug("update existing channel %s: exists but with a different iocid from %s", channel_name, iocid)
                self._update_existing_channel_diff_iocid(
                    existing_channels, channel_name, new_properties, channels, record_info_by_name, ioc_info, iocid
                )
            else:
                self._create_new_channel(channels, channel_name, ioc_info, new_properties, record_info_by_name)

    def _cf_set_chunked(self, channels: List[CFChannel]) -> None:
        chunk_size = int(self.cf_config.cf_query_limit)
        for i in range(0, len(channels), chunk_size):
            self.client.set_channels(channels[i : i + chunk_size])

    def _handle_channels(
        self,
        old_channels: List[CFChannel],
        new_channels: Set[str],
        records_to_delete: List[str],
        ioc_info: IOCInfo,
        recceiverid: str,
        channels: List[CFChannel],
        record_info_by_name: Dict[str, RecordInfo],
        iocid: str,
    ) -> None:
        """Handle channels already present in Channelfinder for this IOC.

        For each old channel: if it is not in new_channels or is being deleted,
        re-assign it to its last known IOC or orphan it; if it is in both old
        and new, update its properties in place.
        """
        for cf_channel in old_channels:
            if not new_channels or cf_channel.name in records_to_delete:
                _log.debug("Channel %s exists in Channelfinder not in new_channels", cf_channel)
                if cf_channel.name in self.channel_ioc_ids:
                    self._handle_channel_is_old(cf_channel, ioc_info, recceiverid, channels, record_info_by_name)
                else:
                    self._orphan_channel(cf_channel, ioc_info, channels, record_info_by_name)
            else:
                if cf_channel.name in new_channels:
                    self._handle_channel_old_and_new(
                        cf_channel, iocid, ioc_info, channels, new_channels, record_info_by_name, old_channels
                    )

    def _handle_channel_is_old(
        self,
        cf_channel: CFChannel,
        ioc_info: IOCInfo,
        recceiverid: str,
        channels: List[CFChannel],
        record_info_by_name: Dict[str, RecordInfo],
    ) -> None:
        """Channel exists in CF but not in this commit — re-assign to its last known IOC."""
        last_ioc_id = self.channel_ioc_ids[cf_channel.name][-1]
        cf_channel.owner = self.iocs[last_ioc_id].owner
        cf_channel.properties = _merge_property_lists(
            create_default_properties(ioc_info, recceiverid, self.channel_ioc_ids, self.iocs, cf_channel),
            cf_channel,
            self.managed_properties,
        )
        channels.append(cf_channel)
        _log.debug("Add existing channel %s to previous IOC %s", cf_channel, last_ioc_id)
        if self.cf_config.alias_enabled:
            if cf_channel.name in record_info_by_name:
                for alias_name in record_info_by_name[cf_channel.name].aliases:
                    # Legacy alias handling retained to avoid changing runtime behavior.
                    alias_channel = CFChannel(alias_name, "", [])
                    if alias_name in self.channel_ioc_ids:
                        last_alias_ioc_id = self.channel_ioc_ids[alias_name][-1]
                        alias_channel.owner = self.iocs[last_alias_ioc_id].owner
                        alias_channel.properties = _merge_property_lists(
                            create_default_properties(
                                ioc_info, recceiverid, self.channel_ioc_ids, self.iocs, cf_channel
                            ),
                            alias_channel,
                            self.managed_properties,
                        )
                        channels.append(alias_channel)
                        _log.debug("Add existing alias %s to previous IOC: %s", alias_channel, last_alias_ioc_id)

    def _orphan_channel(
        self,
        cf_channel: CFChannel,
        ioc_info: IOCInfo,
        channels: List[CFChannel],
        record_info_by_name: Dict[str, RecordInfo],
    ) -> None:
        """Channel exists in CF but has no known IOC — mark inactive."""
        cf_channel.properties = _merge_property_lists(
            [
                CFProperty(CFPropertyName.PV_STATUS.value, ioc_info.owner, PVStatus.INACTIVE.value),
                CFProperty(CFPropertyName.TIME.value, ioc_info.owner, ioc_info.time),
            ],
            cf_channel,
        )
        channels.append(cf_channel)
        _log.debug("Add orphaned channel %s with no IOC: %s", cf_channel, ioc_info)
        if self.cf_config.alias_enabled:
            if cf_channel.name in record_info_by_name:
                for alias_name in record_info_by_name[cf_channel.name].aliases:
                    alias_channel = CFChannel(alias_name, "", [])
                    alias_channel.properties = _merge_property_lists(
                        [
                            CFProperty(CFPropertyName.PV_STATUS.value, ioc_info.owner, PVStatus.INACTIVE.value),
                            CFProperty(CFPropertyName.TIME.value, ioc_info.owner, ioc_info.time),
                        ],
                        alias_channel,
                    )
                    channels.append(alias_channel)
                    _log.debug("Add orphaned alias %s with no IOC: %s", alias_channel, ioc_info)

    def _handle_channel_old_and_new(
        self,
        cf_channel: CFChannel,
        iocid: str,
        ioc_info: IOCInfo,
        channels: List[CFChannel],
        new_channels: Set[str],
        record_info_by_name: Dict[str, RecordInfo],
        old_channels: List[CFChannel],
    ) -> None:
        """Channel exists in CF with the same iocid — mark active and update time."""
        _log.debug("Channel %s exists in Channelfinder with same iocid %s", cf_channel.name, iocid)
        cf_channel.properties = _merge_property_lists(
            [
                CFProperty(CFPropertyName.PV_STATUS.value, ioc_info.owner, PVStatus.ACTIVE.value),
                CFProperty(CFPropertyName.TIME.value, ioc_info.owner, ioc_info.time),
            ],
            cf_channel,
            self.managed_properties,
        )
        channels.append(cf_channel)
        _log.debug("Add existing channel with same IOC: %s", cf_channel)
        new_channels.remove(cf_channel.name)

        if self.cf_config.alias_enabled:
            if cf_channel.name in record_info_by_name:
                for alias_name in record_info_by_name[cf_channel.name].aliases:
                    if alias_name in old_channels:
                        alias_channel = CFChannel(alias_name, "", [])
                        alias_channel.properties = _merge_property_lists(
                            [
                                CFProperty(CFPropertyName.PV_STATUS.value, ioc_info.owner, PVStatus.ACTIVE.value),
                                CFProperty(CFPropertyName.TIME.value, ioc_info.owner, ioc_info.time),
                            ],
                            alias_channel,
                            self.managed_properties,
                        )
                        channels.append(alias_channel)
                        new_channels.remove(alias_name)
                    else:
                        aprops = _merge_property_lists(
                            [
                                CFProperty(CFPropertyName.PV_STATUS.value, ioc_info.owner, PVStatus.ACTIVE.value),
                                CFProperty(CFPropertyName.TIME.value, ioc_info.owner, ioc_info.time),
                                CFProperty(CFPropertyName.ALIAS.value, ioc_info.owner, cf_channel.name),
                            ],
                            cf_channel,
                            self.managed_properties,
                        )
                        channels.append(CFChannel(alias_name, ioc_info.owner, aprops))
                        new_channels.remove(alias_name)
                    _log.debug("Add existing alias with same IOC: %s", cf_channel)

    def _get_existing_channels(self, new_channels: Set[str]) -> Dict[str, CFChannel]:
        """Query CF for channels in new_channels that already exist there."""
        return {ch.name: ch for ch in self.client.find_by_names(list(new_channels))}

    def _update_existing_channel_diff_iocid(
        self,
        existing_channels: Dict[str, CFChannel],
        channel_name: str,
        new_properties: List[CFProperty],
        channels: List[CFChannel],
        record_info_by_name: Dict[str, RecordInfo],
        ioc_info: IOCInfo,
        iocid: str,
    ) -> None:
        """Update a channel that exists in CF but is moving to a new IOC."""
        existing_channel = existing_channels[channel_name]
        existing_channel.properties = _merge_property_lists(
            new_properties,
            existing_channel,
            self.managed_properties,
        )
        channels.append(existing_channel)
        _log.debug("Add existing channel with different IOC: %s", existing_channel)
        if self.cf_config.alias_enabled and channel_name in record_info_by_name:
            alias_properties = [CFProperty(CFPropertyName.ALIAS.value, ioc_info.owner, channel_name)] + new_properties
            for alias_name in record_info_by_name[channel_name].aliases:
                if alias_name in existing_channels:
                    ach = existing_channels[alias_name]
                    ach.properties = _merge_property_lists(alias_properties, ach, self.managed_properties)
                    channels.append(ach)
                else:
                    channels.append(CFChannel(alias_name, ioc_info.owner, alias_properties))
                _log.debug("Add existing alias %s of %s with different IOC from %s", alias_name, channel_name, iocid)

    def _create_new_channel(
        self,
        channels: List[CFChannel],
        channel_name: str,
        ioc_info: IOCInfo,
        new_properties: List[CFProperty],
        record_info_by_name: Dict[str, RecordInfo],
    ) -> None:
        channels.append(CFChannel(channel_name, ioc_info.owner, new_properties))
        _log.debug("Add new channel: %s", channel_name)
        if self.cf_config.alias_enabled and channel_name in record_info_by_name:
            alias_properties = [CFProperty(CFPropertyName.ALIAS.value, ioc_info.owner, channel_name)] + new_properties
            for alias in record_info_by_name[channel_name].aliases:
                channels.append(CFChannel(alias, ioc_info.owner, alias_properties))
                _log.debug("Add new alias: %s from %s", alias, channel_name)


def create_ioc_properties(
    owner: str, ioc_time: str, recceiverid: str, host_name: str, ioc_name: str, ioc_ip: str, iocid: str
) -> List[CFProperty]:
    """Build the standard set of IOC-level CF properties for a channel."""
    return [
        CFProperty(CFPropertyName.HOSTNAME.value, owner, host_name),
        CFProperty(CFPropertyName.IOC_NAME.value, owner, ioc_name),
        CFProperty(CFPropertyName.IOC_ID.value, owner, iocid),
        CFProperty(CFPropertyName.IOC_IP.value, owner, ioc_ip),
        CFProperty(CFPropertyName.PV_STATUS.value, owner, PVStatus.ACTIVE.value),
        CFProperty(CFPropertyName.TIME.value, owner, ioc_time),
        CFProperty(CFPropertyName.RECCEIVER_ID.value, owner, recceiverid),
    ]


def create_default_properties(
    ioc_info: IOCInfo,
    recceiverid: str,
    channels_iocs: Dict[str, List[str]],
    iocs: Dict[str, IOCInfo],
    cf_channel: CFChannel,
) -> List[CFProperty]:
    """Build IOC properties using the last known IOC for a channel."""
    channel_name = cf_channel.name
    last_ioc_info = iocs[channels_iocs[channel_name][-1]]
    return create_ioc_properties(
        ioc_info.owner,
        ioc_info.time,
        recceiverid,
        last_ioc_info.hostname,
        last_ioc_info.ioc_name,
        last_ioc_info.ioc_ip,
        last_ioc_info.id,
    )


def _merge_property_lists(
    new_properties: List[CFProperty], channel: CFChannel, managed_properties: Optional[Set[str]] = None
) -> List[CFProperty]:
    """Merge two property lists; new_properties wins on name collision.

    Properties in channel not in new_properties are kept unless they are
    managed by this recceiver (in which case the absence is intentional).
    """
    managed = managed_properties or set()
    new_property_names = [p.name for p in new_properties]
    for old_property in channel.properties:
        if old_property.name not in new_property_names and old_property.name not in managed:
            new_properties = new_properties + [old_property]
    return new_properties


def get_current_time(timezone: Optional[str] = None) -> str:
    """Return the current time as a string, localised if a timezone is given."""
    if timezone:
        return str(datetime.datetime.now().astimezone())
    return str(datetime.datetime.now())
