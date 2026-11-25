import logging
from typing import List

try:
    from typing import Protocol
except ImportError:
    from typing_extensions import Protocol  # type: ignore[assignment]

from recceiver.cf.model import CFChannel, CFProperty, CFPropertyName, PVStatus

log = logging.getLogger(__name__)

# CF query URLs break above this length; names are pipe-joined and chunked to stay under it.
_CF_NAME_QUERY_LIMIT = 600


class ChannelFinderAdapter(Protocol):
    """Typed boundary between CFProcessor and the ChannelFinder HTTP client.

    All methods accept and return domain objects (CFChannel, CFProperty).
    Dict serialisation is handled inside the implementation, not at callsites.
    """

    def find_by_ioc_id(self, iocid: str) -> List[CFChannel]:
        """Return channels registered under the given IOC ID, at most one query's worth."""
        ...

    def find_all_by_ioc_id(self, iocid: str) -> List[CFChannel]:
        """Return channels registered under the given IOC ID, paging past the size limit."""
        ...

    def find_by_names(self, names: List[str]) -> List[CFChannel]:
        """Return channels whose names are in the given list."""
        ...

    def find_active_for_recceiver(self, recceiverid: str) -> List[CFChannel]:
        """Return all channels marked Active for the given recceiver."""
        ...

    def set_channels(self, channels: List[CFChannel]) -> None:
        """Create or overwrite channels."""
        ...

    def update_property(self, prop: CFProperty, channel_names: List[str]) -> None:
        """Update a single property value across the named channels."""
        ...

    def get_property_names(self) -> List[str]:
        """Return the names of all property definitions registered in ChannelFinder."""
        ...

    def set_property(self, name: str, owner: str) -> None:
        """Register a property definition if it does not already exist."""
        ...


class PyCFClientAdapter:
    """Wraps pyCFClient's ChannelFinderClient to implement ChannelFinderAdapter."""

    def __init__(self, client, size_limit: int = 0):
        self._client = client
        self._size_limit = size_limit

    def _find(self, args: List) -> List[CFChannel]:
        if self._size_limit > 0:
            args = args + [("~size", self._size_limit)]
        return [CFChannel.from_dict(ch) for ch in self._client.findByArgs(args)]

    def find_by_ioc_id(self, iocid: str) -> List[CFChannel]:
        return self._find([(CFPropertyName.IOC_ID.value, iocid)])

    def find_all_by_ioc_id(self, iocid: str) -> List[CFChannel]:
        """Page through the `iocid` query with `~search_after`. Needs ChannelFinder 4.7.0 or newer.

        ChannelFinder sorts results by name and `~search_after` starts a page after the given name.
        A page is requested while the previous one came back full. With a size limit of 0 the single
        query carries no `~size`, so ChannelFinder applies its own cap.
        """
        args = [(CFPropertyName.IOC_ID.value, iocid)]
        channels = self._find(args)
        page = channels
        while self._size_limit > 0 and len(page) >= self._size_limit:
            last_name = page[-1].name
            page = self._find(args + [("~search_after", last_name)])
            if page and (page[0].name <= last_name or page[-1].name <= last_name):
                log.warning(
                    "CF did not advance past %s for iocid %s; keeping the first %d channels",
                    last_name,
                    iocid,
                    len(channels),
                )
                break
            channels.extend(page)
        return channels

    def find_by_names(self, names: List[str]) -> List[CFChannel]:
        if not names:
            return []
        chunks, buf = [], ""
        for name in names:
            if not buf:
                buf = name
            elif len(buf) + len(name) < _CF_NAME_QUERY_LIMIT:
                buf = buf + "|" + name
            else:
                chunks.append(buf)
                buf = name
        if buf:
            chunks.append(buf)
        results = []
        for chunk in chunks:
            results.extend(self._find([("~name", chunk)]))
        return results

    def find_active_for_recceiver(self, recceiverid: str) -> List[CFChannel]:
        return self._find(
            [
                (CFPropertyName.PV_STATUS.value, PVStatus.ACTIVE.value),
                (CFPropertyName.RECCEIVER_ID.value, recceiverid),
            ]
        )

    def set_channels(self, channels: List[CFChannel]) -> None:
        self._client.set(channels=[ch.as_dict() for ch in channels])

    def update_property(self, prop: CFProperty, channel_names: List[str]) -> None:
        self._client.update(property=prop.as_dict(), channelNames=channel_names)

    def get_property_names(self) -> List[str]:
        return [p["name"] for p in self._client.getAllProperties()]

    def set_property(self, name: str, owner: str) -> None:
        self._client.set(property={"name": name, "owner": owner})
