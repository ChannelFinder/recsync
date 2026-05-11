from typing import Any, Dict, List

try:
    from typing import Protocol
except ImportError:
    from typing_extensions import Protocol  # type: ignore[assignment]

from recceiver.cf.model import CFChannel, CFProperty, CFPropertyName, PVStatus


class ChannelFinderAdapter(Protocol):
    """Typed boundary between CFProcessor and the ChannelFinder HTTP client.

    All methods accept and return domain objects (CFChannel, CFProperty).
    Dict serialisation is handled inside the implementation, not at callsites.
    """

    def find_by_ioc_id(self, iocid: str) -> List[CFChannel]:
        """Return all channels registered under the given IOC ID."""
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

    def get_all_properties(self) -> List[Dict[str, Any]]:
        """Return all property definitions registered in ChannelFinder."""
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

    def find_by_names(self, names: List[str]) -> List[CFChannel]:
        if not names:
            return []
        chunks, buf = [], ""
        for name in names:
            if not buf:
                buf = name
            elif len(buf) + len(name) < 600:
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

    def get_all_properties(self) -> List[Dict[str, Any]]:
        return self._client.getAllProperties()

    def set_property(self, name: str, owner: str) -> None:
        self._client.set(property={"name": name, "owner": owner})
