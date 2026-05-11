import enum
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


class PVStatus(enum.Enum):
    """Active/Inactive status values as used in the pvStatus CF property."""

    ACTIVE = "Active"
    INACTIVE = "Inactive"


class CFPropertyName(enum.Enum):
    """Canonical property names registered and managed in Channelfinder."""

    HOSTNAME = "hostName"
    IOC_NAME = "iocName"
    IOC_ID = "iocid"
    IOC_IP = "iocIP"
    PV_STATUS = "pvStatus"
    TIME = "time"
    RECCEIVER_ID = "recceiverID"
    ALIAS = "alias"
    RECORD_TYPE = "recordType"
    RECORD_DESC = "recordDesc"
    CA_PORT = "caPort"
    PVA_PORT = "pvaPort"


@dataclass
class CFProperty:
    """A single named property attached to a Channelfinder channel."""

    name: str
    owner: str
    value: Optional[str] = None

    def as_dict(self) -> Dict[str, str]:
        """Serialise to the dict shape expected by pyCFClient."""
        return {"name": self.name, "owner": self.owner, "value": self.value or ""}

    @classmethod
    def from_dict(cls, prop_dict: Dict[str, str]) -> "CFProperty":
        """Deserialise from the dict shape returned by pyCFClient."""
        return cls(
            name=prop_dict.get("name", ""),
            owner=prop_dict.get("owner", ""),
            value=prop_dict.get("value"),
        )


@dataclass
class CFChannel:
    """A Channelfinder channel with its associated properties."""

    name: str
    owner: str
    properties: List[CFProperty]

    def as_dict(self) -> Dict[str, Any]:
        """Serialise to the dict shape expected by pyCFClient."""
        return {
            "name": self.name,
            "owner": self.owner,
            "properties": [p.as_dict() for p in self.properties],
        }

    @classmethod
    def from_dict(cls, channel_dict: Dict[str, Any]) -> "CFChannel":
        """Deserialise from the dict shape returned by pyCFClient."""
        return cls(
            name=channel_dict.get("name", ""),
            owner=channel_dict.get("owner", ""),
            properties=[CFProperty.from_dict(p) for p in channel_dict.get("properties", [])],
        )


@dataclass
class IocInfo:
    """Runtime state for a connected IOC. The ioc_id property is the primary key."""

    host: str
    hostname: str
    ioc_name: str
    ioc_ip: str
    owner: str
    time: str
    port: int
    channelcount: int = 0

    @property
    def ioc_id(self) -> str:
        return f"{self.host}:{self.port}"


@dataclass
class RecordInfo:
    """Per-record data extracted from a transaction before pushing to CF."""

    pv_name: str
    record_type: Optional[str] = None
    info_properties: List[CFProperty] = field(default_factory=list)
    aliases: List[str] = field(default_factory=list)


class IOCMissingInfoError(Exception):
    """Raised when an IOC is missing required information."""

    def __init__(self, ioc_info: IocInfo):
        super().__init__(f"Missing hostName {ioc_info.hostname} or iocName {ioc_info.ioc_name}")
        self.ioc_info = ioc_info
