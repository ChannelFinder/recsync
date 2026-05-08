import enum
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


class PVStatus(str, enum.Enum):
    """PV Status values."""

    ACTIVE = "Active"
    INACTIVE = "Inactive"


class CFPropertyName(str, enum.Enum):
    """Standard property names used in Channelfinder."""

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
    name: str
    owner: str
    value: Optional[str] = None

    def as_dict(self) -> Dict[str, str]:
        """Convert to dictionary for Channelfinder API."""
        return {"name": self.name, "owner": self.owner, "value": self.value or ""}

    @classmethod
    def from_dict(cls, prop_dict: Dict[str, str]) -> "CFProperty":
        return cls(
            name=prop_dict.get("name", ""),
            owner=prop_dict.get("owner", ""),
            value=prop_dict.get("value"),
        )

    @classmethod
    def record_type(cls, owner: str, record_type: str) -> "CFProperty":
        return cls(CFPropertyName.RECORD_TYPE.value, owner, record_type)

    @classmethod
    def alias(cls, owner: str, alias: str) -> "CFProperty":
        return cls(CFPropertyName.ALIAS.value, owner, alias)

    @classmethod
    def pv_status(cls, owner: str, pv_status: PVStatus) -> "CFProperty":
        return cls(CFPropertyName.PV_STATUS.value, owner, pv_status.value)

    @classmethod
    def active(cls, owner: str) -> "CFProperty":
        return cls.pv_status(owner, PVStatus.ACTIVE)

    @classmethod
    def inactive(cls, owner: str) -> "CFProperty":
        return cls.pv_status(owner, PVStatus.INACTIVE)

    @classmethod
    def time(cls, owner: str, time: str) -> "CFProperty":
        return cls(CFPropertyName.TIME.value, owner, time)


@dataclass
class CFChannel:
    """Representation of a Channelfinder channel."""

    name: str
    owner: str
    properties: List[CFProperty]

    def as_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for Channelfinder API."""
        return {
            "name": self.name,
            "owner": self.owner,
            "properties": [p.as_dict() for p in self.properties],
        }

    @classmethod
    def from_dict(cls, channel_dict: Dict[str, Any]) -> "CFChannel":
        return cls(
            name=channel_dict.get("name", ""),
            owner=channel_dict.get("owner", ""),
            properties=[CFProperty.from_dict(p) for p in channel_dict.get("properties", [])],
        )


@dataclass
class IocInfo:
    """Information about an IOC instance."""

    host: str
    hostname: str
    ioc_name: str
    ioc_ip: str
    owner: str
    time: str
    port: int
    channelcount: int = 0

    @property
    def ioc_id(self):
        return self.host + ":" + str(self.port)


@dataclass
class RecordInfo:
    """Information about a record to be stored in Channelfinder."""

    pv_name: str
    record_type: Optional[str] = None
    info_properties: List[CFProperty] = field(default_factory=list)
    aliases: List[str] = field(default_factory=list)


class IOCMissingInfoError(Exception):
    """Raised when an IOC is missing required information."""

    def __init__(self, ioc_info: IocInfo):
        super().__init__(f"Missing hostName {ioc_info.hostname} or iocName {ioc_info.ioc_name}")
        self.ioc_info = ioc_info
