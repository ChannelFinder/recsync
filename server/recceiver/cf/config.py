import socket
from dataclasses import dataclass, fields
from typing import Optional

from recceiver.processors import ConfigAdapter

RECCEIVERID_DEFAULT = socket.gethostname()
DEFAULT_QUERY_LIMIT = 10_000


@dataclass
class CFConfig:
    """Configuration options for the CF Processor."""

    alias_enabled: bool = False
    record_type_enabled: bool = False
    environment_variables: str = ""
    info_tags: str = ""
    ioc_connection_info: bool = True
    record_description_enabled: bool = False
    clean_on_start: bool = True
    clean_on_stop: bool = True
    username: str = "cfstore"
    env_owner_variable: str = "ENGINEER"
    recceiver_id: str = RECCEIVERID_DEFAULT
    timezone: Optional[str] = None
    cf_query_limit: int = DEFAULT_QUERY_LIMIT
    base_url: Optional[str] = None
    cf_username: Optional[str] = None
    cf_password: Optional[str] = None
    verify_ssl: Optional[bool] = None
    push_max_retries: int = 10
    push_always_retry: bool = False
    status_interval: float = 60.0

    @classmethod
    def loads(cls, conf: ConfigAdapter) -> "CFConfig":
        """Load configuration from a ConfigAdapter instance."""
        return CFConfig(
            alias_enabled=conf.getboolean("alias", False),
            record_type_enabled=conf.getboolean("recordType", False),
            environment_variables=conf.get("environment_vars", ""),
            info_tags=conf.get("infotags", ""),
            ioc_connection_info=conf.getboolean("iocConnectionInfo", True),
            record_description_enabled=conf.getboolean("recordDesc", False),
            clean_on_start=conf.getboolean("cleanOnStart", True),
            clean_on_stop=conf.getboolean("cleanOnStop", True),
            username=conf.get("username", "cfstore"),
            recceiver_id=conf.get("recceiverId", RECCEIVERID_DEFAULT),
            timezone=conf.get("timezone", ""),
            cf_query_limit=conf.get("findSizeLimit", DEFAULT_QUERY_LIMIT),
            base_url=conf.get("baseUrl"),
            cf_username=conf.get("cfUsername"),
            cf_password=conf.get("cfPassword"),
            verify_ssl=conf.getboolean("verifySSL"),
            push_max_retries=conf.getint("pushMaxRetries", 10),
            push_always_retry=conf.getboolean("pushAlwaysRetry", False),
            status_interval=float(conf.get("statusInterval", "60.0")),
        )

    def __repr__(self) -> str:
        parts = []
        for f in fields(self):
            value = getattr(self, f.name)
            if f.name == "cf_password":
                value = "***" if value else None
            parts.append(f"{f.name}={value!r}")
        return f"CFConfig({', '.join(parts)})"
