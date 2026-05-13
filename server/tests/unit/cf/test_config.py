import pytest

from recceiver.cf.config import CFConfig
from tests.unit.conftest import make_adapter


class TestCFConfigLoads:
    def test_loads_defaults_without_error(self):
        adapter = make_adapter()
        config = CFConfig.loads(adapter)
        assert isinstance(config, CFConfig)

    def test_default_push_max_retries(self):
        adapter = make_adapter()
        config = CFConfig.loads(adapter)
        assert config.push_max_retries == 10

    def test_push_max_retries_from_config(self):
        adapter = make_adapter(values={"pushmaxretries": "3"})
        config = CFConfig.loads(adapter)
        assert config.push_max_retries == 3

    def test_push_max_retries_from_env(self):
        adapter = make_adapter(env={"pushmaxretries": "7"})
        config = CFConfig.loads(adapter)
        assert config.push_max_retries == 7

    def test_default_push_always_retry(self):
        adapter = make_adapter()
        config = CFConfig.loads(adapter)
        assert config.push_always_retry is False

    def test_default_env_owner_variable(self):
        adapter = make_adapter()
        config = CFConfig.loads(adapter)
        assert config.env_owner_variable == "ENGINEER"

    def test_env_owner_variable_from_config(self):
        adapter = make_adapter(values={"envownervariable": "RESPONSIBLE_ENGINEER"})
        config = CFConfig.loads(adapter)
        assert config.env_owner_variable == "RESPONSIBLE_ENGINEER"

    def test_alias_disabled_by_default(self):
        adapter = make_adapter()
        config = CFConfig.loads(adapter)
        assert config.alias_enabled is False

    def test_alias_enabled_from_config(self):
        adapter = make_adapter(values={"alias": "true"})
        config = CFConfig.loads(adapter)
        assert config.alias_enabled is True

    def test_default_status_interval(self):
        adapter = make_adapter()
        config = CFConfig.loads(adapter)
        assert config.status_interval == pytest.approx(60.0)

    def test_status_interval_from_config(self):
        adapter = make_adapter(values={"statusinterval": "120.0"})
        config = CFConfig.loads(adapter)
        assert config.status_interval == pytest.approx(120.0)
