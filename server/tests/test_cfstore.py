from configparser import ConfigParser

from recceiver.cfstore import CFConfig
from recceiver.processors import ConfigAdapter


def make_adapter(section: str = "cf", values: dict = None, env: dict = None) -> ConfigAdapter:
    parser = ConfigParser()
    parser.add_section(section)
    for key, value in (values or {}).items():
        parser.set(section, key, str(value))
    adapter = ConfigAdapter(parser, section)
    if env:
        adapter.env_vars = env
    return adapter


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
        assert config.push_always_retry is True

    def test_alias_disabled_by_default(self):
        adapter = make_adapter()
        config = CFConfig.loads(adapter)
        assert config.alias_enabled is False

    def test_alias_enabled_from_config(self):
        adapter = make_adapter(values={"alias": "true"})
        config = CFConfig.loads(adapter)
        assert config.alias_enabled is True
