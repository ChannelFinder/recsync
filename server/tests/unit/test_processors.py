import textwrap
from configparser import ConfigParser
from pathlib import Path

from recceiver.cf import CFProcessor
from recceiver.processors import ConfigAdapter, ProcessorController


def make_adapter(section: str = "cf", values: dict = None, env: dict = None) -> ConfigAdapter:
    parser = ConfigParser()
    parser.add_section(section)
    for key, value in (values or {}).items():
        parser.set(section, key, str(value))
    adapter = ConfigAdapter(parser, section)
    if env:
        adapter.env_vars = env
    return adapter


class TestConfigAdapterGet:
    def test_returns_value(self):
        adapter = make_adapter(values={"baseurl": "https://cf:8080"})
        assert adapter.get("baseurl") == "https://cf:8080"

    def test_returns_default_when_missing(self):
        adapter = make_adapter()
        assert adapter.get("baseurl", "fallback") == "fallback"

    def test_returns_none_when_missing_and_no_default(self):
        adapter = make_adapter()
        assert adapter.get("baseurl") is None

    def test_env_var_overrides_config(self):
        adapter = make_adapter(values={"baseurl": "https://original"}, env={"baseurl": "https://override"})
        assert adapter.get("baseurl") == "https://override"

    def test_env_var_provides_value_not_in_config(self):
        adapter = make_adapter(env={"baseurl": "https://from-env"})
        assert adapter.get("baseurl") == "https://from-env"


class TestConfigAdapterGetBoolean:
    def test_returns_true(self):
        adapter = make_adapter(values={"alias": "true"})
        assert adapter.getboolean("alias") is True

    def test_returns_false(self):
        adapter = make_adapter(values={"alias": "false"})
        assert adapter.getboolean("alias") is False

    def test_returns_default_when_missing(self):
        adapter = make_adapter()
        assert adapter.getboolean("alias", False) is False

    def test_returns_default_on_invalid_value(self):
        adapter = make_adapter(values={"alias": "notabool"})
        assert adapter.getboolean("alias", True) is True


class TestConfigAdapterGetInt:
    def test_returns_int(self):
        adapter = make_adapter(values={"pushmaxretries": "5"})
        assert adapter.getint("pushmaxretries") == 5

    def test_returns_default_when_missing(self):
        adapter = make_adapter()
        assert adapter.getint("pushmaxretries", 10) == 10

    def test_returns_none_when_missing_and_no_default(self):
        adapter = make_adapter()
        assert adapter.getint("pushmaxretries") is None

    def test_returns_default_on_invalid_value(self):
        adapter = make_adapter(values={"pushmaxretries": "notanint"})
        assert adapter.getint("pushmaxretries", 10) == 10

    def test_env_var_overrides_config(self):
        adapter = make_adapter(values={"pushmaxretries": "5"}, env={"pushmaxretries": "99"})
        assert adapter.getint("pushmaxretries", 10) == 99


class TestProcessorControllerFromConfig:
    def test_cf_processor_initialized_from_config_file(self, tmp_path: Path):
        config_file = tmp_path / "recceiver.conf"
        config_file.write_text(
            textwrap.dedent(
                """\
                [recceiver]
                procs = cf

                [cf]
                pushMaxRetries = 5
                """
            )
        )
        ctrl = ProcessorController(cfile=str(config_file))
        assert len(ctrl.procs) == 1
        assert isinstance(ctrl.procs[0], CFProcessor)
        assert ctrl.procs[0].cf_config.push_max_retries == 5
