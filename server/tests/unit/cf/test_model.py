from recceiver.cf.model import CFChannel, CFProperty, CFPropertyName, IocInfo, PVStatus


class TestIocInfo:
    def test_ioc_id_combines_host_and_port(self):
        ioc = IocInfo(host="1.2.3.4", hostname="h", ioc_name="n", ioc_ip="1.2.3.4", owner="o", time="t", port=5064)
        assert ioc.ioc_id == "1.2.3.4:5064"


class TestPVStatus:
    def test_active_value(self):
        assert PVStatus.ACTIVE.value == "Active"

    def test_inactive_value(self):
        assert PVStatus.INACTIVE.value == "Inactive"


class TestCFPropertyName:
    def test_ioc_id_value(self):
        assert CFPropertyName.IOC_ID.value == "iocid"

    def test_pv_status_value(self):
        assert CFPropertyName.PV_STATUS.value == "pvStatus"


class TestCFProperty:
    def test_as_dict_includes_all_fields(self):
        p = CFProperty(name="hostName", owner="admin", value="ioc1")
        d = p.as_dict()
        assert d == {"name": "hostName", "owner": "admin", "value": "ioc1"}

    def test_as_dict_empty_value_becomes_empty_string(self):
        p = CFProperty(name="hostName", owner="admin", value=None)
        assert p.as_dict()["value"] == ""

    def test_from_dict_roundtrip(self):
        original = CFProperty(name="pvStatus", owner="cf", value="Active")
        assert CFProperty.from_dict(original.as_dict()) == original

    def test_active_factory(self):
        p = CFProperty.active("owner")
        assert p.name == CFPropertyName.PV_STATUS.value
        assert p.value == PVStatus.ACTIVE.value

    def test_inactive_factory(self):
        p = CFProperty.inactive("owner")
        assert p.value == PVStatus.INACTIVE.value

    def test_alias_factory(self):
        p = CFProperty.alias("owner", "MY:ALIAS")
        assert p.name == CFPropertyName.ALIAS.value
        assert p.value == "MY:ALIAS"


class TestCFChannel:
    def test_from_dict_roundtrip(self):
        ch = CFChannel(
            name="PV:1",
            owner="admin",
            properties=[CFProperty.active("admin")],
        )
        assert CFChannel.from_dict(ch.as_dict()) == ch

    def test_from_dict_missing_properties_defaults_to_empty(self):
        ch = CFChannel.from_dict({"name": "PV:1", "owner": "admin"})
        assert ch.properties == []
