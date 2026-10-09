import pytest

from recceiver.cf.model import CFChannel, CFProperty, CFPropertyName, IOCInfo, PVStatus


class TestIOCInfo:
    def test_id_combines_host_and_port(self):
        ioc = IOCInfo(host="1.2.3.4", hostname="h", ioc_name="n", ioc_ip="1.2.3.4", owner="o", time="t", port=5064)
        assert ioc.id == "1.2.3.4:5064"


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


class TestMergePropertyLists:
    def test_new_property_replaces_old_property_by_name(self):
        channel = CFChannel("PV:1", "admin", [CFProperty("pvStatus", "old-owner", "Active")])
        updated = CFProperty("pvStatus", "new-owner", "Inactive")
        assert channel.merged_properties([updated], {"pvStatus"}) == [updated]

    def test_preserves_unmanaged_properties_in_order(self):
        custom = CFProperty("custom", "user", "value")
        other = CFProperty("other", "user", "another value")
        channel = CFChannel("PV:1", "admin", [custom, other])
        updated = CFProperty("pvStatus", "admin", "Active")
        assert channel.merged_properties([updated], {"pvStatus"}) == [updated, custom, other]

    def test_removes_omitted_managed_properties(self):
        custom = CFProperty("custom", "user", "value")
        channel = CFChannel("PV:1", "admin", [CFProperty("archive", "admin", "old value"), custom])
        updated = CFProperty("pvStatus", "admin", "Active")
        assert channel.merged_properties([updated], {"archive", "pvStatus"}) == [updated, custom]

    @pytest.mark.parametrize("managed", [None, set()])
    def test_empty_new_properties_preserve_existing_when_nothing_is_managed(self, managed):
        existing = CFProperty("custom", "user", "value")
        channel = CFChannel("PV:1", "admin", [existing])
        assert channel.merged_properties([], managed) == [existing]

    def test_empty_new_properties_remove_all_managed_properties(self):
        channel = CFChannel("PV:1", "admin", [CFProperty("archive", "admin", "old value")])
        assert channel.merged_properties([], {"archive"}) == []

    def test_does_not_mutate_either_input_list(self):
        existing = CFProperty("custom", "user", "value")
        channel = CFChannel("PV:1", "admin", [existing])
        updated = CFProperty("pvStatus", "admin", "Active")
        new_properties = [updated]
        assert channel.merged_properties(new_properties) == [updated, existing]
        assert new_properties == [updated]
        assert channel.properties == [existing]


class TestCFChannel:
    @pytest.mark.parametrize("name", [CFPropertyName.TIME, "time"])
    def test_property_returns_matching_object(self, name):
        expected = CFProperty("time", "user", "timestamp")
        channel = CFChannel("PV:1", "admin", [CFProperty("pvStatus", "admin", "Active"), expected])
        assert channel.property(name) is expected

    def test_property_accepts_custom_name_and_none_value(self):
        expected = CFProperty("custom", "user", None)
        channel = CFChannel("PV:1", "admin", [expected])
        assert channel.property("custom") is expected

    @pytest.mark.parametrize("properties", [[], [CFProperty("pvStatus", "admin", "Active")]])
    def test_property_returns_none_when_absent(self, properties):
        channel = CFChannel("PV:1", "admin", properties)
        assert channel.property(CFPropertyName.TIME) is None

    def test_property_returns_first_match(self):
        first = CFProperty("time", "admin", "first")
        channel = CFChannel("PV:1", "admin", [first, CFProperty("time", "admin", "second")])
        assert channel.property(CFPropertyName.TIME) is first

    @pytest.mark.parametrize("name", [CFPropertyName.TIME, "time"])
    @pytest.mark.parametrize("value", ["timestamp", None, ""])
    def test_has_property_accepts_enum_or_string_regardless_of_value(self, name, value):
        channel = CFChannel("PV:1", "admin", [CFProperty("time", "admin", value)])
        assert channel.has_property(name)

    def test_has_property_accepts_custom_property_name(self):
        channel = CFChannel("PV:1", "admin", [CFProperty("custom", "admin", "value")])
        assert channel.has_property("custom")

    @pytest.mark.parametrize("properties", [[], [CFProperty("pvStatus", "admin", "Active")]])
    def test_has_property_returns_false_when_name_absent(self, properties):
        channel = CFChannel("PV:1", "admin", properties)
        assert not channel.has_property(CFPropertyName.TIME)

    def test_has_property_matches_equal_property(self):
        channel = CFChannel("PV:1", "admin", [CFProperty("pvStatus", "admin", "Inactive")])
        assert channel.has_property(CFProperty("pvStatus", "admin", "Inactive"))

    @pytest.mark.parametrize(
        "prop",
        [
            CFProperty("other", "admin", "Inactive"),
            CFProperty("pvStatus", "other", "Inactive"),
            CFProperty("pvStatus", "admin", "Active"),
        ],
    )
    def test_has_property_requires_matching_name_owner_and_value(self, prop):
        channel = CFChannel("PV:1", "admin", [CFProperty("pvStatus", "admin", "Inactive")])
        assert not channel.has_property(prop)

    def test_has_property_returns_false_for_exact_match_on_empty_channel(self):
        channel = CFChannel("PV:1", "admin", [])
        assert not channel.has_property(CFProperty("pvStatus", "admin", "Inactive"))

    @pytest.mark.parametrize("name", [CFPropertyName.TIME, "time"])
    def test_property_value_accepts_enum_or_string(self, name):
        channel = CFChannel(
            "PV:1",
            "admin",
            [CFProperty("pvStatus", "admin", "Active"), CFProperty("time", "admin", "timestamp")],
        )
        assert channel.property_value(name) == "timestamp"

    def test_property_value_accepts_custom_property(self):
        channel = CFChannel("PV:1", "admin", [CFProperty("custom", "admin", "custom value")])
        assert channel.property_value("custom") == "custom value"

    @pytest.mark.parametrize("properties", [[], [CFProperty("pvStatus", "admin", "Active")]])
    def test_property_value_returns_none_when_absent(self, properties):
        channel = CFChannel("PV:1", "admin", properties)
        assert channel.property_value(CFPropertyName.TIME) is None

    @pytest.mark.parametrize("value", [None, ""])
    def test_property_value_preserves_none_or_empty_value(self, value):
        channel = CFChannel("PV:1", "admin", [CFProperty("time", "admin", value)])
        assert channel.property_value(CFPropertyName.TIME) == value

    def test_property_value_returns_first_match(self):
        channel = CFChannel(
            "PV:1", "admin", [CFProperty("time", "admin", "first"), CFProperty("time", "admin", "second")]
        )
        assert channel.property_value(CFPropertyName.TIME) == "first"

    def test_from_dict_roundtrip(self):
        ch = CFChannel(
            name="PV:1",
            owner="admin",
            properties=[CFProperty(CFPropertyName.PV_STATUS.value, "admin", PVStatus.ACTIVE.value)],
        )
        assert CFChannel.from_dict(ch.as_dict()) == ch

    def test_from_dict_missing_properties_defaults_to_empty(self):
        ch = CFChannel.from_dict({"name": "PV:1", "owner": "admin"})
        assert ch.properties == []
