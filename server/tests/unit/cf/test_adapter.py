from unittest.mock import MagicMock

from recceiver.cf.adapter import PyCFClientAdapter
from tests.unit.cf.conftest import make_ioc


class TestPyCFClientAdapter:
    def test_find_returns_empty_when_client_returns_none(self):
        client = MagicMock()
        client.findByArgs.return_value = None  # pyCFClient's result on a 404

        assert PyCFClientAdapter(client).find_by_ioc_id(make_ioc().id) == []

    def test_get_property_names_returns_empty_when_client_returns_none(self):
        client = MagicMock()
        client.getAllProperties.return_value = None  # pyCFClient's result on a 404

        assert PyCFClientAdapter(client).get_property_names() == []
