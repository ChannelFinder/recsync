from unittest.mock import MagicMock, call

from recceiver.cf.adapter import PyCFClientAdapter
from recceiver.cf.model import CFPropertyName
from tests.unit.cf.conftest import make_ioc

_BY_IOCID = (CFPropertyName.IOC_ID.value, make_ioc().id)


def _page(*names):
    return [{"name": name, "owner": "admin", "properties": []} for name in names]


def _cursors(client):
    return [dict(c.args[0]).get("~search_after") for c in client.findByArgs.call_args_list]


class TestFindByIocId:
    def test_makes_one_capped_query(self):
        client = MagicMock()
        client.findByArgs.side_effect = [_page("A", "B")]

        channels = PyCFClientAdapter(client, size_limit=2).find_by_ioc_id(make_ioc().id)

        assert [ch.name for ch in channels] == ["A", "B"]
        assert _cursors(client) == [None]

    def test_follows_pages_until_short_page(self):
        client = MagicMock()
        client.findByArgs.side_effect = [_page("A", "B"), _page("C", "D"), _page("E")]

        channels = PyCFClientAdapter(client, size_limit=2).find_all_by_ioc_id(make_ioc().id)

        assert [ch.name for ch in channels] == ["A", "B", "C", "D", "E"]
        assert _cursors(client) == [None, "B", "D"]

    def test_stops_on_empty_page(self):
        client = MagicMock()
        client.findByArgs.side_effect = [_page("A", "B"), _page()]

        channels = PyCFClientAdapter(client, size_limit=2).find_all_by_ioc_id(make_ioc().id)

        assert [ch.name for ch in channels] == ["A", "B"]
        assert _cursors(client) == [None, "B"]

    def test_stops_when_cursor_is_ignored(self):
        client = MagicMock()
        client.findByArgs.side_effect = [_page("A", "B"), _page("A", "B")]

        channels = PyCFClientAdapter(client, size_limit=2).find_all_by_ioc_id(make_ioc().id)

        assert [ch.name for ch in channels] == ["A", "B"]
        assert _cursors(client) == [None, "B"]

    def test_stops_when_cursor_is_inclusive(self):
        client = MagicMock()
        client.findByArgs.side_effect = [_page("A", "B"), _page("B", "C")]

        channels = PyCFClientAdapter(client, size_limit=2).find_all_by_ioc_id(make_ioc().id)

        assert [ch.name for ch in channels] == ["A", "B"]
        assert _cursors(client) == [None, "B"]

    def test_stops_when_page_is_unsorted(self):
        client = MagicMock()
        client.findByArgs.side_effect = [_page("A", "C"), _page("D", "B")]

        channels = PyCFClientAdapter(client, size_limit=2).find_all_by_ioc_id(make_ioc().id)

        assert [ch.name for ch in channels] == ["A", "C"]
        assert _cursors(client) == [None, "C"]

    def test_makes_one_query_without_size_when_limit_is_zero(self):
        # Without `~size` ChannelFinder applies its own cap, and there is no page size to compare against.
        client = MagicMock()
        client.findByArgs.side_effect = [_page("A", "B")]

        channels = PyCFClientAdapter(client).find_all_by_ioc_id(make_ioc().id)

        assert [ch.name for ch in channels] == ["A", "B"]
        assert client.findByArgs.call_args_list == [call([_BY_IOCID])]
