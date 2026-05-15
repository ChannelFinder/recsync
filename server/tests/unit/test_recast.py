from unittest.mock import MagicMock

from twisted.internet import defer
from twisted.internet.address import IPv4Address

from recceiver.recast import CollectionSession


def _make_session() -> CollectionSession:
    ep = IPv4Address("TCP", "1.2.3.4", 5678)
    session = CollectionSession(MagicMock(), ep)
    session.factory = MagicMock()
    session.factory.commit.return_value = None
    return session


class TestCollectionSessionAbort:
    def test_disconnect_commit_runs_after_data_commit_cancels(self):
        session = _make_session()
        committed = []

        def mock_commit(t):
            committed.append(t)
            if t.connected:
                return defer.fail(defer.CancelledError("CF unavailable"))
            return None

        session.factory.commit.side_effect = mock_commit

        session.ioc_info("IOCNAME", "IOC1")
        session.flush()
        session.close()

        assert len(committed) == 2
        assert committed[-1].connected is False

    def test_disconnect_commit_runs_after_unexpected_commit_error(self):
        session = _make_session()
        committed = []

        def mock_commit(t):
            committed.append(t)
            if t.connected:
                return defer.fail(RuntimeError("unexpected"))
            return None

        session.factory.commit.side_effect = mock_commit

        session.ioc_info("IOCNAME", "IOC1")
        session.flush()
        session.close()

        assert len(committed) == 2
        assert committed[-1].connected is False
        session.proto.transport.loseConnection.assert_called_once()


class TestCollectionSessionFlush:
    def test_client_infos_carried_forward_after_intermediate_flush(self):
        session = _make_session()
        session.ioc_info("IOCNAME", "MY-IOC")
        session.ioc_info("HOSTNAME", "myhost")

        session.flush()

        assert session.transaction.client_infos == {"IOCNAME": "MY-IOC", "HOSTNAME": "myhost"}

    def test_replacement_transaction_is_not_initial(self):
        session = _make_session()
        session.ioc_info("IOCNAME", "MY-IOC")
        assert session.transaction.initial is True

        session.flush()

        assert session.transaction.initial is False

    def test_flush_triggered_by_trlimit_carries_forward_client_infos(self):
        session = _make_session()
        session.ioc_info("IOCNAME", "MY-IOC")
        session.trlimit = 2

        # add_record calls flush_safely, which triggers flush once trlimit is reached
        session.add_record(1, "ai", "PV:1")
        session.add_record(2, "ai", "PV:2")

        assert session.transaction.client_infos.get("IOCNAME") == "MY-IOC"
