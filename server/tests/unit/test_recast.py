from unittest.mock import MagicMock

from twisted.internet import defer
from twisted.internet.address import IPv4Address

from recceiver.recast import CastFactory, CollectionSession


def _make_session() -> CollectionSession:
    ep = IPv4Address("TCP", "1.2.3.4", 5678)
    session = CollectionSession(MagicMock(), ep)
    session.factory = MagicMock()
    session.factory.commit.return_value = None
    return session


class TestCastFactoryThrottling:
    def _make_factory(self, max_active=1):
        factory = CastFactory()
        factory.maxActive = max_active
        factory.protocol = lambda active=True: MagicMock(active=active)
        return factory

    def test_connections_beyond_maxactive_are_queued(self):
        factory = self._make_factory(max_active=1)
        p1 = factory.buildProtocol(None)
        p2 = factory.buildProtocol(None)
        p3 = factory.buildProtocol(None)

        assert factory.NActive == 1
        assert len(factory.Wait) == 2
        assert p1.active is True
        assert p2.active is False
        assert p3.active is False

    def test_queued_connection_activated_when_active_completes(self):
        factory = self._make_factory(max_active=1)
        p1 = factory.buildProtocol(None)
        p2 = factory.buildProtocol(None)

        factory.isDone(p1, active=True)

        assert factory.NActive == 1
        assert len(factory.Wait) == 0
        assert p2.active is True
        p2.transport.resumeProducing.assert_called_once()
        p2.connectionMade.assert_called_once()

    def test_nactive_decremented_when_active_completes_with_no_waiters(self):
        factory = self._make_factory(max_active=2)
        p1 = factory.buildProtocol(None)

        factory.isDone(p1, active=True)

        assert factory.NActive == 0
        assert len(factory.Wait) == 0

    def test_connection_closed_before_activation_removed_from_wait(self):
        factory = self._make_factory(max_active=1)
        factory.buildProtocol(None)
        p2 = factory.buildProtocol(None)

        factory.isDone(p2, active=False)

        assert len(factory.Wait) == 0
        assert factory.NActive == 1

    def test_fifo_order_preserved_across_multiple_waiters(self):
        factory = self._make_factory(max_active=1)
        p1 = factory.buildProtocol(None)
        p2 = factory.buildProtocol(None)
        p3 = factory.buildProtocol(None)

        factory.isDone(p1, active=True)

        assert p2.active is True
        p2.connectionMade.assert_called_once()
        assert len(factory.Wait) == 1
        assert factory.Wait[0] is p3


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
