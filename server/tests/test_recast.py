from unittest.mock import MagicMock

from twisted.internet import defer

from recceiver.recast import CollectionSession


def make_session() -> CollectionSession:
    proto = MagicMock()
    proto.transport = MagicMock()
    session = CollectionSession(proto, "test:1234")
    session.factory = MagicMock()
    return session


class TestFlushSafely:
    def test_trlimit_triggers_flush_when_reached(self):
        session = make_session()
        session.trlimit = 3
        session.flush = MagicMock()
        session.mark_dirty()
        for i in range(3):
            session.transaction.records_to_add[i] = (f"REC:{i}", "ai")
        session.flush_safely()
        session.flush.assert_called_once()

    def test_trlimit_does_not_flush_below_limit(self):
        session = make_session()
        session.trlimit = 3
        session.flush = MagicMock()
        session.mark_dirty()
        for i in range(2):
            session.transaction.records_to_add[i] = (f"REC:{i}", "ai")
        session.flush_safely()
        session.flush.assert_not_called()

    def test_trlimit_zero_never_triggers_flush(self):
        session = make_session()
        session.trlimit = 0
        session.flush = MagicMock()
        session.mark_dirty()
        for i in range(10000):
            session.transaction.records_to_add[i] = (f"REC:{i}", "ai")
        session.flush_safely()
        session.flush.assert_not_called()


class TestCollectionSessionClose:
    def test_close_does_not_cancel_pending_commit(self):
        session = make_session()
        pending = defer.Deferred()
        cancelled_errors = []
        pending.addErrback(lambda f: cancelled_errors.append(f.type) or f)
        session.C = pending
        session.close()
        assert cancelled_errors == [], "close() must not cancel a queued data commit"
