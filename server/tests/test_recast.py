from unittest.mock import MagicMock

from twisted.internet import defer

from recceiver.recast import CollectionSession


def make_session() -> CollectionSession:
    proto = MagicMock()
    proto.transport = MagicMock()
    session = CollectionSession(proto, "test:1234")
    session.factory = MagicMock()
    return session


class TestCollectionSessionClose:
    def test_close_does_not_cancel_pending_commit(self):
        session = make_session()
        pending = defer.Deferred()
        cancelled_errors = []
        pending.addErrback(lambda f: cancelled_errors.append(f.type) or f)
        session.C = pending
        session.close()
        assert cancelled_errors == [], "close() must not cancel a queued data commit"
