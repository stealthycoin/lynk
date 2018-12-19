import mock

import lynk
from lynk.session import Session
from lynk.lock import Lock


class TestSession(object):
    def test_can_create_session_from_lynk(self):
        session = lynk.get_session('table_name')
        assert isinstance(session, Session)

    def test_can_create_session(self):
        session = Session('table_name')
        assert isinstance(session, Session)

    def test_can_create_lock(self):
        identifier = 'foobar'
        bridge_factory = mock.Mock()
        bridge_factory.create.return_value = (mock.Mock(), mock.Mock())
        session = Session(
            'table_name',
            host_identifier=identifier,
            backend_bridge_factory=bridge_factory,
        )
        lock = session.create_lock('foo')
        assert isinstance(lock, Lock)

    def test_can_create_lock_without_refresher(self):
        identifier = 'foobar'
        bridge_factory = mock.Mock()
        bridge_factory.create.return_value = (mock.Mock(), mock.Mock())
        session = Session(
            'table_name',
            host_identifier=identifier,
            backend_bridge_factory=bridge_factory,
        )
        lock = session.create_lock('foo', auto_refresh=False)
        assert isinstance(lock, Lock)
        # No other easy way to check this without a real backend. So for a
        # simple unit test we will reach into the private varaible to check.
        assert lock._refresher_factory is None
