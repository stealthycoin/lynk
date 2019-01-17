import json

import pytest
import mock

import lynk
from lynk.session import Session
from lynk.lock import Lock
from lynk.exceptions import CannotDeserializeError


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

    def test_can_deserialize_lock(self):
        bridge_factory = mock.Mock()
        mock_bridge = mock.Mock()
        mock_backend = mock.Mock()
        bridge_factory.create.return_value = (mock_bridge, mock_backend)
        session = Session(
            'table_name',
            backend_bridge_factory=bridge_factory,
        )
        serialized_lock = json.dumps({
            '__version': 'Lock.1',
            'name': 'foo',
            'technique': (
                '{"__version": "VersionLeaseTechinque.1", '
                '"versions": {"foo": "version-identifier"}}'
            ),
        })
        lock = session.deserialize_lock(serialized_lock)
        assert isinstance(lock, Lock)

        # Ensure that the lock tried to refresh itself asap after being
        # deserialized since we don't know how much time has gone by.
        mock_bridge.we_own_lock.assert_called_with('version-identifier')
        mock_backend.update.assert_called_with(
            {'lockKey': 'foo'},
            condition=mock.ANY,
            updates=mock.ANY,
        )

    def test_can_deserialize_lock_without_auto_refresher(self):
        bridge_factory = mock.Mock()
        mock_bridge = mock.Mock()
        mock_backend = mock.Mock()
        bridge_factory.create.return_value = (mock_bridge, mock_backend)
        session = Session(
            'table_name',
            backend_bridge_factory=bridge_factory,
        )
        serialized_lock = json.dumps({
            '__version': 'Lock.1',
            'name': 'foo',
            'technique': (
                '{"__version": "VersionLeaseTechinque.1", '
                '"versions": {"foo": "version-identifier"}}'
            ),
        })
        lock = session.deserialize_lock(serialized_lock, auto_refresh=False)
        assert isinstance(lock, Lock)

        mock_bridge.we_own_lock.assert_called_with('version-identifier')
        mock_backend.update.assert_called_with(
            {'lockKey': 'foo'},
            condition=mock.ANY,
            updates=mock.ANY,
        )

    def test_does_raise_on_invalid_lock(self):
        bridge_factory = mock.Mock()
        mock_bridge = mock.Mock()
        mock_backend = mock.Mock()
        bridge_factory.create.return_value = (mock_bridge, mock_backend)
        session = Session(
            'table_name',
            backend_bridge_factory=bridge_factory,
        )
        serialized_lock = json.dumps({})

        with pytest.raises(CannotDeserializeError):
            session.deserialize_lock(serialized_lock, auto_refresh=False)

    def test_does_raise_on_wrong_serialized_version(self):
        bridge_factory = mock.Mock()
        mock_bridge = mock.Mock()
        mock_backend = mock.Mock()
        bridge_factory.create.return_value = (mock_bridge, mock_backend)
        session = Session(
            'table_name',
            backend_bridge_factory=bridge_factory,
        )
        serialized_lock = json.dumps({
            '__version': 'Lock.2',
        })

        with pytest.raises(CannotDeserializeError):
            session.deserialize_lock(serialized_lock, auto_refresh=False)

    def test_does_raise_on_missing_name(self):
        bridge_factory = mock.Mock()
        mock_bridge = mock.Mock()
        mock_backend = mock.Mock()
        bridge_factory.create.return_value = (mock_bridge, mock_backend)
        session = Session(
            'table_name',
            backend_bridge_factory=bridge_factory,
        )
        serialized_lock = json.dumps({
            '__version': 'Lock.1',
            'name': 'Some name',
        })

        with pytest.raises(CannotDeserializeError):
            session.deserialize_lock(serialized_lock, auto_refresh=False)

    def test_does_raise_on_missing_technique(self):
        bridge_factory = mock.Mock()
        mock_bridge = mock.Mock()
        mock_backend = mock.Mock()
        bridge_factory.create.return_value = (mock_bridge, mock_backend)
        session = Session(
            'table_name',
            backend_bridge_factory=bridge_factory,
        )
        serialized_lock = json.dumps({
            '__version': 'Lock.1',
            'technique': '...',
        })

        with pytest.raises(CannotDeserializeError):
            session.deserialize_lock(serialized_lock, auto_refresh=False)
