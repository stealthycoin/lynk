import json

import pytest
import mock

from lynk.lock import Lock
from lynk.techniques import BaseTechnique
from lynk.refresh import LockRefresherFactory
from lynk.refresh import LockRefresher
from lynk.exceptions import LockNotGrantedError


@pytest.fixture
def create_lock():
    def wrapped(name=None, technique=None, refresher=False):
        if name is None:
            name = 'lock name'
        if technique is None:
            technique = mock.Mock(spec=BaseTechnique)
        if refresher:
            refresh_factory = mock.Mock(spec=LockRefresherFactory)
        else:
            refresh_factory = None

        lock = Lock(name, technique, refresh_factory)
        return lock, technique, refresh_factory
    return wrapped


class TestLock(object):
    def test_can_serialize_lock(self, create_lock):
        lock, tech, _ = create_lock(name='foo')
        tech.serialize.return_value = 'SERIALIZED_TECHNIQUE'
        serial = json.loads(lock.serialize())

        assert serial == {
            '__version': 'Lock.1',
            'name': 'foo',
            'technique': 'SERIALIZED_TECHNIQUE',
        }

    def test_can_acquire_lock(self, create_lock):
        lock, tech, _ = create_lock()
        lock.acquire()
        tech.acquire.assert_called_with('lock name', 20, max_wait_seconds=300)

    def test_can_acquire_lock_with_custom_params(self, create_lock):
        lock, tech, _ = create_lock()
        lock.acquire(100, max_wait_seconds=10)
        tech.acquire.assert_called_with('lock name', 100, max_wait_seconds=10)

    def test_can_release_lock(self, create_lock):
        lock, tech, _ = create_lock()
        lock.release()
        tech.release.assert_called_with('lock name')

    def test_can_refresh_lock(self, create_lock):
        lock, tech, _ = create_lock()
        lock.refresh()
        tech.refresh.assert_called_with('lock name')

    def test_context_manager_does_acquire_and_release(self, create_lock):
        lock, tech, _ = create_lock()
        with lock():
            pass
        tech.acquire.assert_called_with('lock name', 20, max_wait_seconds=300)
        tech.release.assert_called_with('lock name')

    def test_lock_not_granted_does_escape_context_manager(self, create_lock):
        # The context manager swallows errors, its important that the
        # LockNotGrantedError escapes this otherwise it could be silenced and
        # the with block would exceute and operate on a resource protected by
        # the lock, even though the lock acquisition failed.
        # Also the release should not be called, since the acquire failed.
        lock, tech, _ = create_lock()
        tech.acquire.side_effect = LockNotGrantedError()
        with pytest.raises(LockNotGrantedError):
            with lock():
                pass
        tech.acquire.assert_called_with('lock name', 20, max_wait_seconds=300)
        tech.release.assert_not_called()

    def test_acquire_does_create_and_start_refresher(self, create_lock):
        lock, tech, refresher_factory = create_lock(refresher=True)
        mock_refresher = mock.Mock(spec=LockRefresher)
        refresher_factory.create_lock_refresher.return_value = mock_refresher
        lock.acquire()

        refresher_factory.create_lock_refresher.assert_called_with(
            lock,
            15,
        )
        mock_refresher.start.assert_called_once()

    def test_release_does_stop_refresher(self, create_lock):
        lock, tech, refresher_factory = create_lock(refresher=True)
        mock_refresher = mock.Mock(spec=LockRefresher)
        refresher_factory.create_lock_refresher.return_value = mock_refresher
        lock.acquire()
        lock.release()

        mock_refresher.stop.assert_called_once()
