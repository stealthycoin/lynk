import re

import pytest
import mock

from lynk.lock import Lock
from lynk.lock import Locker
from lynk.lock import LockFactory
from lynk.backends import LockBackend
from lynk.exceptions import LockNotGrantedError
from lynk.exceptions import LockAlreadyInUseError


class FakeTime(object):
    def __init__(self, times=None):
        if times is None:
            times = []
        self._times = times
        self.sleeps = []

    def time(self):
        if self._times:
            return self._times.pop(0)
        return 1

    def sleep(self, amt):
        self.sleeps.append(amt)


@pytest.fixture
def create_lock():
    def wrapped(locker=None):
        if locker is None:
            locker = mock.Mock(spec=Locker)
        lock = Lock(locker)
        return lock, locker
    return wrapped


@pytest.fixture
def create_locker():
    def wrapped(name, backend=None, agent=None, times=None):
        if backend is None:
            backend = mock.Mock(spec=LockBackend)
        fake_time = FakeTime(times)
        locker = Locker(name, backend, host_identifier=agent,
                        time_utils=fake_time)
        return locker, backend, fake_time
    return wrapped


class TestLock(object):
    def test_can_acquire_lock(self, create_lock):
        lock, locker = create_lock()
        lock.acquire()
        assert locker.acquire_lock.called is True

    def test_can_release_lock(self, create_lock):
        lock, locker = create_lock()
        lock.release()
        assert locker.delete_lock.called is True

    def test_can_refresh_lock(self, create_lock):
        lock, locker = create_lock()
        lock.refresh()
        assert locker.refresh_lock.called is True

    def test_context_manager_does_acquire_and_release(self, create_lock):
        lock, locker = create_lock()
        with lock():
            pass
        locker.acquire_lock.assert_called()
        locker.delete_lock.assert_called()


class TestLocker(object):
    UUID_PATTERN = re.compile(
        '^[0-9a-f]{8}-([0-9a-f]{4}-){3}[0-9a-f]{12}$',
        re.I
    )

    def test_can_get_lock_name(self, create_locker):
        locker, _, _ = create_locker('name')
        assert locker.lock_name == 'name'

    def test_does_make_agent_identifier_if_not_provided(self, create_locker):
        locker, _, _ = create_locker('name')
        assert self.UUID_PATTERN.match(locker.host_identifier) is not None

    def test_does_use_provided_agent_identifier(self, create_locker):
        locker, _, _ = create_locker('name', agent='foobar')
        assert locker.host_identifier == 'foobar'

    def test_can_delete_lock(self, create_locker):
        locker, backend, _ = create_locker('name')
        locker.delete_lock()
        backend.delete.assert_called_with('name', mock.ANY)

    def test_can_refresh_lock(self, create_locker):
        locker, backend, _ = create_locker('name')
        locker.refresh_lock()
        backend.refresh_lock.assert_called_with('name', mock.ANY, mock.ANY)

    def test_acquire_does_acquire_new_lock(self, create_locker):
        locker, backend, _ = create_locker('name')
        locker.acquire_lock(400)
        assert backend.try_write_new_lock.called is True
        # Only care about the first two arguments here, the name and lease time
        name, lease = backend.try_write_new_lock.call_args[0][:2]
        assert name == 'name'
        assert lease == 400

    def test_does_try_to_steal_lock_when_already_taken(self, create_locker):
        locker, backend, time = create_locker('name')
        prior_lock_lease = 100
        prior_lock_version = 'identifier'
        # Reject the initial lock request with a LockAlreadyInUseError
        backend.try_write_new_lock.side_effect = LockAlreadyInUseError(
            prior_lock_lease, prior_lock_version
        )
        locker.acquire_lock(400)

        # It should wait for whatever the lease duration was and then call
        # try_write_lock with the same parameters, as well as the expected
        # version number it got back from the lock error above.
        assert backend.try_write_lock.called is True
        call = backend.try_write_lock.call_args[0]
        name, lease = call[:2]
        expected_prior_lock = call[-1]
        assert name == 'name'
        assert lease == 400
        assert expected_prior_lock == prior_lock_version

        # Ensure that it slept once for the prior lock's full lease duration
        assert len(time.sleeps) == 1
        assert time.sleeps[0] == prior_lock_lease

    def test_does_try_to_steal_lock_repeatedly(self, create_locker):
        # This cannot be completed until the refersher is working and tested
        # locker, backend, time = create_locker('name')
        # prior_lock_lease = 100
        # prior_lock_version = 'identifier'
        # # Reject the first two lock requests with a LockAlreadyInUseError
        # error = LockAlreadyInUseError(prior_lock_lease, prior_lock_version)
        # backend.try_write_new_lock.side_effect = [error, error]
        # locker.acquire_lock(400)

        # # It should wait for whatever the lease duration was and then call
        # # try_write_lock with the same parameters, as well as the expected
        # # version number it got back from the lock error above.
        # assert backend.try_write_lock.called is True
        # print(backend.try_write_lock.call_count)
        # fdas
        # call = backend.try_write_lock.call_args[0]
        # name, lease = call[:2]
        # expected_prior_lock = call[-1]
        # assert name == 'name'
        # assert lease == 400
        # assert expected_prior_lock == prior_lock_version

        # # Ensure that it slept once for the prior lock's full lease duration
        # assert len(time.sleeps) == 1
        # assert time.sleeps[0] == prior_lock_lease
        pass

    def test_does_fail_when_timeout_over_boundry(self, create_locker):
        locker, backend, time = create_locker('name', times=[0, 300])
        prior_lock_lease = 100
        max_wait_seconds = 200
        prior_lock_version = 'identifier'

        # Reject the initial lock request with a LockAlreadyInUseError
        # The follow up should fails since the prior lock lease is longer
        # than our max_wait_seconds for this call
        backend.try_write_new_lock.side_effect = LockAlreadyInUseError(
            prior_lock_lease, prior_lock_version
        )
        with pytest.raises(LockNotGrantedError):
            locker.acquire_lock(400, max_wait_seconds=max_wait_seconds)

    def test_does_fail_when_timeout_on_boundry(self, create_locker):
        locker, backend, time = create_locker('name', times=[0, 200])
        prior_lock_lease = 100
        max_wait_seconds = 200
        prior_lock_version = 'identifier'

        # Reject the initial lock request with a LockAlreadyInUseError
        # The follow up should fails since the prior lock lease is longer
        # than our max_wait_seconds for this call
        backend.try_write_new_lock.side_effect = LockAlreadyInUseError(
            prior_lock_lease, prior_lock_version
        )
        with pytest.raises(LockNotGrantedError):
            locker.acquire_lock(400, max_wait_seconds=max_wait_seconds)


class TestLockFactory(object):
    def test_can_create_lock_factory(self):
        backend = mock.Mock(spec=LockBackend)
        factory = LockFactory(backend)
        assert isinstance(factory, LockFactory)

    def test_can_create_lock(self):
        identifier = 'foobar'
        backend = mock.Mock(spec=LockBackend)
        factory = LockFactory(backend, host_identifier=identifier)
        lock = factory.create_lock('foo')
        assert lock.name == 'foo'
        locker = lock._locker
        assert locker._host_identifier == identifier
