import json

import pytest
import mock

from lynk.techniques import VersionLeaseTechinque
from lynk.exceptions import NoSuchLockError
from lynk.exceptions import LockLostError
from lynk.exceptions import LockNotGrantedError
from lynk.exceptions import CannotDeserializeError
from lynk.backends.base import BaseBackend
from lynk.backends.dynamodb import DynamoDBVersionLeaseBridge


class ConditionFailedError(Exception):
    pass


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
def version_lease_factory():
    def wrapped(bridge=None, backend=None, host=None, times=None):
        if bridge is None:
            bridge = mock.Mock(spec=DynamoDBVersionLeaseBridge)
            bridge.ConditionFailedError = ConditionFailedError
            bridge.NoSuchLockError = NoSuchLockError
        if backend is None:
            backend = mock.Mock(spec=BaseBackend)
        fake_time = FakeTime(times)
        vlt = VersionLeaseTechinque(bridge, backend, host_identifier=host,
                                    time_utils=fake_time)
        return vlt, bridge, backend, fake_time
    return wrapped


class TestVersionLeaseTechnique(object):
    def test_can_serialize_technique(self, version_lease_factory):
        vlt, bridge, backend, _ = version_lease_factory()
        vlt.acquire('lock name', 5, 200)
        version = backend.put.call_args_list[0][0][0]['versionNumber']
        serial = json.loads(vlt.serialize())

        assert '__version' in serial
        assert serial['__version'] == 'VersionLeaseTechinque.1'
        assert 'versions' in serial
        assert serial['versions'] == {
            'lock name': version,
        }

    def test_can_deserialize_technique(self, version_lease_factory):
        _, bridge, backend, _ = version_lease_factory()
        serialized = json.dumps({
            '__version': 'VersionLeaseTechinque.1',
            'versions': {
                'test-lock': 'version-identifier',
            },
        })
        vlt = VersionLeaseTechinque.from_serialized_technique(
            serialized, bridge, backend)
        assert isinstance(vlt, VersionLeaseTechinque)
        vlt.refresh('test-lock')
        bridge.we_own_lock.assert_called_with('version-identifier')

    def test_does_raise_on_invalid_lock(self, version_lease_factory):
        _, bridge, backend, _ = version_lease_factory()
        serialized = json.dumps({})
        with pytest.raises(CannotDeserializeError):
            VersionLeaseTechinque.from_serialized_technique(
                serialized, bridge, backend)

    def test_does_raise_on_wrong_serialized_version(
            self, version_lease_factory):
        _, bridge, backend, _ = version_lease_factory()
        serialized = json.dumps({
            '__version': 'VersionLeaseTechinque.2',
        })
        with pytest.raises(CannotDeserializeError):
            VersionLeaseTechinque.from_serialized_technique(
                serialized, bridge, backend)

    def test_can_acquire_new_lock(self, version_lease_factory):
        vlt, bridge, backend, _ = version_lease_factory()
        vlt.acquire('lock name', 5, 200)
        backend.put.assert_called_with(
            {
                'lockKey': 'lock name',
                'leaseDuration': 5,
                'hostIdentifier': mock.ANY,
                'versionNumber': mock.ANY,
            },
            condition=mock.ANY,
        )

    def test_can_release_lock(self, version_lease_factory):
        vlt, bridge, backend, _ = version_lease_factory()
        vlt.acquire('lock name', 5, 200)
        vlt.release('lock name')
        backend.delete.assert_called_with(
            {'lockKey': 'lock name'},
            condition=mock.ANY,
        )

    def test_can_refresh_lock(self, version_lease_factory):
        vlt, bridge, backend, _ = version_lease_factory()
        vlt.acquire('lock name', 5, 200)
        vlt.refresh('lock name')
        version = backend.put.call_args_list[0][0][0]['versionNumber']
        backend.update.assert_called_with(
            {'lockKey': 'lock name'},
            condition=mock.ANY,
            updates={'versionNumber': mock.ANY},
        )
        bridge.we_own_lock.assert_called_with(version)

    def test_does_try_to_steal_lock_repeatedly(self, version_lease_factory):
        # This is a fairly complex test that makes a lot of assertions on all
        # the assertions we can check.
        vlt, bridge, backend, time = version_lease_factory(host='host_ident')

        # Mark the return values here so we can compare them in the backend
        # calls to make sure the correct conditions are being used. Without
        # coupling to what the acutal conditions themselves are.
        bridge.lock_free.return_value = 'lock free'
        bridge.lock_free_or_expired.return_value = 'lock free or expired'

        error = bridge.ConditionFailedError()
        backend.put.side_effect = [
            # First throw an error when we try to write the new lock.
            error,
            # Second throw another error to emulate the lock still being taken.
            error,
            # No error, so the put was successful.
            {}
        ]
        backend.get.side_effect = [
            # First get result is from the initial get_lock_info call
            {'leaseDuration': 5, 'versionNumber': 'first_fail_version'},
            # First get result is from the initial get_lock_info call
            {'leaseDuration': 10, 'versionNumber': 'second_fail_version'},
        ]
        vlt.acquire('lock name', 200, 400)

        # Check that put was called 3 times. The first two were rejected.
        # the third one was accepted.
        assert backend.put.call_count == 3
        backend.put.assert_has_calls(
            [
                mock.call(
                    {
                        'lockKey': 'lock name',
                        'leaseDuration': 200,
                        'hostIdentifier': 'host_ident',
                        'versionNumber': mock.ANY,
                    },
                    condition='lock free',
                ),
                mock.call(
                    {
                        'lockKey': 'lock name',
                        'leaseDuration': 200,
                        'hostIdentifier': 'host_ident',
                        'versionNumber': mock.ANY
                    },
                    condition='lock free or expired',
                ),
                mock.call(
                    {
                        'lockKey': 'lock name',
                        'leaseDuration': 200,
                        'hostIdentifier': 'host_ident',
                        'versionNumber': mock.ANY
                    },
                    condition='lock free or expired',
                ),
            ],
        )

        # Since there were two failed puts, each of those cooresponds to a
        # get, to get lock info and wait the appropriate time.
        assert backend.get.call_count == 2
        backend.get.assert_has_calls(
            [
                mock.call(
                    {
                        'lockKey': 'lock name',
                    },
                    attributes=['leaseDuration', 'versionNumber'],
                ),
                mock.call(
                    {
                        'lockKey': 'lock name',
                    },
                    attributes=['leaseDuration', 'versionNumber'],
                ),
            ],
        )

        # Since there were 3 put calls, there should be 2 sleep calls between
        # them. Each should be based on the leaseDuration of the prior failed
        # put call. In this case 5 and 10 from our mocked get responses.
        assert time.sleeps == [5, 10]

        # Assert that the calls to the bridge have the correct arguments.
        # Called once with no arguments for the first attempt.
        bridge.lock_free.assert_called_once()
        # Subsequent calls to free_or_expired should be given the versionNumber
        # returned by the get call.
        bridge.lock_free_or_expired.assert_has_calls([
            mock.call('first_fail_version'),
            mock.call('second_fail_version'),
        ])

    def test_does_steal_lock_if_it_vanishes(self, version_lease_factory):
        # in the case where the lock is released between the failed put, and
        # the get for its info about how long to wait.
        vlt, bridge, backend, time = version_lease_factory(host='host_ident')

        # Mark the return values here so we can compare them in the backend
        # calls to make sure the correct conditions are being used. Without
        # coupling to what the acutal conditions themselves are.
        bridge.lock_free.return_value = 'lock free'

        error = bridge.ConditionFailedError()
        backend.put.side_effect = [
            # First throw an error when we try to write the new lock.
            error,
            # No error, so the put was successful.
            {}
        ]
        backend.get.side_effect = [
            # First get result is from the initial get_lock_info call
            {},
        ]
        vlt.acquire('lock name', 200, 400)

        # Check that put was called 3 times. The first two were rejected.
        # the third one was accepted.
        assert backend.put.call_count == 2
        backend.put.assert_has_calls(
            [
                mock.call(
                    {
                        'lockKey': 'lock name',
                        'leaseDuration': 200,
                        'hostIdentifier': 'host_ident',
                        'versionNumber': mock.ANY,
                    },
                    condition='lock free',
                ),
                mock.call(
                    {
                        'lockKey': 'lock name',
                        'leaseDuration': 200,
                        'hostIdentifier': 'host_ident',
                        'versionNumber': mock.ANY
                    },
                    condition='lock free',
                ),
            ],
        )

        # Since there were two failed puts, each of those cooresponds to a
        # get, to get lock info and wait the appropriate time.
        assert backend.get.call_count == 1
        backend.get.assert_has_calls(
            [
                mock.call(
                    {
                        'lockKey': 'lock name',
                    },
                    attributes=['leaseDuration', 'versionNumber'],
                ),
            ],
        )

        # Since there were 2 put calls, there should be 1 sleep call between
        # them. Since the get failed we should sleep for0 seconds before
        # retyring.
        assert time.sleeps == [0]

        # Assert that the calls to the bridge have the correct arguments.
        # Called twice with no arguments for the first attempt and sescond
        # attempt. Since it was a failed get there is no lock to check the
        # expiration on, we simply do another free condition put.
        bridge.lock_free.call_count == 2

    def test_release_unacquired_lock_raises(self, version_lease_factory):
        vlt, bridge, backend, time = version_lease_factory()
        with pytest.raises(bridge.NoSuchLockError):
            vlt.release('my lock')

    def test_release_lost_lock_does_raise(self, version_lease_factory):
        vlt, bridge, backend, _ = version_lease_factory()
        error = bridge.ConditionFailedError()
        backend.delete.side_effect = [
            # Refresh attempt fails meaning it lost the lock ownership.
            error,
        ]

        vlt.acquire('my lock', 200, 200)
        with pytest.raises(LockLostError):
            vlt.release('my lock')

    def test_refresh_unacquired_lock_raises(self, version_lease_factory):
        vlt, bridge, backend, time = version_lease_factory()
        with pytest.raises(bridge.NoSuchLockError):
            vlt.refresh('my lock')

    def test_refresh_lost_lock_does_raise(self, version_lease_factory):
        vlt, bridge, backend, _ = version_lease_factory()
        error = bridge.ConditionFailedError()
        backend.update.side_effect = [
            # Refresh attempt fails meaning it lost the lock ownership.
            error,
        ]

        vlt.acquire('my lock', 200, 200)
        with pytest.raises(LockLostError):
            vlt.refresh('my lock')

    def test_timeout_on_acquire_does_raise(self, version_lease_factory):
        vlt, bridge, backend, _ = version_lease_factory()
        error = bridge.ConditionFailedError()
        backend.put.side_effect = [
            # Put attempt fails meaning we could not get lock ownership.
            error,
        ]
        backend.get.side_effect = [
            # First get result is from the initial get_lock_info call it
            # yields a leaseDuration of 200 seconds.
            {'leaseDuration': 200, 'versionNumber': 'first_fail_version'},
        ]

        with pytest.raises(LockNotGrantedError):
            # Since the leaseDuration is 200 on the prior lock holder, and
            # we only have a timeout of 10 seconds, we will timeout acquiring
            # the lock.
            vlt.acquire('my lock', 10, 10)

    def test_timeout_on_acquire_exact_does_raise(self, version_lease_factory):
        # In this case our timeout math in a pure world would work out. However
        # with the timings of networks and code execution it will add up to
        # a little bit more wall-clock time than we are willing to spend.
        vlt, bridge, backend, time = version_lease_factory(
            times=[0, 10, 10.1],
        )
        error = bridge.ConditionFailedError()
        backend.put.side_effect = [
            # Put attempt fails meaning we could not get lock ownership.
            error,
        ]
        backend.get.side_effect = [
            # First get result is from the initial get_lock_info call it
            # yields a leaseDuration of 10 seconds.
            {'leaseDuration': 10, 'versionNumber': 'first_fail_version'},
        ]

        with pytest.raises(LockNotGrantedError):
            # Since the leaseDuration is 10 on the prior lock holder, and
            # we only have a timeout of 10 seconds, we will timeout acquiring
            # the lock since our wall-clock time is going to be 10 and a
            # fraction of a second.
            vlt.acquire('my lock', 10, 10)
