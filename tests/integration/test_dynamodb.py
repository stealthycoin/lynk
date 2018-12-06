import time
import uuid

import pytest
import boto3

from lynk.lock import LockFactory
from lynk.backends.dynamodb import DynamoBackend
from lynk.exceptions import LockNotGrantedError


def ensure_table_deleted(table_name):
    client = boto3.client('dynamodb')
    while True:
        try:
            client.delete_table(TableName=table_name)
            break
        except client.exceptions.ResourceInUseException:
            time.sleep(10)
    waiter = client.get_waiter('table_not_exists')
    waiter.wait(TableName=table_name)


@pytest.fixture(scope="module")
def lock_factory():
    try:
        table_name = 'lynk-integ-table-%s' % str(uuid.uuid4())
        client = boto3.client('dynamodb')
        backend = DynamoBackend(table_name)
        backend.create()
        waiter = client.get_waiter('table_exists')
        waiter.wait(TableName=table_name)
        factory = LockFactory(backend)
        yield factory
    finally:
        ensure_table_deleted(table_name)


@pytest.fixture
def lock_name():
    return 'lock-%s' % str(uuid.uuid4())


def test_table_creation_cycle():
    table_name = 'lynk-integ-table-%s' % str(uuid.uuid4())
    backend = DynamoBackend(table_name)
    assert backend.exists() is False
    try:
        backend.create()
        assert backend.exists() is True
    finally:
        ensure_table_deleted(table_name)


class TestLockAcquiring(object):
    def delete_lock_from_table(self, lock):
        lock.release()

    def test_can_aquire_lock_from_empty_table(self, lock_factory, lock_name):
        lock = lock_factory.create_lock(lock_name)
        lock.acquire()
        assert lock.name == lock_name
        self.delete_lock_from_table(lock)

    def test_cannot_re_acquire_lock(self, lock_factory, lock_name):
        # Create a lock and then try to acquire it again right away. It should
        # fail even though it is the same client issuing the request since the
        # acutal Lock object would differ.
        lock_1 = lock_factory.create_lock(lock_name)
        lock_2 = lock_factory.create_lock(lock_name)
        lock_1.acquire(max_wait_seconds=0)
        with pytest.raises(LockNotGrantedError):
            lock_2.acquire(max_wait_seconds=0)

    def test_can_acquire_lock_when_old_one_expires(
            self, lock_factory, lock_name):
        # Acquire a lock that expires right away. The second call to the lock
        # should succeed since it will fail its first attempt to write the lock
        # and then wait 0 seconds since the lease time was 0 on the first lock,
        # then since the lock entry did not change its record version number
        # that means it is free to be acquired.
        lock_1 = lock_factory.create_lock(lock_name)
        lock_2 = lock_factory.create_lock(lock_name)
        lock_1.acquire(lease_duration=0)
        lock_2.acquire(
            lease_duration=100,
            max_wait_seconds=12,
        )
        assert lock_2.name == lock_name

    def test_cannot_acquire_lock_with_short_max_wait(
            self, lock_factory, lock_name):
        # Waiting for the lock should fail since the lease is 5 seconds and
        # the second lock acquisition will only wait for 2 seconds.
        lock_1 = lock_factory.create_lock(lock_name)
        lock_2 = lock_factory.create_lock(lock_name)
        lock_1.acquire(lease_duration=5)
        with pytest.raises(LockNotGrantedError):
            lock_2.acquire(max_wait_seconds=2)
