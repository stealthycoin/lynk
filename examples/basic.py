import time
import threading
import logging

import boto3

from lynk.lock import LockFactory
from lynk.backends.dynamodb import DynamoBackend


LOG = logging.getLogger(__file__)


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


class SleepyThread(threading.Thread):
    def __init__(self, factory):
        super(SleepyThread, self).__init__()
        self._factory = factory

    def run(self):
        LOG.debug('Starting')
        # Create a lock from our shared factory. Since each thread is the same
        # the lock names will be the same.
        lock = self._factory.create_lock('my lock')
        # The with block here will block until it aquires the lock. Once
        # aquired we have a lease duration of 5 seconds. That means we have 5
        # seconds guerenteed to do our work before the lock can be stolen by
        # another client. This is easy since our only task is sleeping for 1
        # seconds.
        # Once the with block is exited the lock is released from our backend.
        # Allowing other clients to aquire the lock.
        # By default our lock aquisition comes with a 300 second timeout. In
        # other words if we wait more than 300 seconds to aquire the lock
        # we give up.
        with lock(lease_duration=5):
            LOG.debug('Acquired lock')
            time.sleep(1)
            LOG.debug('Done sleeping')
        LOG.debug('Ending')


def do_work_with_locks(backend):
    # Create a lock factory that produces locks tied to our backend.
    # We pass a reference of this into each thread so the threads can
    # create their own lock object.
    factory = LockFactory(backend=backend)
    threads = [SleepyThread(factory) for _ in range(20)]

    for t in threads:
        t.start()

    for t in threads:
        t.join()


def main():
    # Instantiate a dynamo backend that will use a dynamodb table to
    # store lock information. This means our lock objects can be spread
    # across multiple machines, as long as they are using the same
    # AWS creds, table name for the backend, and lock name, they will work
    # the same as shown here in threads.
    table_name = 'example-lynk-lock-table'
    backend = DynamoBackend(table_name)
    try:
        backend.create()
        do_work_with_locks(backend)
    finally:
        ensure_table_deleted(table_name)


def configure_logging():
    # Configure logging so we can see which thread is doing what.
    LOG.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(threadName)s - %(message)s')
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    LOG.addHandler(ch)


if __name__ == '__main__':
    configure_logging()
    main()
