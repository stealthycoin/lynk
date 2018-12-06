import time
import logging
import threading

import boto3

from lynk import DynamoDBLynk

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
        # Create an auto-refreshing lock from our shared factory.
        # We explicitly set the lease duration to be shorter than the ammount
        # of time we hold the lock for. The lease duration is 5 seconds, and
        # we operate on the locked resource for 10 seconds.
        # This works because we are using an auto refreshing lock, which spins
        # up a thread to automatically refreshes the lock before the lease
        # expires. Once the lock is released the refresher thread is destroyed.
        lock = self._factory.create_auto_refreshing_lock('my lock')
        with lock(lease_duration=5):
            LOG.debug('Acquired lock')
            time.sleep(10)
            LOG.debug('Done sleeping')
        LOG.debug('Ending')


def do_work_with_locks(factory):
    # Create a lock factory that produces locks tied to our backend.
    # We pass a reference of this into each thread so the threads can
    # create their own lock object.
    threads = [SleepyThread(factory) for _ in range(2)]

    for t in threads:
        t.start()

    for t in threads:
        t.join()


def main():
    table_name = 'example-lynk-lock-table-auto-refresh'
    factory = DynamoDBLynk(table_name)

    try:
        factory.create_table()
        do_work_with_locks(factory)
    finally:
        ensure_table_deleted(table_name)


def configure_logging():
    # Configure logging so we can see which thread is doing what.
    LOG.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(threadName)s - %(message)s')
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    LOG.addHandler(ch)


if __name__ == "__main__":
    configure_logging()
    main()
