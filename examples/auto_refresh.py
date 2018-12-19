import uuid
import time
import logging
import threading

import lynk
from lynk.backends.dynamodb import DynamoDBControl

LOG = logging.getLogger(__file__)
THREADS = 3


class SleepyThread(threading.Thread):
    def __init__(self, session):
        super(SleepyThread, self).__init__()
        self._session = session

    def run(self):
        LOG.debug('Starting')
        # Create an auto-refreshing lock from our shared session.
        # We explicitly set the lease duration to be shorter than the ammount
        # of time we hold the lock for. The lease duration is 5 seconds, and
        # we operate on the locked resource for 10 seconds.
        # This works because we are using an auto refreshing lock, which spins
        # up a thread to automatically refreshes the lock before the lease
        # expires. Once the lock is released the refresher thread is destroyed.
        lock = self._session.create_lock('my lock', auto_refresh=True)
        with lock(lease_duration=5):
            LOG.debug('Acquired lock')
            time.sleep(10)
            LOG.debug('Releasing lock')
        LOG.debug('Ending')


def do_work_with_locks(session):
    # Create a lock session that produces locks tied to our backend.
    # We pass a reference of this into each thread so the threads can
    # create their own lock object.
    threads = [SleepyThread(session) for _ in range(THREADS)]

    for t in threads:
        t.start()

    for t in threads:
        t.join()


def main():
    table_name = 'example-lynk-auto-refresh-table-%s' % str(uuid.uuid4())
    session = lynk.get_session(table_name)
    # Create a DynamoDBControl object to give us access to the control plane
    # of dynamodb for creation/deletion of tables. For this example we create
    # a table if needed, and destroy it after running the example.
    control = DynamoDBControl(table_name)

    try:
        if not control.exists():
            LOG.debug('Creating table %s', table_name)
            control.create()
        do_work_with_locks(session)
    finally:
        LOG.debug('Destroying table %s', table_name)
        control.destroy()


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
