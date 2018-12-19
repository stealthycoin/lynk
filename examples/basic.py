import uuid
import time
import threading
import logging

import lynk
from lynk.backends.dynamodb import DynamoDBControl


LOG = logging.getLogger(__file__)
THREADS = 20


class SleepyThread(threading.Thread):
    def __init__(self, session):
        super(SleepyThread, self).__init__()
        self._session = session

    def run(self):
        LOG.debug('Starting')
        # Create a lock from our shared session. Since each thread is the same
        # the lock names will be the same.
        lock = self._session.create_lock('my lock', auto_refresh=False)
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
            LOG.debug('Releasing lock')
        LOG.debug('Ending')


def do_work_with_locks(session):
    # Pass our session to each thread. Each thread can create its own lock
    # instance from the shared session, ensuring they are backed by the same
    # table.
    threads = [SleepyThread(session) for _ in range(THREADS)]

    for t in threads:
        t.start()

    for t in threads:
        t.join()


def main():
    # Instantiate a default Session with our table name. By default the
    # Session uses DynamoDB as a backend to store lock information. This
    # means our lock objects can be spread across multiple machines, as long as
    # they are using the same AWS creds, table name for the backend, and lock
    # name, they will work the same as shown here in threads.
    table_name = 'example-lynk-basic-table-%s' % str(uuid.uuid4())
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


if __name__ == '__main__':
    configure_logging()
    main()
