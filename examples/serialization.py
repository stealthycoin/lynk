import uuid
import time
import threading
import logging
import queue

import lynk
from lynk.backends.dynamodb import DynamoDBControl


LOG = logging.getLogger(__file__)
THREADS = 20


class SleepyThread(threading.Thread):
    def __init__(self, session, input_q, output_q):
        super(SleepyThread, self).__init__()
        self._session = session
        self._iq = input_q
        self._oq = output_q

    def run(self):
        LOG.debug('Starting')
        # Block until we get the lock in our input queue.
        serialized_lock = self._iq.get()

        # Turn into a real lock object
        lock = self._session.deserialize_lock(serialized_lock)
        LOG.debug('Deserialized lock')

        # Do "work"
        LOG.debug('Working...')
        time.sleep(1)

        # Serialize the lock to pass to next thread
        serialized_lock = lock.serialize()
        LOG.debug('Serialized lock')
        LOG.debug('Ending')
        self._oq.put(serialized_lock)


def do_work_with_locks(session):
    # Pass our session to each thread. Each thread can create its own lock
    # instance from the shared session, ensuring they are backed by the same
    # table.
    lock = session.create_lock('my lock', auto_refresh=False)

    # Each thread has two queues, an input queue and an output queue. These
    # overlap by 1, so Thread-1's output queue is Thread-2's input queue.
    # Each time a Thread is done working, it seraializes its lock and passes it
    # to the next thread in the queue.
    Qs = [queue.Queue() for _ in range(THREADS + 1)]
    threads = [SleepyThread(session, Qs[i], Qs[i+1]) for i in range(THREADS)]

    for t in threads:
        t.start()

    # Acquire the lock and pass the serialized lock to the first thread.
    LOG.debug("Sending lock to first thread.")
    lock.acquire()
    serialized_lock = lock.serialize()
    Qs[0].put(serialized_lock)

    # Wait on the last Queue to get the lock back.
    serialized_lock = Qs[-1].get()

    LOG.debug("Got lock back.")

    # Release the lock and ensure the threads are done.
    lock = session.deserialize_lock(serialized_lock)
    lock.release()

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
