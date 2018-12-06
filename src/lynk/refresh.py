from threading import Thread
from threading import Event


class LockRefresher(Thread):
    """A class to refresh a lock's ownership over it's keyname in a backing.

    A thread subclass that can be used to repeatedly referesh a Lock in the
    background while some other thread operates on the resource the lock is
    protecting.
    """
    def __init__(self, refresh_fn, refresh_period_seconds=5):
        super(LockRefresher, self).__init__()
        self._refresh_fn = refresh_fn
        self._refresh_period_seconds = refresh_period_seconds
        self._stop_event = Event()

    def stop(self):
        self._stop_event.set()

    def run(self):
        while not self._stop_event.wait(timeout=self._refresh_period_seconds):
            self._refresh_fn()


class LockRefresherFactory(object):
    def create_lock_refresher(self, lock, refresh_period_seconds):
        return LockRefresher(lock.refresh, refresh_period_seconds)
