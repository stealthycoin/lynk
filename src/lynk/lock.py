import json

from contextlib import contextmanager


class Lock(object):
    """A class that provides an interface with which to use a lock.

    The Lock object is a wrapper that provides a convenient interface, the
    actual work is done in the underlying Techinque and Backend objects. This
    object should not be initialized directly, but created from a
    :class:`lynk.session.Session`.
    """
    _REFRESH_PERIOD_RATIO = 3.0 / 4.0

    def __init__(self, name, technique, refresher_factory=None):
        """Initialize a new Lock.

        :type locker: :class:`lynk.lock.Locker`
        :param locker: The provided Locker is responsible for running the
            actual locking algorithms for creation releasing and stealing.
        """
        self._name = name
        self._technique = technique
        self._refresher_factory = refresher_factory
        self._refresher = None

    def acquire(self, lease_duration=20, max_wait_seconds=300):
        """Try to aquire this lock.

        This call will block until the lock has been confirmed as owned by us
        in the backing store, or if the number of timeout seconds has been
        reached. If the timout is reached then a LockNotGrantedError will be
        raised.

        :type lease_duration: int
        :param lease_duration: The number of seconds to hold the lock for
            initially. The lock can be refreshed in smaller periods than the
            lease_duration to hold the lock longer.

        :type max_wait_seconds: float
        :param max_wait_seconds: Number of seconds to wait to aquire the lock
            before giving up and raising a
            :class:`lynk.exceptions.LockNotGrantedError`.
        """
        self._technique.acquire(
            self._name,
            lease_duration,
            max_wait_seconds=max_wait_seconds,
        )
        self._start_refresher(lease_duration)

    def release(self):
        """Release this lock."""
        self._stop_refresher()
        self._technique.release(self._name)

    def refresh(self):
        """Refresh this lock."""
        self._technique.refresh(self._name)

    def __call__(self, lease_duration=20, timeout_seconds=300):
        return self._context_manager(lease_duration, timeout_seconds)

    @contextmanager
    def _context_manager(self, lease_duration, max_wait_seconds):
        self.acquire(
            lease_duration=lease_duration,
            max_wait_seconds=max_wait_seconds,
        )
        try:
            yield
        finally:
            self.release()

    def _start_refresher(self, lease_duration):
        if not self._refresher_factory:
            return
        self._refresher = self._refresher_factory.create_lock_refresher(
            self,
            lease_duration * self._REFRESH_PERIOD_RATIO,
        )
        self._refresher.start()

    def _stop_refresher(self):
        if not self._refresher:
            return
        self._refresher.stop()
        self._refresher = None

    def serialize(self):
        """Serialize this lock to a UTF-8 string.

        To restore the string to a lock object, construct a session object
        that matches the one that this lock was constructed with. Call its
        :meth:`lynk.session.Session.deserialize_lock` method.

        To improve its chances of making the journey to the new host with
        ownership of the lock entry in the remote table, the Lock calls it's
        own :meth:`lynk.lock.Lock.refresh` method.
        """
        self._stop_refresher()
        self.refresh()
        properties = {
            '__version': '%s.1' % self.__class__.__name__,
            'name': self._name,
            'technique': self._technique.serialize(),
        }
        return json.dumps(properties)
