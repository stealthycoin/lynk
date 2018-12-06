import socket
from contextlib import contextmanager

from lynk.locker import Locker
from lynk.refresh import LockRefresherFactory


class Lock(object):
    """A class that provides an interface with which to use a lock.

    The Lock object is a wrapper that provides a convenient interface, the
    actual work is done in the underlying Locker object. This object should
    not be initialized directly, but created from a
    :class:`lynk.lock.LockFactory`.
    """
    def __init__(self, locker, refresher_factory=None):
        """Initialize a new Lock.

        :type locker: :class:`lynk.lock.Locker`
        :param locker: The provided Locker is responsible for running the
            actual locking algorithms for creation releasing and stealing.
        """
        self._locker = locker

    @property
    def name(self):
        return self._locker.lock_name

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
        self._locker.acquire_lock(
            lease_duration,
            max_wait_seconds=max_wait_seconds,
        )

    def release(self):
        """Release this lock.
        """
        self._locker.delete_lock()

    def refresh(self):
        """Refresh this lock.
        """
        self._locker.refresh_lock()

    def __call__(self, lease_duration=20, timeout_seconds=300):
        return self._context_manager(lease_duration, timeout_seconds)

    @contextmanager
    def _context_manager(self, lease_duration, max_wait_seconds):
        try:
            self.acquire(
                lease_duration=lease_duration,
                max_wait_seconds=max_wait_seconds,
            )
            yield
        finally:
            self.release()


class AutoRefreshingLock(Lock):
    _REFRESH_PERIOD_RATIO = 2.0 / 3.0

    def __init__(self, locker, refresher_factory):
        """Initialize a new Lock.

        :type locker: :class:`lynk.lock.Locker`
        :param locker: The provided Locker is responsible for running the
            actual locking algorithms for creation releasing and stealing.

        :type refresher_factory: :class:`lynk.refresh.LockRefresherFactory`
        :param refresher_factory: The refresher is in charge of keeping the
            lock owned by this lock object. If this factory is set it will be
            have its
            :meth:`lynk.refresh.LockRefresherFactory.create_lock_refresher`
            called When an acquire is called. This produces a
            :class:`lynk.refresh.LockRefresher` which is used to maintain
            ownership of the lock in the backing store. It does this by
            spinning up a separate thread which periodically calls the refresh
            function of this lock. The period will be chosen based on the
            lease duration that the acquire was called with.
        """
        super(AutoRefreshingLock, self).__init__(locker)
        self._refresher_factory = refresher_factory
        self._refresher = None

    def acquire(self, lease_duration=20, max_wait_seconds=300):
        """Try to aquire this lock.

        This method is the same as the parent lock class's acquire except that
        it creates and starts a :class:`lynk.refresh.LockRefresher` after
        acquiring the lock.

        :type lease_duration: int
        :param lease_duration: The number of seconds to hold the lock for
            initially. The lock can be refreshed in smaller periods than the
            lease_duration to hold the lock longer.

        :type max_wait_seconds: float
        :param max_wait_seconds: Number of seconds to wait to aquire the lock
            before giving up and raising a
            :class:`lynk.exceptions.LockNotGrantedError`.
        """
        super(AutoRefreshingLock, self).acquire(
            lease_duration, max_wait_seconds)
        self._start_refresher(lease_duration)

    def release(self):
        """Release this lock.
        """
        self._stop_refresher()
        super(AutoRefreshingLock, self).release()

    def _start_refresher(self, lease_duration):
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


class LockFactory(object):
    def __init__(self, backend, host_identifier=None):
        """Initialize a new LockFactory.
        """
        self._backend = backend
        if host_identifier is None:
            host_identifier = socket.gethostname()
        self._host_identifier = host_identifier

    def create_lock(self, lock_name):
        locker = self._create_locker(lock_name)
        lock = Lock(locker)
        return lock

    def create_auto_refreshing_lock(self, lock_name):
        locker = self._create_locker(lock_name)
        refresher_factory = LockRefresherFactory()
        lock = AutoRefreshingLock(locker, refresher_factory)
        return lock

    def _create_locker(self, lock_name):
        locker = Locker(
            lock_name,
            self._backend,
            host_identifier=self._host_identifier,
        )
        return locker
