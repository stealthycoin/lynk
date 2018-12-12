import socket
from contextlib import contextmanager

from lynk.techniques import VersionLeaseTechinque
from lynk.refresh import LockRefresherFactory
from lynk.backends.dynamodb import DynamoDBBackendBridgeFactory


class Lock(object):
    """A class that provides an interface with which to use a lock.

    The Lock object is a wrapper that provides a convenient interface, the
    actual work is done in the underlying Techinque and Backend objects. This
    object should not be initialized directly, but created from a
    :class:`lynk.lock.LockFactory`.
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
        try:
            self.acquire(
                lease_duration=lease_duration,
                max_wait_seconds=max_wait_seconds,
            )
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


class LockFactory(object):
    """Class for constructing locks."""
    def __init__(self, table_name, host_identifier=None,
                 backend_bridge_factory=None):
        """Initialize a new LockFactory.

        :type table_name: str
        :param table_name: Name of the table in the backend.

        :type host_identifier: str
        :param host_identifier: A unique identifier for a host. A host is just
            an unused field in the database. It is for debugging and nothing
            more so any value can be used that has meaning to the developer.
            By default hostname is used.

        :type backend_bridge_factory: Anything with a create method or None.
        :param backend_bridge_factory: A factory that creates our backend and
            its associated bridge class to be injected into our lock. Usually
            these need to be created by a shared factory class because they
            have shared dependencies. If None is provided the default is a
            :class:`lynk.backends.dynamodb.DynamoDBBackendBridgeFactory` which
            will create locks bound to a DynamoDB Table.
        """
        self._table_name = table_name
        if host_identifier is None:
            host_identifier = socket.gethostname()
        self._host_identifier = host_identifier
        if backend_bridge_factory is None:
            backend_bridge_factory = DynamoDBBackendBridgeFactory()
        self._backend_bridge_factory = backend_bridge_factory

    def create_lock(self, lock_name, auto_refresh=True):
        """Create a new lock object.

        :type lock_name: str
        :param lock_name: Logical name of the lock in the backend.

        :type auto_refresh: bool
        :param auto_refresh: If ``True`` the created lock will automatically
            refresh itself. If ``False`` it will not. The default value is
            ``True``.
        """
        bridge, backend = self._backend_bridge_factory.create(
            self._table_name,
        )
        technique = VersionLeaseTechinque(
            bridge,
            backend,
            host_identifier=self._host_identifier,
        )
        refresher_factory = None
        if auto_refresh:
            refresher_factory = LockRefresherFactory()
        lock = Lock(
            lock_name,
            technique,
            refresher_factory=refresher_factory,
        )
        return lock
