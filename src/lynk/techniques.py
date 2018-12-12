"""This module encapsulates the tecnique used to maintain locks."""
import uuid
import socket

from lynk.utils import TimeUtils
from lynk.exceptions import LockNotGrantedError
from lynk.exceptions import LockAlreadyInUseError
from lynk.exceptions import LockLostError
from lynk.exceptions import NoSuchLockError


class BaseTechnique(object):
    def acquire(self, name, lease_duration, max_wait_seconds):
        pass

    def release(self, name):
        pass

    def refresh(self, name):
        pass


class VersionLeaseTechinque(BaseTechnique):
    """A class to implement the version lease technique.

    The version lease technique uses a uuid4 as a fencing token, and a timing
    system based on lease durations. When a lock is acquired an entry is pushed
    into a backing store that has consistent read/write with the following
    properties::

      name:           string
      versionNumber:  string
      leaseDuration:  int
      hostIdentifier: string

    * Name - In a distributed system multiple hosts/entities sometimes need to
      operate on the same resource. To do so they acquire a lock on the
      resource by name. This name is chosen by the client that is acquiring
      the lock and only has meaning within their system.
    * versionNumber - The version number is our fencing token. When we create a
      lock entry it is given a uuid4 as it's versionNumber. Any
      writes/deletes to this entry in the future require that the caller know
      the matching versionNumber in order for the change to be enacted.
    * leaseDuration - This is the number of seconds that the lock is good for,
      at a minimum. Once an entry is written the owner of the lock is
      responsible for refreshing or releasing it in < leaseDuration seconds.
      If it does not do that the lock can be stolen by another client.
    * hostIdentifier - This is a convenience for debugging. This simply gives a
      way for a debugging programmer to see which host currently owns a lock.


    The three elemental operations that make up the algorithm are acquire,
    release and refresh.
    Acquire writes one of the above locking entries into the backing store
    (DynamoDB by default). If this succeeds the local process now own the
    rights to operate on whatever shared resource this lock logically protects.
    If this process takes longer than leaseDuration seconds, the owning host
    needs to refresh the lock before the leaseDuration expires.
    When a refresh occurs it attempts to re-write to lock entry by replacing
    its versionNumber the write is conditional on the versionNumber locally
    known matching the current one in the backing store entry. If they do not
    match that means this host was not the last one to write the lock and the
    lock has been stolen. This can happen if the refresh did not happen
    quickly enough for some reason (gc pause, thread scheduling preventing
    refresh etc).
    Delete will delete the entry in the backing store, again contingent on
    knowing the correct versionNumber. This is logically releasing the lock.

    When acquire fails because another host owns the lock, the versionNumber
    and leaseDuration are recorded locally. The acquire call will block until
    the lock is successfully acquired or a provided timeout is exceeded. The
    acquire call will be repeated after sleeping for leaseDuration seconds.
    There are three cases, that can be distinquished between since the
    versionNumber was recorded:

    * The lock entry is gone. This means the lock was released. The acquire can
      go ahead without issue.
    * The lock entry has the same versionNumber. This means the owner of the
      lock failed to refresh. This can happen for many reasons, the host may
      have crashed, a gc pause prevented the refresh from going through,
      network could have have a temporary outage. In any case it means the
      lock is no longer owned by that host.
    * The lock entry has a new versionNumber. This means the lock has been
      refreshed, or another host that was interested in this lock tried to
      acquire it first, and found it released/expired. This case is the same as
      failing to acquire in the first place. The leaseDuration and
      versionNumber are recorded and used to retry again.

    This basic locking scheme has no concept of priority or a sepmaphore. A
    lock acquisition can easily time out by happenstance if a lock gets unlucky
    it can still get starved and timeout.
    """
    def __init__(self, backend_bridge, backend, host_identifier=None,
                 time_utils=None):
        """Initialize a VersionLeaseTechinque.

        :type backend_bridge: Bridge class to bridge the interface betwen
            this Technique class and a Backend.
        :param backend_bridge: This locking algorhtm requires conditions to
            be fulfilled as it is writing to its backend. This class helps
            take the high-level logical conditions and re-write them as
            arguments that can be passed to (and understood by) the backend's
            interface.

        :type backend: :class:`lynk.backends.backend.BaseBackend` subclass
        :param backend: The backend used to fufill the lock storage. Requires
            consistent read/write.

        :type host_identifier: str
        :param host_identifier: A unique identifier for a host. A host is just
            an unused field in the database. It is for debugging and nothing
            more so any value can be used that has meaning to the developer.
            By default hostname is used.

        :type time_utils: :class:`lynk.utils.TimeUtils`
        :param time_utils: A set of utilities for interacting with time.

        """
        self._backend_bridge = backend_bridge
        self._backend = backend
        if host_identifier is None:
            host_identifier = socket.gethostname()
        self._host_identifier = host_identifier
        if time_utils is None:
            time_utils = TimeUtils()
        self._time_utils = time_utils
        self._versions = {}

    def acquire(self, name, lease_duration, max_wait_seconds):
        """Acquire a lock.

        Tries to acquire a lock using the strategy outlined above.

        :type name: str
        :param name: Logical name of the lock being aquired.

        :type lease_duration: int
        :param lease_duration: Number of seconds to acquire the lock.

        :type max_wait_seconds: int
        :param max_wait_seconds: Maximum number of seconds to wait till
            giving up on acquiring the lock.
        """
        start_time = self._time_utils.time()
        version = self._create_version_number()
        try:
            self._try_write_new_lock(
                name,
                lease_duration,
                version,
            )
            self._version_number = version
        except LockAlreadyInUseError as prior_lock:
            self._try_steal_lock(
                name,
                lease_duration,
                max_wait_seconds,
                prior_lock.lease_duration,
                prior_lock.version_number,
                start_time,
            )

    def _try_write_new_lock(self, name, lease_duration, version):
        item = {
            'lockKey': name,
            'leaseDuration': lease_duration,
            'hostIdentifier': self._host_identifier,
            'versionNumber': version,
        }
        try:
            self._backend.put(
                item,
                condition=self._backend_bridge.lock_free(),
            )
            self._versions[name] = version
        except self._backend_bridge.ConditionFailedError:
            self._raise_lock_in_use(name)

    def _try_steal_lock(self, name, lease_duration, max_wait_seconds,
                        existing_lease, existing_version_number, start_time):
        version = self._create_version_number()
        while True:
            time_waited = self._time_utils.time() - start_time
            sleep_time = self._calculate_sleep_time(
                time_waited,
                max_wait_seconds,
                existing_lease,
            )
            self._time_utils.sleep(sleep_time)
            lock_method_args = [
                name,
                lease_duration,
                version,
            ]
            lock_method = self._try_write_new_lock
            if existing_version_number:
                lock_method_args.append(existing_version_number)
                lock_method = self._try_write_lock
            try:
                lock_method(*lock_method_args)
                self._versions[name] = version
                break
            except LockAlreadyInUseError as prior_lock:
                existing_lease = prior_lock.lease_duration
                existing_version_number = prior_lock.version_number

    def _try_write_lock(self, name, lease_duration, version,
                        existing_version_number):
        item = {
            'lockKey': name,
            'leaseDuration': lease_duration,
            'hostIdentifier': self._host_identifier,
            'versionNumber': version,
        }
        try:
            self._backend.put(
                item,
                condition=self._backend_bridge.lock_free_or_expired(
                    existing_version_number,
                ),
            )
            self._version_number = version
        except self._backend_bridge.ConditionFailedError:
            self._raise_lock_in_use(name)

    def _create_version_number(self):
        identifier = str(uuid.uuid4())
        return identifier

    def _calculate_sleep_time(self, time_waited, max_wait, existing_lease):
        if time_waited >= max_wait:
            raise LockNotGrantedError()
        remaining_wait_time = max_wait - time_waited
        next_wait = existing_lease
        if next_wait > remaining_wait_time:
            raise LockNotGrantedError()
        return next_wait

    def _raise_lock_in_use(self, name):
        lock_info = self._backend.get(
            {'lockKey': name},
            attributes=['leaseDuration', 'versionNumber'],
        )
        # If we could not find any lock info that means between our call to
        # write the lock which failed, and our call to get the lock info, the
        # agent that owned the lock releaesd it. Since there is no longer a
        # lock we don't need to wait, and we don't have a prior versionNumber
        # to look for. So the leaseDuration can be set to 0 since we can retry
        # right away. And the versionNumber can be set to None so that the
        # correct _try_write_new_lock method is called instead of
        # _try_write_lock.
        if not lock_info:
            lock_info = {'leaseDuration': 0, 'versionNumber': None}
        raise LockAlreadyInUseError(
            lock_info['leaseDuration'],
            lock_info['versionNumber'],
        )

    def release(self, name):
        """Release a lock.

        :type name: str
        :param name: Logical name of the lock to release.
        """
        try:
            version_number = self._get_version_for_name(name)
            self._backend.delete(
                {'lockKey': name},
                condition=self._backend_bridge.we_own_lock(version_number),
            )
            del self._versions[name]
        except self._backend_bridge.ConditionFailedError:
            raise LockLostError()

    def refresh(self, name):
        """Refresh a lock.

        :type name: str
        :param name: Logical name of the lock to refresh.
        """
        old_version = self._get_version_for_name(name)
        new_version = self._create_version_number()
        try:
            self._backend.update(
                {'lockKey': name},
                updates={
                    'versionNumber': new_version,
                },
                condition=self._backend_bridge.we_own_lock(old_version),
            )
            self._versions[name] = new_version
        except self._backend_bridge.ConditionFailedError:
            raise LockLostError()

    def _get_version_for_name(self, name):
        if name not in self._versions:
            raise NoSuchLockError()
        return self._versions[name]
