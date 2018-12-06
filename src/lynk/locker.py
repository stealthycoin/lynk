import time
import uuid

from lynk.exceptions import LockNotGrantedError
from lynk.exceptions import LockAlreadyInUseError


class Locker(object):
    def __init__(self, lock_name, backend, host_identifier=None,
                 time_utils=None):
        self._lock_name = lock_name
        self._backend = backend
        if host_identifier is None:
            host_identifier = str(uuid.uuid4())
        self._host_identifier = host_identifier
        if time_utils is None:
            time_utils = time
        self._time_utils = time_utils
        self._version_number = None

    @property
    def lock_name(self):
        return self._lock_name

    @property
    def host_identifier(self):
        return self._host_identifier

    def _create_version_number(self):
        identifier = str(uuid.uuid4())
        return identifier

    def acquire_lock(self, lease_duration, max_wait_seconds=300):
        """Called to register a new lock with the backing store.

        Register this lock with our backing store. If a lock with this name
        already exists we fall back to trying to
        :meth:`lynk.lock.Locker.try_steal_lock` to acquire the lock.

        :type lease_duration: int
        :param lease_duration: Number of seconds that the lease on this lock is
            guaranteed.

        :type max_wait_seconds: int
        :param max_wait_seconds: Maximum number of seconds to wait until giving
            up on creating the lock.
        """
        start_time = self._time_utils.time()
        version = self._create_version_number()
        try:
            self._backend.try_write_new_lock(
                self._lock_name,
                lease_duration,
                self._host_identifier,
                version,
            )
            self._version_number = version
        except LockAlreadyInUseError as prior_lock:
            self._try_steal_lock(
                lease_duration,
                max_wait_seconds,
                prior_lock.lease_duration,
                prior_lock.version_number,
                start_time,
            )

    def _try_steal_lock(self, lease_duration, max_wait_seconds, existing_lease,
                        existing_version_number, start_time):
        version = self._create_version_number()
        while True:
            time_waited = self._time_utils.time() - start_time
            sleep_time = self._calculate_sleep_time(
                time_waited,
                max_wait_seconds,
                existing_lease,
            )
            self._time_utils.sleep(sleep_time)
            try:
                self._backend.try_write_lock(
                    self._lock_name,
                    lease_duration,
                    self._host_identifier,
                    version,
                    existing_version_number,
                )
                self._version_number = version
                break
            except LockAlreadyInUseError as prior_lock:
                existing_lease = prior_lock.lease_duration
                existing_version_number = prior_lock.version_number

    def _calculate_sleep_time(self, time_waited, max_wait, existing_lease):
        if time_waited >= max_wait:
            raise LockNotGrantedError()
        remaining_wait_time = max_wait - time_waited
        next_wait = existing_lease
        if next_wait > remaining_wait_time:
            raise LockNotGrantedError()
        return next_wait

    def delete_lock(self):
        """Delete the lock from the backing store if we still own it.

        Tries to delete the lock_name from the backing store. Uses
        the saved version_number to determine if we still own the lock.
        """
        self._backend.delete(self._lock_name, self._version_number)

    def refresh_lock(self):
        """Refresh the lock in the backing store if we still own it.

        Tries to refresh using the version number we last refreshed/created it
        with.
        """
        new_version = self._create_version_number()
        self._backend.refresh_lock(
            self._lock_name,
            self._version_number,
            new_version,
        )
        self._version_number = new_version
