from threading import RLock

from lynk.backends import LockBackend
from lynk.exceptions import LockAlreadyInUseError


class LocalBackend(LockBackend):
    """Local in memory storage backing that uses :class:`threading.Lock`

    This class is for test and demonstration purposes.
    """
    def __init__(self):
        self._locks = {}
        self._lock = RLock()

    def _write_lock(self, key, lease_duration, agent_identifier,
                    version_number):
        self._locks[key] = {
            'name': key,
            'lease_duration': lease_duration,
            'agent_identifier': agent_identifier,
            'version_number': version_number
        }

    def try_write_new_lock(self, key, lease_duration, agent_identifier,
                           version_number):
        """Writes a new lock ``key`` must not already exist"""
        with self._lock:
            if key not in self._locks:
                self._write_lock(
                    key, lease_duration, agent_identifier, version_number
                )
            else:
                existing_lease = self._locks[key]['lease_duration']
                existing_version = self._locks[key]['version_number']
                raise LockAlreadyInUseError(existing_lease, existing_version)

    def try_write_lock(self, key, lease_duration, agent_identifier,
                       new_version_number, expected_version_number):
        with self._lock:
            try:
                self.try_write_new_lock(
                    key, lease_duration, agent_identifier, new_version_number
                )
            except LockAlreadyInUseError as prior_lock:
                # If this lock has the same version we are expecting then we can
                # overwrite it since it timed out.
                if prior_lock.version_number == expected_version_number:
                    self._write_lock(
                        key, lease_duration, agent_identifier,
                        new_version_number
                    )
                else:
                    raise

    def delete(self, key):
        with self._lock:
            self._locks.pop(key)
