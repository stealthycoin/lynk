class LockNotGrantedError(Exception):
    """Thrown when a lock is not granted due to a timeout"""


class LockAlreadyInUseError(Exception):
    """Thrown when a lock cannot be acquired because another client owns it.

    It will be populated with the existing lock's record_version_number and
    lease_duration which allows us to try and take the lock after that timeout.
    """
    def __init__(self, lease_duration, version_number):
        self.lease_duration = lease_duration
        self.version_number = version_number


class LockLostError(Exception):
    """Thrown if a lock is lost.

    A lock is discoved to be stolen a refresh fails. Assuming proper client
    cooperation this means another client stole the lock since it was not
    refreshed before the lease duration expired.
    """


class NoSuchLockError(Exception):
    """Raised when an operation is performed on a non-existant lock."""
