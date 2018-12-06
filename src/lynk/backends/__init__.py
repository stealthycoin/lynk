class LockBackendConfigProvider(object):
    """Provides arguments to configure a LockBackend"""


class LockBackend(object):
    def try_write_new_lock(self, key, lease_duration, agent_identifier,
                           record_version_number):
        raise NotImplementedError('try_write_new_lock')

    def try_write_lock(self, key, lease_duration, agent_identifier,
                       record_version_number):
        raise NotImplementedError('try_write_lock')

    def refresh_lock(self, key, old_version_number, new_version_number):
        raise NotImplementedError('refresh_lock')

    def delete(self, key, version_number):
        raise NotImplementedError('delete')
