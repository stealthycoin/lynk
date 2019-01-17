from lynk.session import Session

__version__ = '0.3.0'


def get_session(table_name, host_identifier=None,
                backend_bridge_factory=None):
    """Create a new :class:`lynk.session.Session` with default settings.

    This is a convenience function for getting a default configured
    Session.

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
    return Session(table_name, host_identifier, backend_bridge_factory)


__all__ = ['Session', 'get_session']
