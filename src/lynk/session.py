import json
import socket

from lynk.techniques import VersionLeaseTechinque
from lynk.refresh import LockRefresherFactory
from lynk.backends.dynamodb import DynamoDBBackendBridgeFactory
from lynk.lock import Lock
from lynk.exceptions import CannotDeserializeError


class Session(object):
    """A session can create locks bound to a particular backend.

    A ``Session`` represents the logical binding beteween an agent that
    can interact with locks, and a backend table where the locks are stored.
    This relationship is defined by the ``table_name`` and ``host_identifier``
    parameters outlined below.

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
    def __init__(self, table_name, host_identifier=None,
                 backend_bridge_factory=None):
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

    def deserialize_lock(self, serialized_lock, auto_refresh=True):
        """Create a lock object from a serialized lock.

        :type serialized_lock: str
        :param serialized_lock: The serialized lock.

        :type auto_refresh: bool
        :param auto_refresh: If ``True`` the created lock will automatically
            refresh itself. If ``False`` it will not. The default value is
            ``True``.

        :returns: The deserialized Lock object.
        """
        bridge, backend = self._backend_bridge_factory.create(
            self._table_name,
        )
        data = json.loads(serialized_lock)
        version = data.get('__version')
        if not version:
            raise CannotDeserializeError(
                "Serialized data does not contain a lock.")
        if version != 'Lock.1':
            raise CannotDeserializeError(
                "Unsupported serialized data version. Found %s, expected "
                "Lock.1" % version)
        try:
            lock_name = data['name']
            serialized_technique = data['technique']
        except KeyError:
            raise CannotDeserializeError(
                "Missing property. Needs both 'name' and 'technique' "
                "properties."
            )

        technique = VersionLeaseTechinque.from_serialized_technique(
            serialized_technique,
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
        lock.refresh()
        return lock
