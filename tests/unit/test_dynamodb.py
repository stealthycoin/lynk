import pytest
import mock
from boto3.session import Session
from boto3.dynamodb.conditions import Attr

from lynk.backends.dynamodb import DynamoBackend
from lynk.exceptions import LockLostError
from lynk.exceptions import LockAlreadyInUseError


class ResourceNotFoundException(Exception):
    pass

class ConditionalCheckFailedException(Exception):
    pass


class Exceptions(object):
    def __init__(self):
        self.ResourceNotFoundException = ResourceNotFoundException
        self.ConditionalCheckFailedException = ConditionalCheckFailedException


@pytest.fixture
def backend_factory():
    def wrapped(table_name=None, session=None):
        if table_name is None:
            table_name = 'table_name'
        if session is None:
            session = mock.Mock(spec=Session)
        backend = DynamoBackend(table_name, session)
        backend._exceptions = Exceptions()
        return backend
    return wrapped


class TestDyanmoBackend(object):
    def test_does_save_table_name(self, backend_factory):
        backend = backend_factory('table')
        assert backend.name == 'table'

    def test_can_check_if_table_exists(self, backend_factory):
        session = mock.Mock(spec=Session)
        mock_client = mock.Mock()
        session.client.return_value = mock_client
        backend = backend_factory(session=session)

        exists = backend.exists()
        mock_client.describe_table.assert_called_with(TableName='table_name')
        assert exists is True

    def test_can_check_if_table_not_exists(self, backend_factory):
        session = mock.Mock(spec=Session)
        mock_client = mock.Mock()
        mock_client.describe_table.side_effect = ResourceNotFoundException()
        session.client.return_value = mock_client
        backend = backend_factory(session=session)

        exists = backend.exists()
        mock_client.describe_table.assert_called_with(TableName='table_name')
        assert exists is False

    def test_can_create_table(self, backend_factory):
        session = mock.Mock(spec=Session)
        mock_waiter = mock.Mock()
        mock_client = mock.Mock()
        mock_client.get_waiter.return_value = mock_waiter
        session.client.return_value = mock_client
        backend = backend_factory(session=session)

        backend.create()
        mock_client.create_table.assert_called_with(
            TableName='table_name',
            AttributeDefinitions=[
                {
                    'AttributeName': 'lockKey',
                    'AttributeType': 'S',
                },
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5,
            },
            KeySchema=[
                {
                    'AttributeName': 'lockKey',
                    'KeyType': 'HASH',
                }
            ],
        )
        mock_client.get_waiter.assert_called_with('table_exists')
        mock_waiter.wait.assert_called_with(TableName='table_name')

    def test_can_refresh_lock(self, backend_factory):
        session = mock.Mock(spec=Session)
        mock_table = mock.Mock()

        mock_resource = mock.Mock()
        mock_resource.Table.return_value = mock_table

        session.resource.return_value = mock_resource
        backend = backend_factory(session=session)

        lock_name = 'lock'
        old_version_number = '1'
        new_version_number = '2'

        backend.refresh_lock(lock_name, old_version_number, new_version_number)
        mock_table.update_item.assert_called_with(
            Key={'lockKey': lock_name},
            UpdateExpression='set versionNumber = :v',
            ExpressionAttributeValues={':v': new_version_number},
            ConditionExpression=Attr('versionNumber').eq(old_version_number),
        )

    def test_does_raise_refreshing_non_existant_lock(self, backend_factory):
        session = mock.Mock(spec=Session)
        mock_table = mock.Mock()
        mock_table.update_item.side_effect = ResourceNotFoundException()

        mock_resource = mock.Mock()
        mock_resource.Table.return_value = mock_table

        session.resource.return_value = mock_resource
        backend = backend_factory(session=session)

        lock_name = 'lock'
        old_version_number = '1'
        new_version_number = '2'

        with pytest.raises(LockLostError):
            backend.refresh_lock(
                lock_name, old_version_number, new_version_number)

    def test_does_raise_refreshing_stolen_lock(self, backend_factory):
        session = mock.Mock(spec=Session)
        mock_table = mock.Mock()
        mock_table.update_item.side_effect = ConditionalCheckFailedException()

        mock_resource = mock.Mock()
        mock_resource.Table.return_value = mock_table

        session.resource.return_value = mock_resource
        backend = backend_factory(session=session)

        lock_name = 'lock'
        old_version_number = '1'
        new_version_number = '2'

        with pytest.raises(LockLostError):
            backend.refresh_lock(
                lock_name, old_version_number, new_version_number)

    def test_can_delete_owned_lock(self, backend_factory):
        session = mock.Mock(spec=Session)
        mock_table = mock.Mock()

        mock_resource = mock.Mock()
        mock_resource.Table.return_value = mock_table

        session.resource.return_value = mock_resource
        backend = backend_factory(session=session)

        lock_name = 'lock'
        version_number = '1'
        backend.delete(lock_name, version_number)

        mock_table.delete_item.called_with(
            Key={'lockKey': lock_name},
            ConditionExpression=Attr('versionNumber').eq(version_number),
        )

    def test_can_delete_non_existant_lock(self, backend_factory):
        session = mock.Mock(spec=Session)
        mock_table = mock.Mock()
        mock_table.delete_item.side_effect = ResourceNotFoundException()

        mock_resource = mock.Mock()
        mock_resource.Table.return_value = mock_table

        session.resource.return_value = mock_resource
        backend = backend_factory(session=session)

        lock_name = 'lock'
        version_number = '1'
        backend.delete(lock_name, version_number)

        mock_table.delete_item.called_with(
            Key={'lockKey': lock_name},
            ConditionExpression=Attr('versionNumber').eq(version_number),
        )

    def test_does_raise_deleting_stolen_lock(self, backend_factory):
        session = mock.Mock(spec=Session)
        mock_table = mock.Mock()
        mock_table.delete_item.side_effect = ConditionalCheckFailedException()

        mock_resource = mock.Mock()
        mock_resource.Table.return_value = mock_table

        session.resource.return_value = mock_resource
        backend = backend_factory(session=session)

        lock_name = 'lock'
        version_number = '1'
        with pytest.raises(LockLostError):
            backend.delete(lock_name, version_number)

        mock_table.delete_item.called_with(
            Key={'lockKey': lock_name},
            ConditionExpression=Attr('versionNumber').eq(version_number),
        )

    def test_can_write_lock(self, backend_factory):
        session = mock.Mock(spec=Session)
        mock_table = mock.Mock()

        mock_resource = mock.Mock()
        mock_resource.Table.return_value = mock_table

        session.resource.return_value = mock_resource
        backend = backend_factory(session=session)

        lock_name = 'lock'
        duration = 5
        identifier = 'foobar'
        new_version_number ='2'
        expected_version_number = '1'

        backend.try_write_lock(
            lock_name,
            duration,
            identifier,
            new_version_number,
            expected_version_number,
        )

        lock_free = Attr('lockKey').not_exists()
        lock_expired = Attr('versionNumber').eq(expected_version_number)
        expr = (lock_free | lock_expired)
        mock_table.put_item.assert_called_with(
            Item={
                'lockKey': lock_name,
                'leaseDuration': duration,
                'hostIdentifier': identifier,
                'versionNumber': new_version_number,
            },
            ConditionExpression=expr,
        )

    def test_does_raise_if_lock_not_available(self, backend_factory):
        session = mock.Mock(spec=Session)
        mock_table = mock.Mock()
        mock_table.put_item.side_effect = ConditionalCheckFailedException()
        mock_table.get_item.return_value = {
            'Item': {
                'leaseDuration': 100,
                'versionNumber': 'old-version-number',
            }
        }

        mock_resource = mock.Mock()
        mock_resource.Table.return_value = mock_table

        session.resource.return_value = mock_resource
        backend = backend_factory(session=session)

        lock_name = 'lock'
        duration = 5
        identifier = 'foobar'
        new_version_number ='2'
        expected_version_number = '1'

        with pytest.raises(LockAlreadyInUseError) as e:
            backend.try_write_lock(
                lock_name,
                duration,
                identifier,
                new_version_number,
                expected_version_number,
            )

        lock_free = Attr('lockKey').not_exists()
        lock_expired = Attr('versionNumber').eq(expected_version_number)
        expr = (lock_free | lock_expired)
        mock_table.put_item.assert_called_with(
            Item={
                'lockKey': lock_name,
                'leaseDuration': duration,
                'hostIdentifier': identifier,
                'versionNumber': new_version_number,
            },
            ConditionExpression=expr,
        )
        assert e.value.lease_duration == 100
        assert e.value.version_number == 'old-version-number'

    def test_can_write_new_lock(self, backend_factory):
        session = mock.Mock(spec=Session)
        mock_table = mock.Mock()

        mock_resource = mock.Mock()
        mock_resource.Table.return_value = mock_table

        session.resource.return_value = mock_resource
        backend = backend_factory(session=session)

        lock_name = 'lock'
        duration = 5
        identifier = 'foobar'
        new_version_number ='2'
        expected_version_number = '1'

        backend.try_write_new_lock(
            lock_name,
            duration,
            identifier,
            new_version_number,
        )

        mock_table.put_item.assert_called_with(
            Item={
                'lockKey': lock_name,
                'leaseDuration': duration,
                'hostIdentifier': identifier,
                'versionNumber': new_version_number,
            },
            ConditionExpression=Attr('lockKey').not_exists(),
        )


    def test_does_raise_if_new_lock_not_available(self, backend_factory):
        session = mock.Mock(spec=Session)
        mock_table = mock.Mock()
        mock_table.put_item.side_effect = ConditionalCheckFailedException()
        mock_table.get_item.return_value = {
            'Item': {
                'leaseDuration': 100,
                'versionNumber': 'old-version-number',
            }
        }

        mock_resource = mock.Mock()
        mock_resource.Table.return_value = mock_table

        session.resource.return_value = mock_resource
        backend = backend_factory(session=session)

        lock_name = 'lock'
        duration = 5
        identifier = 'foobar'
        new_version_number ='2'

        with pytest.raises(LockAlreadyInUseError) as e:
            backend.try_write_new_lock(
                lock_name,
                duration,
                identifier,
                new_version_number,
            )

        lock_free = Attr('lockKey').not_exists()
        mock_table.put_item.assert_called_with(
            Item={
                'lockKey': lock_name,
                'leaseDuration': duration,
                'hostIdentifier': identifier,
                'versionNumber': new_version_number,
            },
            ConditionExpression=lock_free,
        )
        assert e.value.lease_duration == 100
        assert e.value.version_number == 'old-version-number'
