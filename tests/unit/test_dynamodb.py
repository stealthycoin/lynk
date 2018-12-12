import pytest
import mock
from boto3.session import Session
from boto3.dynamodb.conditions import Attr

from lynk.backends.dynamodb import DynamoDBBackend
from lynk.backends.dynamodb import DynamoDBControl
from lynk.backends.dynamodb import DynamoDBVersionLeaseBridge
from lynk.backends.dynamodb import DynamoDBBackendBridgeFactory


class ResourceNotFoundException(Exception):
    pass


class ConditionalCheckFailedException(Exception):
    pass


class Exceptions(object):
    def __init__(self):
        self.ResourceNotFoundException = ResourceNotFoundException
        self.ConditionalCheckFailedException = ConditionalCheckFailedException


@pytest.fixture
def ddb_control_factory():
    def wrapped(table_name=None, session=None):
        if table_name is None:
            table_name = 'table_name'
        if session is None:
            session = mock.Mock(spec=Session)
        control = DynamoDBControl(table_name, session)
        return control
    return wrapped


@pytest.fixture
def backend_factory():
    def wrapped():
        mock_table = mock.Mock()
        backend = DynamoDBBackend(mock_table)
        return mock_table, backend
    return wrapped


class TestControlPlane(object):
    def test_can_check_if_table_exists(self, ddb_control_factory):
        session = mock.Mock(spec=Session)
        mock_client = mock.Mock()
        mock_client.exceptions = Exceptions()
        session.client.return_value = mock_client
        control = ddb_control_factory(session=session)

        exists = control.exists()
        mock_client.describe_table.assert_called_with(TableName='table_name')
        assert exists is True

    def test_can_check_if_table_not_exists(self, ddb_control_factory):
        session = mock.Mock(spec=Session)
        mock_client = mock.Mock()
        mock_client.exceptions = Exceptions()
        mock_client.describe_table.side_effect = ResourceNotFoundException()
        session.client.return_value = mock_client
        control = ddb_control_factory(session=session)

        exists = control.exists()
        mock_client.describe_table.assert_called_with(TableName='table_name')
        assert exists is False

    def test_can_create_table(self, ddb_control_factory):
        session = mock.Mock(spec=Session)
        mock_waiter = mock.Mock()
        mock_client = mock.Mock()
        mock_client.get_waiter.return_value = mock_waiter
        mock_client.create_table.return_value = {
            'TableDescription': {'TableArn': 'arn'},
        }

        session.client.return_value = mock_client
        control = ddb_control_factory(session=session)

        control.create()
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

    def test_can_destroy_table(self, ddb_control_factory):
        session = mock.Mock(spec=Session)
        mock_waiter = mock.Mock()
        mock_client = mock.Mock()
        mock_client.get_waiter.return_value = mock_waiter
        session.client.return_value = mock_client
        control = ddb_control_factory(session=session)

        control.destroy()
        mock_client.delete_table.assert_called_with(
            TableName='table_name',
        )
        mock_client.get_waiter.assert_called_with('table_not_exists')
        mock_waiter.wait.assert_called_with(TableName='table_name')


class TestDynamoDBBackendBridgeFactory(object):
    def test_can_create(self):
        factory = DynamoDBBackendBridgeFactory()
        bridge, backend = factory.create('table_name')
        assert isinstance(bridge, DynamoDBVersionLeaseBridge)
        assert isinstance(backend, DynamoDBBackend)


class TestDyanmoDBBackend(object):
    def test_can_put(self, backend_factory):
        table, backend = backend_factory()
        backend.put({'key': 'value'})

        table.put_item.assert_called_with(
            Item={'key': 'value'},
        )

    def test_can_put_with_condition(self, backend_factory):
        table, backend = backend_factory()
        backend.put({'key': 'value'}, condition='foo')

        table.put_item.assert_called_with(
            Item={'key': 'value'},
            ConditionExpression='foo',
        )

    def test_can_update(self, backend_factory):
        table, backend = backend_factory()
        backend.update({'key': 'value'}, updates={'attribute': 'new value'})

        table.update_item.assert_called_with(
            Key={'key': 'value'},
            UpdateExpression='set attribute = :a',
            ExpressionAttributeValues={
                ':a': 'new value',
            }
        )

    def test_can_update_with_condition(self, backend_factory):
        table, backend = backend_factory()
        backend.update({'key': 'value'}, updates={'attribute': 'new value'},
                       condition='foo')

        table.update_item.assert_called_with(
            Key={'key': 'value'},
            UpdateExpression='set attribute = :a',
            ExpressionAttributeValues={
                ':a': 'new value',
            },
            ConditionExpression='foo',
        )

    def test_can_update_mulitple_attributes(self, backend_factory):
        table, backend = backend_factory()
        backend.update(
            {'key': 'value'},
            updates={
                'attribute': 'new value',
                'baz': 'buz',
            },
        )

        table.update_item.assert_called_with(
            Key={'key': 'value'},
            UpdateExpression='set attribute = :a, baz = :b',
            ExpressionAttributeValues={
                ':a': 'new value',
                ':b': 'buz',
            }
        )

    def test_can_delete(self, backend_factory):
        table, backend = backend_factory()
        backend.delete({'key': 'value'})

        table.delete_item.assert_called_with(
            Key={'key': 'value'},
        )

    def test_can_delete_with_condition(self, backend_factory):
        table, backend = backend_factory()
        backend.delete({'key': 'value'}, condition='foo')

        table.delete_item.assert_called_with(
            Key={'key': 'value'},
            ConditionExpression='foo',
        )

    def test_can_get(self, backend_factory):
        table, backend = backend_factory()
        table.get_item.return_value = {
            'Item': {
                'foo': 'bar',
            }
        }
        result = backend.get({'key': 'value'}, attributes=['foo'])

        table.get_item.assert_called_with(
            Key={'key': 'value'},
            AttributesToGet=['foo'],
            ConsistentRead=True,
        )
        assert result == {'foo': 'bar'}

    def test_can_get_no_result(self, backend_factory):
        table, backend = backend_factory()
        table.get_item.return_value = {}
        result = backend.get({'key': 'value'}, attributes=['foo'])

        table.get_item.assert_called_with(
            Key={'key': 'value'},
            AttributesToGet=['foo'],
            ConsistentRead=True,
        )
        assert result is None


class TestDynamoDBVersionLeaseBridge(object):
    def test_lock_free(self):
        mock_resource = mock.Mock()
        bridge = DynamoDBVersionLeaseBridge(mock_resource)
        expr = bridge.lock_free()
        assert expr == Attr('lockKey').not_exists()

    def test_lock_expired(self):
        mock_resource = mock.Mock()
        bridge = DynamoDBVersionLeaseBridge(mock_resource)
        expr = bridge.lock_expired('version')
        assert expr == Attr('versionNumber').eq('version')

    def test_lock_free_or_expired(self):
        mock_resource = mock.Mock()
        bridge = DynamoDBVersionLeaseBridge(mock_resource)
        expr = bridge.lock_free_or_expired('version')
        assert expr == (
            Attr('lockKey').not_exists() | Attr('versionNumber').eq('version')
        )

    def test_we_own_lock(self):
        mock_resource = mock.Mock()
        bridge = DynamoDBVersionLeaseBridge(mock_resource)
        expr = bridge.we_own_lock('version')
        assert expr == Attr('versionNumber').eq('version')
