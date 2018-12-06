import boto3
from boto3.dynamodb.conditions import Attr

from lynk.lock import LockFactory
from lynk.backends import LockBackend
from lynk.exceptions import LockAlreadyInUseError
from lynk.exceptions import LockLostError


class DynamoDBLynk(object):
    def __init__(self, table_name, host_identifier=None):
        self._backend = DynamoBackend(table_name)
        self._factory = LockFactory(
            backend=self._backend,
            host_identifier=host_identifier,
        )

    def create_lock(self, lock_name):
        return self._factory.create_lock(lock_name)

    def create_auto_refreshing_lock(self, lock_name):
        return self._factory.create_auto_refreshing_lock(lock_name)

    def create_table(self):
        self._backend.create()


class DynamoBackend(LockBackend):
    def __init__(self, table_name, session=None):
        if session is None:
            session = boto3.Session()
        self._client = session.client('dynamodb')
        self._resource = session.resource('dynamodb')
        self._table = self._resource.Table(table_name)
        self._table_name = table_name
        self._exceptions = self._table.meta.client.exceptions

    @property
    def name(self):
        """Get the name of the table"""
        return self._table_name

    def exists(self):
        """Check if the DynamoDB table this client expects exists"""
        try:
            self._client.describe_table(TableName=self._table_name)
            return True
        except self._exceptions.ResourceNotFoundException:
            return False

    def create(self):
        """Create DynamoDB table"""
        self._client.create_table(
            TableName=self._table_name,
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
        waiter = self._client.get_waiter('table_exists')
        waiter.wait(TableName=self._table_name)

    def refresh_lock(self, key, old_version_number, new_version_number):
        try:
            self._table.update_item(
                Key={
                    'lockKey': key,
                },
                UpdateExpression='set versionNumber = :v',
                ExpressionAttributeValues={
                    ':v': new_version_number,
                },
                ConditionExpression=Attr('versionNumber').eq(
                    old_version_number,
                ),
            )
        except self._exceptions.ResourceNotFoundException:
            # This one shouldn't happen. This would mean that another client
            # stole the lock and then released it. LockLostError should be
            # raised here as we have to assume the resource we have beeen
            # operating on is no longer correct.
            raise LockLostError()
        except self._exceptions.ConditionalCheckFailedException:
            # We no longer own this lock. Raise the LockLostError since we need
            # to alert the client that they no longer own the resource.
            raise LockLostError()

    def delete(self, key, version_number):
        try:
            self._table.delete_item(
                Key={
                    'lockKey': key,
                },
                ConditionExpression=Attr('versionNumber').eq(
                    version_number)
            )
        except self._exceptions.ResourceNotFoundException:
            # Lock doesn't exist in the table.
            pass
        except self._exceptions.ConditionalCheckFailedException:
            # We no longer own this lock. Raise LockLostError since we cannot
            # delete it.
            raise LockLostError()

    def try_write_lock(self, key, duration, identifier, new_version_number,
                       expected_version_number):
        lock_free = Attr('lockKey').not_exists()
        lock_expired = Attr('versionNumber').eq(expected_version_number)
        try:
            self._table.put_item(
                Item={
                    'lockKey': key,
                    'leaseDuration': duration,
                    'hostIdentifier': identifier,
                    'versionNumber': new_version_number
                },
                ConditionExpression=(lock_free | lock_expired)
            )
        except self._exceptions.ConditionalCheckFailedException:
            # Some other lock changed the version number so we can't have it
            duration, record_number = self._get_lock_info(key)
            raise LockAlreadyInUseError(duration, record_number)

    def try_write_new_lock(self, key, lease_duration, identifier,
                           version_number):
        try:
            self._table.put_item(
                Item={
                    'lockKey': key,
                    'leaseDuration': lease_duration,
                    'hostIdentifier': identifier,
                    'versionNumber': version_number,
                },
                ConditionExpression=Attr('lockKey').not_exists()
            )
        except self._exceptions.ConditionalCheckFailedException:
            duration, record_number = self._get_lock_info(key)
            raise LockAlreadyInUseError(duration, record_number)

    def _get_lock_info(self, key):
        result = self._table.get_item(
            Key={
                'lockKey': key,
            },
            AttributesToGet=['leaseDuration', 'versionNumber'],
            ConsistentRead=True,
        )
        item = result['Item']
        return item['leaseDuration'], item['versionNumber']
