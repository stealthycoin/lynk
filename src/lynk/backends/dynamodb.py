import string

import boto3
from boto3.dynamodb.conditions import Attr
from lynk.backends.base import BaseBackend


class DynamoDBControl(object):
    """Class used to interact with the control plane of DynamoDB."""
    def __init__(self, table_name, session=None):
        """Initialize DynamoDBControl.

        :type table_name: str
        :param table_name: The name of the DynamoDB table.

        :type session: :class:`boto3.session.Session` or None
        :param session: The session to use constructing a dynamodb client.
            By default a new session is created, which will use the standard
            boto3 AWS credential chain to find credentials.
        """
        self._table_name = table_name
        if session is None:
            session = boto3.Session()
        self._client = session.client('dynamodb')
        self._tags = session.client('resourcegroupstaggingapi')

    def create(self):
        """Create DynamoDB table for use as a lynk backend.

        Blocks until the table is created.
        """
        response = self._client.create_table(
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

        table_arn = response['TableDescription']['TableArn']
        self._client.tag_resource(
            ResourceArn=table_arn,
            Tags=[
                {
                    'Key': 'lynk',
                    'Value': 'lock-table',
                }
            ]
        )

    def destroy(self):
        """Destroy DymamoDB table.

        Blocks until the table is destroyed.
        """
        self._client.delete_table(
            TableName=self._table_name,
        )
        waiter = self._client.get_waiter('table_not_exists')
        waiter.wait(TableName=self._table_name)

    def exists(self):
        """Check if the DynamoDB table is created."""
        try:
            self._client.describe_table(TableName=self._table_name)
            return True
        except self._client.exceptions.ResourceNotFoundException:
            return False

    def find(self):
        response = self._tags.get_resources(
            TagFilters=[
                {
                    'Key': 'lynk',
                    'Values': ['lock-table'],
                }
            ]
        )
        arns = [r['ResourceARN'] for r in response['ResourceTagMappingList']]
        names = [a[a.rfind('/')+1:] for a in arns]
        return names


class DynamoDBBackendBridgeFactory(object):
    def create(self, table_name, session=None):
        if session is None:
            session = boto3.session.Session()
        resource = session.resource('dynamodb')
        table = resource.Table(table_name)
        bridge = DynamoDBVersionLeaseBridge(resource)
        backend = DynamoDBBackend(table)
        return bridge, backend


class DynamoDBVersionLeaseBridge(object):
    """Acts as a bridge between DynamoDBBackend and VersionLeaseTechinque.

    This class is coupled to :class:`lynk.techniques.VersionLeaseTechinque` and
    :class:`lynk.backend.DynamoDBBackend`. It serves as a way to express the
    high-level concepts required by the VersionLeaseTechinque as low-level
    concepts consumed by the DynamoDBBackend.
    """
    def __init__(self, resource):
        """
        :type session: :class:`boto3.session.Session` or none
        :param session: The session to use constructing a dynamodb client.
        """
        self._client = resource.meta.client
        self.ConditionFailedError = \
            self._client.exceptions.ConditionalCheckFailedException
        self.NoSuchLockError = \
            self._client.exceptions.ResourceNotFoundException

    def lock_free(self):
        """Build the condition that the lock is currently free.

        In the DynamoDBBackend this means the item does not currenlty exist.
        """
        lock_free = Attr('lockKey').not_exists()
        return lock_free

    def lock_expired(self, version_number):
        """Build the condition that the lock has expired.

        In the DynamoDBBackend if the verison number has not changed since the
        last time an agent checked on it, then it has expired since we waited
        the leaseDuration and it didn't get refreshed, stolen by another agent
        or deleted.

        :type version_number: str
        :param version_number: The version number that the agent remembers from
            last time it interacted with the lock (failed write). If it
            matches what is in the table then it has not been refreshed/stolen
            so it is free for us to steal.
        """
        lock_expired = Attr('versionNumber').eq(version_number)
        return lock_expired

    def lock_free_or_expired(self, version_number):
        """Build the condition that a lock is free or expired.

        :type version_number: str
        :param version_number: The version number that the agent remembers from
            last time it interacted with the lock (failed write). If it
            matches what is in the table then it has not been refreshed/stolen
            so it is free for us to steal.
        """
        return self.lock_free() | self.lock_expired(version_number)

    def we_own_lock(self, version_number):
        """Build the condition that "we" own this lock.

        Ownership over a lock is asserted by matching the version_number with
        the one "we" (the agent making the request) have. If they match, then
        the agent owns the lock and can refresh/delete it as needed.

        Note::

          This is implemented the same as
          :meth:lynk.backend.DynamoDBVersionLeaseBridge.lock_expired` however
          since it has a differnet symantic meaning it is codified here as it's
          own method.

        :type version_number: str
        :param version_number: The version number that the agent remembers from
            last time it interacted with the lock (write, refresh). If it
            matches what is in the table then this agent still owns it.
        """
        own_lock = Attr('versionNumber').eq(version_number)
        return own_lock


class DynamoDBBackend(BaseBackend):
    def __init__(self, table):
        """Initialize a DynamoDBBackend.

        :type table: boto3 table resource
        :param table: The boto3 table object to operate on.
        """
        self._table = table

    def put(self, item, condition=None):
        """Put an item into the DynamoDB table."""
        arguments = {
            'Item': item,
        }
        if condition:
            arguments['ConditionExpression'] = condition
        self._table.put_item(**arguments)

    def update(self, key, updates, condition=None):
        """Update an item in the DynamoDB table.

        :type key: dict
        :param key: The key to update in the table.

        :type updates: dict
        :param updates: A dictionary of attribute -> new_value updates to make
            to the item. These will be used to create a UpdateExpression and
            ExpressionAttributeValues.

        :type condition: Condition expression or None
        :param condition: A boto3 dynamodb condition expression to put on the
           update.
        """
        update_exprs = []
        update_vals = {}
        for i, (attr, value) in enumerate(updates.items()):
            letter = string.ascii_letters[i]
            update_exprs.append('%s = :%s' % (attr, letter))
            update_vals[':%s' % letter] = value
        update_expr = 'set %s' % ', '.join(update_exprs)
        arguments = {
            'Key': key,
            'UpdateExpression': update_expr,
            'ExpressionAttributeValues': update_vals,
        }
        if condition:
            arguments['ConditionExpression'] = condition
        self._table.update_item(**arguments)

    def delete(self, key, condition=None):
        """Delete an item from the DynamoDB table."""
        arguments = {
            'Key': key,
        }
        if condition:
            arguments['ConditionExpression'] = condition
        self._table.delete_item(**arguments)

    def get(self, key, attributes):
        """Get an item from the DynamoDB Table.

        :type name: dict
        :param name: The key to look up.

        :type attributes: list
        :param attributes: List of attributes to get from the stored item.

        :rvalue: dict
        :returns: A dictionary of attributeName -> attributeValue for each
            attribute in the ``attributes`` list.
        """
        result = self._table.get_item(
            Key=key,
            AttributesToGet=attributes,
            ConsistentRead=True,
        )

        if 'Item' not in result:
            return None
        item = result['Item']
        return {attr: item[attr] for attr in attributes}
