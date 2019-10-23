import random
import string
import boto3
import uuid as uuid
import arrow as arrow
from boto3.dynamodb.conditions import Key
from moto import mock_dynamodb2
import unittest
from src import app


class TestHandlerCase(unittest.TestCase):
    table_name = 'nva-test'
    dynamodb_client = boto3.client('dynamodb', region_name='eu-west-1', endpoint_url='http://localhost:8000')
    dynamodb_resource = boto3.resource('dynamodb', region_name='eu-west-1', endpoint_url='http://localhost:8000')

    table_connection = None

    def setUp(self):

        # self.dynamodb_client.delete_table(TableName=self.table_name)
        self.table_connection = self.dynamodb_resource.create_table(TableName=self.table_name,
                                                                    KeySchema=[
                                                                        {'AttributeName': 'resource_identifier',
                                                                         'KeyType': 'HASH'},
                                                                        {'AttributeName': 'modifiedDate',
                                                                         'KeyType': 'RANGE'}],
                                                                    AttributeDefinitions=[
                                                                        {'AttributeName': 'resource_identifier',
                                                                         'AttributeType': 'S'},
                                                                        {'AttributeName': 'modifiedDate',
                                                                         'AttributeType': 'S'}],
                                                                    ProvisionedThroughput={'ReadCapacityUnits': 1,
                                                                                           'WriteCapacityUnits': 1})

    def tearDown(self):
        self.dynamodb_client.delete_table(TableName=self.table_name)

    @mock_dynamodb2
    def insert_resource(self, uuid, time_created, resource):
        return app.insert_resource(uuid, time_created, resource)

    @mock_dynamodb2
    def query_resource(self, uuid):
        return self.table_connection.query(
            TableName=self.table_name,
            KeyConditionExpression=Key('resource_identifier').eq(
                uuid)
        )

    def generate_random_resource(self, time_created, time_modified=None, uuid=uuid.uuid4().__str__()):
        if (time_modified is None):
            time_modified = time_created
        return {
            'resource_identifier': uuid,
            'modifiedDate': time_modified,
            'createdDate': time_created,
            'metadata': {
                'titles': {
                    'no': self.random_word(6)
                }
            }
        }

    def random_word(self, length):
        letters = string.ascii_lowercase
        return ''.join(random.choice(letters) for i in range(length))

    @mock_dynamodb2
    def test_insert_resource(self):
        test_time_created = arrow.utcnow().isoformat().replace("+00:00", "Z")

        test_resource_insert = self.generate_random_resource(test_time_created)
        test_uuid = test_resource_insert['resource_identifier']

        insert_response = self.insert_resource(test_uuid, test_time_created, test_resource_insert)

        self.assertEqual(insert_response['ResponseMetadata']['HTTPStatusCode'], 200, 'HTTP Status code not 200')

        query_results = self.query_resource(test_uuid)

        newest_resource = query_results['Items'][0]
        self.assertEqual(newest_resource['modifiedDate'], test_time_created, 'Value not persisted as expected')
        self.assertEqual(newest_resource['createdDate'], test_time_created, 'Value not persisted as expected')
        self.assertEqual(newest_resource['metadata'], test_resource_insert['metadata'],
                         'Value not persisted as expected')

    @mock_dynamodb2
    def test_modify_resource(self):

        test_time_created = arrow.utcnow().isoformat().replace("+00:00", "Z")
        test_resource_initial = self.generate_random_resource(test_time_created)

        uuid = test_resource_initial['resource_identifier']
        self.insert_resource(uuid, test_time_created, test_resource_initial)

        for counter in range(2):
            generated_resource_modified = self.generate_random_resource(test_time_created, None, uuid)
            test_time_modified = arrow.utcnow().isoformat().replace("+00:00", "Z")
            modify_response = app.modify_resource(test_time_modified, generated_resource_modified)
            self.assertEqual(modify_response['ResponseMetadata']['HTTPStatusCode'], 200, 'HTTP Status code not 200')

        query_results = self.query_resource(uuid)

        self.assertEqual(len(query_results['Items']), 3, 'Value not persisted as expected')

        initial_resource = query_results['Items'][0]
        first_modification_resource = query_results['Items'][1]
        second_modification_resource = query_results['Items'][2]
        # self.assertEqual(newest_resource['modifiedDate'], test_time_modified, 'Value not persisted as expected')
        # self.assertNotEqual(newest_resource['modifiedDate'], test_time_created, 'Value not persisted as expected')
        # self.assertEqual(newest_resource['createdDate'], test_time_created, 'Value not persisted as expected')
        # self.assertNotEqual(newest_resource['metadata'], test_resource_original['metadata'],
        #                     'Value not persisted as expected')


if __name__ == '__main__':
    unittest.main()
