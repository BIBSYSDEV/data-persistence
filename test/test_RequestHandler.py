import os
import random
import string
import unittest

import boto3
import uuid
from boto3.dynamodb.conditions import Key
from moto import mock_dynamodb2
import arrow as arrow


@mock_dynamodb2
class TestHandlerCase(unittest.TestCase):

    def setUp(self):
        """Mocked AWS Credentials for moto."""
        os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
        os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'
        os.environ['AWS_SECURITY_TOKEN'] = 'testing'
        os.environ['AWS_SESSION_TOKEN'] = 'testing'

    def tearDown(self):
        pass

    def setup_mock_database(self, table_name):
        dynamodb = boto3.resource('dynamodb', region_name='eu-west-1')
        dynamodb.create_table(TableName=table_name,
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
        return dynamodb

    def delete_mock_database(self, dynamodb, table_name):
        dynamodb = boto3.resource('dynamodb', region_name='eu-west-1')
        dynamodb.create_table(TableName=table_name,
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
        return dynamodb

    def remove_mock_database(self, dynamodb, table_name):
        dynamodb.Table(table_name).delete()

    def generate_random_resource(self, time_created, time_modified=None, uuid=uuid.uuid4().__str__()):
        if time_modified is None:
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

    def test_insert_resource(self):
        from src.classes.RequestHandler import RequestHandler
        table_name = 'testing'
        dynamodb = self.setup_mock_database(table_name)
        request_handler = RequestHandler(dynamodb, table_name)

        test_time_created = arrow.utcnow().isoformat().replace("+00:00", "Z")
        test_resource_insert = self.generate_random_resource(test_time_created)
        test_uuid = test_resource_insert['resource_identifier']

        insert_response = request_handler.insert_resource(test_uuid, test_time_created, test_resource_insert)

        self.assertEqual(insert_response['ResponseMetadata']['HTTPStatusCode'], 200, 'HTTP Status code not 200')

        query_results = request_handler.get_table_connection().query(
            KeyConditionExpression=Key('resource_identifier').eq(test_uuid),
            ScanIndexForward=True
        )

        inserted_resource = query_results['Items'][0]
        self.assertEqual(inserted_resource['modifiedDate'], test_time_created, 'Value not persisted as expected')
        self.assertEqual(inserted_resource['createdDate'], test_time_created, 'Value not persisted as expected')
        self.assertEqual(inserted_resource['metadata'], test_resource_insert['metadata'],
                         'Value not persisted as expected')
        self.remove_mock_database(dynamodb, table_name)

    def test_modify_resource(self):
        from src.classes.RequestHandler import RequestHandler
        table_name = 'testing'
        dynamodb = self.setup_mock_database(table_name)
        request_handler = RequestHandler(dynamodb, table_name)

        test_time_created = arrow.utcnow().isoformat().replace("+00:00", "Z")
        test_resource_initial = self.generate_random_resource(test_time_created)
        test_uuid = test_resource_initial['resource_identifier']

        request_handler.insert_resource(test_uuid, test_time_created, test_resource_initial)
        for counter in range(2):
            generated_resource_modified = self.generate_random_resource(test_time_created, None, test_uuid)
            test_time_modified = arrow.utcnow().isoformat().replace("+00:00", "Z")
            modify_response = request_handler.modify_resource(test_time_modified, generated_resource_modified)
            self.assertEqual(modify_response['ResponseMetadata']['HTTPStatusCode'], 200, 'HTTP Status code not 200')

        query_results = request_handler.get_table_connection().query(
            KeyConditionExpression=Key('resource_identifier').eq(test_uuid),
            ScanIndexForward=True
        )

        self.assertEqual(len(query_results['Items']), 3, 'Value not persisted as expected')

        initial_resource = query_results['Items'][0]
        first_modification_resource = query_results['Items'][1]
        second_modification_resource = query_results['Items'][2]

        self.assertEqual(initial_resource['createdDate'], test_time_created, 'Value not persisted as expected')
        self.assertEqual(first_modification_resource['createdDate'], test_time_created,
                         'Value not persisted as expected')
        self.assertEqual(second_modification_resource['createdDate'], test_time_created,
                         'Value not persisted as expected')
        self.assertEqual(initial_resource['modifiedDate'], test_time_created, 'Value not persisted as expected')
        self.assertNotEqual(first_modification_resource['modifiedDate'], test_time_created,
                            'Value not persisted as expected')
        self.assertNotEqual(second_modification_resource['modifiedDate'], test_time_created,
                            'Value not persisted as expected')
        self.assertNotEqual(first_modification_resource['modifiedDate'], second_modification_resource['modifiedDate'],
                            'Value not persisted as expected')
        self.assertNotEqual(initial_resource['metadata'], first_modification_resource['metadata'],
                            'Value not persisted as expected')
        self.assertNotEqual(initial_resource['metadata'], second_modification_resource['metadata'],
                            'Value not persisted as expected')
        self.assertNotEqual(first_modification_resource['metadata'], second_modification_resource['metadata'],
                            'Value not persisted as expected')
        self.remove_mock_database(dynamodb, table_name)


if __name__ == '__main__':
    unittest.main()
