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
        os.environ['TABLE_NAME'] = 'testing'

    def tearDown(self):
        pass

    def setup_mock_database(self):
        dynamodb = boto3.resource('dynamodb', region_name='eu-west-1')
        table_connection = dynamodb.create_table(TableName=os.environ['TABLE_NAME'],
                                      KeySchema=[{'AttributeName': 'resource_identifier', 'KeyType': 'HASH'},
                                                 {'AttributeName': 'modifiedDate', 'KeyType': 'RANGE'}],
                                      AttributeDefinitions=[
                                          {'AttributeName': 'resource_identifier', 'AttributeType': 'S'},
                                          {'AttributeName': 'modifiedDate', 'AttributeType': 'S'}],
                                      ProvisionedThroughput={'ReadCapacityUnits': 1, 'WriteCapacityUnits': 1})
        table_connection.put_item(
            Item={
                'resource_identifier': 'ebf20333-35a5-4a06-9c58-68ea688a9a8b',
                'modifiedDate': '2019-10-24T12:57:02.655994Z',
                'createdDate': '2019-10-24T12:57:02.655994Z',
                'metadata': {
                    'titles': {
                        'no': 'En tittel'
                    }
                }
            }
        )
        return dynamodb

    def remove_mock_database(self, dynamodb):
        dynamodb.Table(os.environ['TABLE_NAME']).delete()

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

    def test_handler_insert_resource(self):
        from src.classes.RequestHandler import RequestHandler
        dynamodb = self.setup_mock_database()
        request_handler = RequestHandler(dynamodb)
        event = {
            "body": "{\"operation\": \"INSERT\",\"resource\": {\"metadata\": {\"titles\": {\"no\": \"En tittel\","
                    "\"en\": \"A title\"}}}} "
        }
        handler_insert_response = request_handler.handler(event, None)
        self.assertEqual(handler_insert_response['statusCode'], 201, 'HTTP Status code not 201')
        self.remove_mock_database(dynamodb)

    def test_handler_modify_resource(self):
        from src.classes.RequestHandler import RequestHandler
        dynamodb = self.setup_mock_database()
        request_handler = RequestHandler(dynamodb)
        event = {
            "body": "{\"operation\": \"MODIFY\",\"resource\": {\"resource_identifier\": "
                    "\"ebf20333-35a5-4a06-9c58-68ea688a9a8b\", \"metadata\": {\"titles\": {\"no\": \"En tittel\","
                    "\"en\": \"A title\"}}}} "
        }
        handler_modify_response = request_handler.handler(event, None)
        self.assertEqual(handler_modify_response['statusCode'], 200, 'HTTP Status code not 200')
        self.remove_mock_database(dynamodb)

    def test_handler_modify_resource_missing_metadata(self):
        from src.classes.RequestHandler import RequestHandler
        dynamodb = self.setup_mock_database()
        request_handler = RequestHandler(dynamodb)
        event = {
            "body": "{\"operation\": \"MODIFY\",\"resource\": {\"resource_identifier\": "
                    "\"ebf20333-35a5-4a06-9c58-68ea688a9a8b\"}} "
        }
        handler_modify_response = request_handler.handler(event, None)
        self.assertEqual(handler_modify_response['statusCode'], 400, 'HTTP Status code not 400')
        self.remove_mock_database(dynamodb)

    # def test_handler_modify_resource_empty_metadata(self):
    #     from src.classes.RequestHandler import RequestHandler
    #     dynamodb = self.setup_mock_database()
    #     request_handler = RequestHandler(dynamodb)
    #     event = {
    #         "body": "{\"operation\": \"MODIFY\",\"resource\": {\"resource_identifier\": "
    #                 "\"ebf20333-35a5-4a06-9c58-68ea688a9a8b\", \"metadata\": {}}}} "
    #     }
    #     handler_modify_response = request_handler.handler(event, None)
    #     self.assertEqual(handler_modify_response['statusCode'], 400, 'HTTP Status code not 400')
    #     self.remove_mock_database(dynamodb)

    def test_handler_modify_resource_wrong_id(self):
        from src.classes.RequestHandler import RequestHandler
        dynamodb = self.setup_mock_database()
        request_handler = RequestHandler(dynamodb)
        event = {
            "body": "{\"operation\": \"MODIFY\",\"resource\": {\"resource_identifier\": "
                    "\"WRONG_ID\", \"metadata\": {\"titles\": {\"no\": \"En tittel\","
                    "\"en\": \"A title\"}}}} "
        }
        handler_modify_response = request_handler.handler(event, None)
        self.assertEqual(handler_modify_response['statusCode'], 400, 'HTTP Status code not 400')
        self.assertEqual(handler_modify_response['body'], "resource with identifier WRONG_ID not found", 'Did not get expected error message')
        self.remove_mock_database(dynamodb)

    def test_unknown_operation(self):
        from src.classes.RequestHandler import RequestHandler
        dynamodb = self.setup_mock_database()
        request_handler = RequestHandler(dynamodb)
        event = {
            "body": "{\"operation\": \"UNKNOWN_OPERATION\",\"resource\": {\"resource_identifier\": "
                    "\"ebf20333-35a5-4a06-9c58-68ea688a9a8b\", \"metadata\": {\"titles\": {\"no\": \"En tittel\","
                    "\"en\": \"A title\"}}}} "
        }
        handler_modify_response = request_handler.handler(event, None)
        self.assertEqual(handler_modify_response['statusCode'], 400, 'HTTP Status code not 400')
        self.remove_mock_database(dynamodb)

    def test_missing_resource(self):
        from src.classes.RequestHandler import RequestHandler
        dynamodb = self.setup_mock_database()
        request_handler = RequestHandler(dynamodb)
        event = {
            "body": "{\"operation\": \"INSERT\"} "
        }
        handler_modify_response = request_handler.handler(event, None)
        self.assertEqual(handler_modify_response['statusCode'], 400, 'HTTP Status code not 400')
        self.remove_mock_database(dynamodb)

    def test_missing_operation(self):
        from src.classes.RequestHandler import RequestHandler
        dynamodb = self.setup_mock_database()
        request_handler = RequestHandler(dynamodb)
        event = {
            "body": "{\"resource\": {\"resource_identifier\": "
                    "\"ebf20333-35a5-4a06-9c58-68ea688a9a8b\", \"metadata\": {\"titles\": {\"no\": \"En tittel\","
                    "\"en\": \"A title\"}}}} "
        }
        handler_modify_response = request_handler.handler(event, None)
        self.assertEqual(handler_modify_response['statusCode'], 400, 'HTTP Status code not 400')
        self.remove_mock_database(dynamodb)

    def test_insert_resource(self):
        from src.classes.RequestHandler import RequestHandler
        dynamodb = self.setup_mock_database()
        request_handler = RequestHandler(dynamodb)

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
        self.remove_mock_database(dynamodb)

    def test_modify_resource(self):
        from src.classes.RequestHandler import RequestHandler
        dynamodb = self.setup_mock_database()
        request_handler = RequestHandler(dynamodb)

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
        self.remove_mock_database(dynamodb)


if __name__ == '__main__':
    unittest.main()
