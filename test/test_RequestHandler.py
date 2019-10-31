import http
import json
import os
import random
import string
import unittest
import uuid

import arrow as arrow
import boto3
from boto3.dynamodb.conditions import Key
from moto import mock_dynamodb2

from common.constants import Constants
from common.encoders import encode_resource
from common.helpers import remove_none_values
from data.creator import Creator
from data.file import File
from data.file_metadata import FileMetadata
from data.metadata import Metadata
from data.resource import Resource
from data.title import Title


def unittest_lambda_handler(event, context):
    unittest.TextTestRunner().run(
        unittest.TestLoader().loadTestsFromTestCase(TestHandlerCase))


@mock_dynamodb2
class TestHandlerCase(unittest.TestCase):
    EXISTING_RESOURCE_IDENTIFIER = 'ebf20333-35a5-4a06-9c58-68ea688a9a8b'
    EXISTING_RESOURCE_IDENTIFIER_MISSING_CREATED_DATE = 'acf20333-35a5-4a06-9c58-68ea688a9a9c'

    def setUp(self):
        """Mocked AWS Credentials for moto."""
        os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
        os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'
        os.environ['AWS_SECURITY_TOKEN'] = 'testing'
        os.environ['AWS_SESSION_TOKEN'] = 'testing'
        os.environ[Constants.ENV_VAR_TABLE_NAME] = 'testing'
        os.environ[Constants.ENV_VAR_REGION] = 'eu-west-1'

    def tearDown(self):
        pass

    def setup_mock_database(self):
        dynamodb = boto3.resource('dynamodb', region_name=os.environ[Constants.ENV_VAR_REGION])
        table_connection = dynamodb.create_table(TableName=os.environ[Constants.ENV_VAR_TABLE_NAME],
                                                 KeySchema=[{'AttributeName': 'resource_identifier', 'KeyType': 'HASH'},
                                                            {'AttributeName': 'modifiedDate', 'KeyType': 'RANGE'}],
                                                 AttributeDefinitions=[
                                                     {'AttributeName': 'resource_identifier', 'AttributeType': 'S'},
                                                     {'AttributeName': 'modifiedDate', 'AttributeType': 'S'}],
                                                 ProvisionedThroughput={'ReadCapacityUnits': 1,
                                                                        'WriteCapacityUnits': 1})
        table_connection.put_item(
            Item={
                'resource_identifier': self.EXISTING_RESOURCE_IDENTIFIER,
                'modifiedDate': '2019-10-24T12:57:02.655994Z',
                'createdDate': '2019-10-24T12:57:02.655994Z',
                'metadata': {
                    'titles': {
                        'no': 'En tittel'
                    }
                },
                'files': {},
                'owner': 'owner@unit.no'
            }
        )

        table_connection.put_item(
            Item={
                'resource_identifier': self.EXISTING_RESOURCE_IDENTIFIER_MISSING_CREATED_DATE,
                'modifiedDate': '2019-10-24T12:57:02.655994Z',
                'metadata': {
                    'titles': {
                        'no': 'En tittel'
                    }
                },
                'files': {},
                'owner': 'owner@unit.no'
            }
        )

        return dynamodb

    def remove_mock_database(self, dynamodb):
        dynamodb.Table(os.environ[Constants.ENV_VAR_TABLE_NAME]).delete()

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
            },
            'files': {},
            'owner': 'owner@unit.no'
        }

    def random_word(self, length):
        letters = string.ascii_lowercase
        return ''.join(random.choice(letters) for i in range(length))

    def generate_mock_resource(self, time_created=None, time_modified=None, uuid=uuid.uuid4().__str__()):
        title_1 = Title('no', self.random_word(6))
        title_2 = Title('en', self.random_word(6))
        titles = {title_1.language_code: title_1.title, title_2.language_code: title_2.title}
        creator_one = Creator('AUTHORITY_IDENTIFIER_1')
        creator_two = Creator('AUTHORITY_IDENTIFIER_2')
        creators = [creator_one, creator_two]
        metadata = Metadata(creators, 'https://hdl.handle.net/11250.1/1', 'LICENSE_IDENTIFIER_1', '2019', 'Unit',
                            titles, 'text')
        file_metadata_1 = FileMetadata(self.random_word(6) + '.txt', 'text/plain', '595f44fec1e92a71d3e9e77456ba80d1',
                                       '987654321')
        file_metadata_2 = FileMetadata(self.random_word(6) + '.pdf', 'application/pdf',
                                       '71f920fa275127a7b60fa4d4d41432a3', '123456789')
        file_1 = File('FILE_IDENTIFIER_1', file_metadata_1)
        file_2 = File('FILE_IDENTIFIER_2', file_metadata_2)
        files = dict()
        files[file_1.identifier] = file_1.file_metadata
        files[file_2.identifier] = file_2.file_metadata
        return Resource(uuid, time_modified, time_created, metadata, files, 'owner@unit.no')

    def generate_mock_event(self, operation, resource):
        body = Body(operation, resource)
        body_value = json.dumps(body, default=encode_body)
        return {
            'body': body_value
        }

    def test_handler_insert_resource(self):
        from src.classes.RequestHandler import RequestHandler
        dynamodb = self.setup_mock_database()
        request_handler = RequestHandler(dynamodb)
        resource = self.generate_mock_resource()
        event = self.generate_mock_event(Constants.OPERATION_INSERT, resource)
        handler_insert_response = request_handler.handler(event, None)
        self.assertEqual(handler_insert_response[Constants.RESPONSE_STATUS_CODE], http.HTTPStatus.CREATED,
                         'HTTP Status code not 201')
        self.remove_mock_database(dynamodb)

    def test_handler_insert_resource_missing_resource_owner_in_event_body(self):
        from src.classes.RequestHandler import RequestHandler
        dynamodb = self.setup_mock_database()
        request_handler = RequestHandler(dynamodb)
        resource = self.generate_mock_resource()
        resource.owner = None
        event = self.generate_mock_event(Constants.OPERATION_INSERT, resource)
        handler_insert_response = request_handler.handler(event, None)
        self.assertEqual(handler_insert_response[Constants.RESPONSE_STATUS_CODE], http.HTTPStatus.BAD_REQUEST,
                         'HTTP Status code not 400')
        self.remove_mock_database(dynamodb)

    def test_handler_modify_resource(self):
        from src.classes.RequestHandler import RequestHandler
        dynamodb = self.setup_mock_database()
        request_handler = RequestHandler(dynamodb)
        resource = self.generate_mock_resource(None, None, self.EXISTING_RESOURCE_IDENTIFIER)
        event = self.generate_mock_event(Constants.OPERATION_MODIFY, resource)
        handler_modify_response = request_handler.handler(event, None)
        self.assertEqual(handler_modify_response[Constants.RESPONSE_STATUS_CODE], http.HTTPStatus.OK,
                         'HTTP Status code not 200')
        self.remove_mock_database(dynamodb)

    def test_handler_modify_resource_missing_resource_metadata_in_event_body(self):
        from src.classes.RequestHandler import RequestHandler
        dynamodb = self.setup_mock_database()
        request_handler = RequestHandler(dynamodb)
        resource = self.generate_mock_resource(None, None, self.EXISTING_RESOURCE_IDENTIFIER)
        resource.metadata = None
        event = self.generate_mock_event(Constants.OPERATION_MODIFY, resource)
        handler_modify_response = request_handler.handler(event, None)
        self.assertEqual(handler_modify_response[Constants.RESPONSE_STATUS_CODE], http.HTTPStatus.BAD_REQUEST,
                         'HTTP Status code not 400')
        self.remove_mock_database(dynamodb)

    def test_handler_modify_resource_missing_resource_owner_in_event_body(self):
        from src.classes.RequestHandler import RequestHandler
        dynamodb = self.setup_mock_database()
        request_handler = RequestHandler(dynamodb)
        resource = self.generate_mock_resource(None, None, self.EXISTING_RESOURCE_IDENTIFIER)
        resource.owner = None
        event = self.generate_mock_event(Constants.OPERATION_MODIFY, resource)
        handler_modify_response = request_handler.handler(event, None)
        self.assertEqual(handler_modify_response[Constants.RESPONSE_STATUS_CODE], http.HTTPStatus.BAD_REQUEST,
                         'HTTP Status code not 400')
        self.remove_mock_database(dynamodb)

    def test_handler_modify_resource_missing_resource_files_in_event_body(self):
        from src.classes.RequestHandler import RequestHandler
        dynamodb = self.setup_mock_database()
        request_handler = RequestHandler(dynamodb)
        resource = self.generate_mock_resource(None, None, self.EXISTING_RESOURCE_IDENTIFIER)
        resource.files = None
        event = self.generate_mock_event(Constants.OPERATION_MODIFY, resource)
        handler_modify_response = request_handler.handler(event, None)
        self.assertEqual(handler_modify_response[Constants.RESPONSE_STATUS_CODE], http.HTTPStatus.BAD_REQUEST,
                         'HTTP Status code not 400')
        self.remove_mock_database(dynamodb)

    def test_handler_modify_resource_empty_resource_metadata_in_event_body(self):
        from src.classes.RequestHandler import RequestHandler
        dynamodb = self.setup_mock_database()
        request_handler = RequestHandler(dynamodb)
        resource = self.generate_mock_resource(None, None, self.EXISTING_RESOURCE_IDENTIFIER)
        resource.metadata = Metadata(None, None, None, None, None, None)
        event = self.generate_mock_event(Constants.OPERATION_MODIFY, resource)
        handler_modify_response = request_handler.handler(event, None)
        self.assertEqual(handler_modify_response[Constants.RESPONSE_STATUS_CODE], http.HTTPStatus.OK,
                         'HTTP Status code not 200')
        self.remove_mock_database(dynamodb)

    def test_handler_modify_resource_invalid_resource_metadata_type_in_event_body(self):
        from src.classes.RequestHandler import RequestHandler
        dynamodb = self.setup_mock_database()
        request_handler = RequestHandler(dynamodb)
        event = {
            "body": "{\"operation\": \"MODIFY\",\"resource\": {\"resource_identifier\": "
                    "\"ebf20333-35a5-4a06-9c58-68ea688a9a8b\", \"owner\": \"owner@unit.no\", \"files\": {}, \"metadata\": \"invalid type\"}}"
        }
        handler_modify_response = request_handler.handler(event, None)
        self.assertEqual(handler_modify_response[Constants.RESPONSE_STATUS_CODE], http.HTTPStatus.BAD_REQUEST,
                         'HTTP Status code not 400')
        self.assertEqual(handler_modify_response[Constants.RESPONSE_BODY],
                         'Resource with identifier ebf20333-35a5-4a06-9c58-68ea688a9a8b has invalid attribute type for metadata',
                         'HTTP Status code not 400')
        self.remove_mock_database(dynamodb)

    def test_handler_modify_resource_invalid_files_type_in_event_body(self):
        from src.classes.RequestHandler import RequestHandler
        dynamodb = self.setup_mock_database()
        request_handler = RequestHandler(dynamodb)
        event = {
            "body": "{\"operation\": \"MODIFY\",\"resource\": {\"resource_identifier\": "
                    "\"ebf20333-35a5-4a06-9c58-68ea688a9a8b\", \"owner\": \"owner@unit.no\", \"files\": \"invalid type\", \"metadata\": {}}}"
        }
        handler_modify_response = request_handler.handler(event, None)
        self.assertEqual(handler_modify_response[Constants.RESPONSE_STATUS_CODE], http.HTTPStatus.BAD_REQUEST,
                         'HTTP Status code not 400')
        self.assertEqual(handler_modify_response[Constants.RESPONSE_BODY],
                         'Resource with identifier ebf20333-35a5-4a06-9c58-68ea688a9a8b has invalid attribute type for files',
                         'HTTP Status code not 400')
        self.remove_mock_database(dynamodb)

    def test_handler_modify_resource_invalid_resource_identifier_field_json_in_event_body(self):
        from src.classes.RequestHandler import RequestHandler
        dynamodb = self.setup_mock_database()
        request_handler = RequestHandler(dynamodb)
        event = {
            "body": "{\"operation\": \"MODIFY\",\"resource\": {\"identifer\": "
                    "\"ebf20333-35a5-4a06-9c58-68ea688a9a8b\", \"owner\": \"owner@unit.no\", \"files\": \"{}\", \"metadata\": {}}}"
        }
        handler_modify_response = request_handler.handler(event, None)
        self.assertEqual(handler_modify_response[Constants.RESPONSE_STATUS_CODE], http.HTTPStatus.BAD_REQUEST,
                         'HTTP Status code not 400')
        self.remove_mock_database(dynamodb)

    def test_handler_modify_resource_unexpected_resource_field_in_event_body(self):
        from src.classes.RequestHandler import RequestHandler
        dynamodb = self.setup_mock_database()
        request_handler = RequestHandler(dynamodb)
        event = {
            "body": "{\"operation\": \"MODIFY\",\"resource\": {\"resource_identifier\": "
                    "\"ebf20333-35a5-4a06-9c58-68ea688a9a8b\", \"registrator\": \"owner@unit.no\", \"owner\": \"owner@unit.no\", \"files\": \"{}\", \"metadata\": {}}}"
        }
        handler_modify_response = request_handler.handler(event, None)
        self.assertEqual(handler_modify_response[Constants.RESPONSE_STATUS_CODE], http.HTTPStatus.BAD_REQUEST,
                         'HTTP Status code not 400')
        self.remove_mock_database(dynamodb)

    def test_handler_modify_resource_created_date_missing_in_existing_resource(self):
        from src.classes.RequestHandler import RequestHandler
        dynamodb = self.setup_mock_database()
        request_handler = RequestHandler(dynamodb)
        resource = self.generate_mock_resource(None, None, self.EXISTING_RESOURCE_IDENTIFIER_MISSING_CREATED_DATE)
        event = self.generate_mock_event(Constants.OPERATION_MODIFY, resource)
        handler_modify_response = request_handler.handler(event, None)
        self.assertEqual(handler_modify_response[Constants.RESPONSE_STATUS_CODE], http.HTTPStatus.BAD_REQUEST,
                         'HTTP Status code not 400')
        self.assertEqual(handler_modify_response[Constants.RESPONSE_BODY],
                         'Resource with identifier acf20333-35a5-4a06-9c58-68ea688a9a9c has no ' + Constants.DDB_FIELD_CREATED_DATE + ' in DB',
                         'HTTP Status code not 400')
        self.remove_mock_database(dynamodb)

    def test_handler_modify_resource_unknown_resource_identifier_in_event_body(self):
        from src.classes.RequestHandler import RequestHandler
        dynamodb = self.setup_mock_database()
        request_handler = RequestHandler(dynamodb)
        resource = self.generate_mock_resource(None, None, 'xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx')
        event = self.generate_mock_event(Constants.OPERATION_MODIFY, resource)
        handler_modify_response = request_handler.handler(event, None)
        self.assertEqual(handler_modify_response[Constants.RESPONSE_STATUS_CODE], http.HTTPStatus.BAD_REQUEST,
                         'HTTP Status code not 400')
        self.assertEqual(handler_modify_response['body'],
                         'Resource with identifier xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx not found',
                         'Did not get expected error message')
        self.remove_mock_database(dynamodb)

    def test_handler_unknown_operation_in_event_body(self):
        from src.classes.RequestHandler import RequestHandler
        dynamodb = self.setup_mock_database()
        request_handler = RequestHandler(dynamodb)
        resource = self.generate_mock_resource(None, None, self.EXISTING_RESOURCE_IDENTIFIER)
        event = self.generate_mock_event('UNKNOWN_OPERATION', resource)
        handler_modify_response = request_handler.handler(event, None)
        self.assertEqual(handler_modify_response[Constants.RESPONSE_STATUS_CODE], http.HTTPStatus.BAD_REQUEST,
                         'HTTP Status code not 400')
        self.remove_mock_database(dynamodb)

    def test_handler_missing_resource_in_event_body(self):
        from src.classes.RequestHandler import RequestHandler
        dynamodb = self.setup_mock_database()
        request_handler = RequestHandler(dynamodb)
        event = self.generate_mock_event(Constants.OPERATION_INSERT, None)
        handler_modify_response = request_handler.handler(event, None)
        self.assertEqual(handler_modify_response[Constants.RESPONSE_STATUS_CODE], http.HTTPStatus.BAD_REQUEST,
                         'HTTP Status code not 400')
        self.remove_mock_database(dynamodb)

    def test_handler_missing_operation_in_event_body(self):
        from src.classes.RequestHandler import RequestHandler
        dynamodb = self.setup_mock_database()
        request_handler = RequestHandler(dynamodb)
        resource = self.generate_mock_resource(None, None, self.EXISTING_RESOURCE_IDENTIFIER)
        event = self.generate_mock_event(None, resource)
        handler_modify_response = request_handler.handler(event, None)
        self.assertEqual(handler_modify_response[Constants.RESPONSE_STATUS_CODE], http.HTTPStatus.BAD_REQUEST,
                         'HTTP Status code not 400')
        self.remove_mock_database(dynamodb)

    def test_handler_missing_event(self):
        from src.classes.RequestHandler import RequestHandler
        dynamodb = self.setup_mock_database()
        request_handler = RequestHandler(dynamodb)
        handler_modify_response = request_handler.handler(None, None)
        self.assertEqual(handler_modify_response[Constants.RESPONSE_STATUS_CODE], http.HTTPStatus.BAD_REQUEST,
                         'HTTP Status code not 400')
        self.remove_mock_database(dynamodb)

    def test_insert_resource(self):
        from src.classes.RequestHandler import RequestHandler
        dynamodb = self.setup_mock_database()
        request_handler = RequestHandler(dynamodb)

        resource = self.generate_mock_resource(None, None, None)
        event = self.generate_mock_event(Constants.OPERATION_INSERT, resource)
        handler_insert_response = request_handler.handler(event, None)

        self.assertEqual(handler_insert_response[Constants.RESPONSE_STATUS_CODE], http.HTTPStatus.CREATED,
                         'HTTP Status code not 201')

        # TODO Get resource identifier from response, then run tests below

        # query_results = request_handler.get_table_connection().query(
        #     KeyConditionExpression=Key(Constants.DDB_FIELD_RESOURCE_IDENTIFIER).eq(resource_identifier),
        #     ScanIndexForward=True
        # )
        #
        # inserted_resource = query_results[Constants.DDB_RESPONSE_ATTRIBUTE_NAME_ITEMS][0]
        # self.assertIsNotNone(inserted_resource[Constants.DDB_FIELD_CREATED_DATE], 'Value not persisted as expected')
        # self.assertIsNotNone(inserted_resource[Constants.DDB_FIELD_MODIFIED_DATE], 'Value not persisted as expected')
        # self.assertIsNotNone(inserted_resource[Constants.DDB_FIELD_METADATA], 'Value not persisted as expected')
        # self.assertEqual(inserted_resource[Constants.DDB_FIELD_MODIFIED_DATE],
        #                  inserted_resource[Constants.DDB_FIELD_CREATED_DATE],
        #                  'Value not persisted as expected')
        # self.assertEqual(inserted_resource[Constants.DDB_FIELD_METADATA], resource.metadata,
        #                  'Value not persisted as expected')
        self.remove_mock_database(dynamodb)

    # TODO: Use handler request
    def test_modify_resource(self):
        from src.classes.RequestHandler import RequestHandler
        dynamodb = self.setup_mock_database()
        request_handler = RequestHandler(dynamodb)

        time_created = arrow.utcnow().isoformat().replace('+00:00', 'Z')
        resource = self.generate_mock_resource(time_created, time_created)
        resource_identifier = resource.resource_identifier

        request_handler.insert_resource(resource_identifier, time_created, resource)
        for counter in range(2):
            generated_resource_modified = self.generate_mock_resource(time_created, None, resource_identifier)
            time_modified = arrow.utcnow().isoformat().replace('+00:00', 'Z')
            modify_response = request_handler.modify_resource(time_modified, generated_resource_modified)
            self.assertEqual(modify_response[Constants.DDB_RESPONSE_ATTRIBUTE_NAME_RESPONSE_METADATA][
                                 Constants.DDB_RESPONSE_ATTRIBUTE_NAME_RESPONSE_HTTP_STATUS_CODE], http.HTTPStatus.OK,
                             'HTTP Status code not 200')

        query_results = request_handler.get_table_connection().query(
            KeyConditionExpression=Key(Constants.DDB_FIELD_RESOURCE_IDENTIFIER).eq(resource_identifier),
            ScanIndexForward=True
        )

        self.assertEqual(len(query_results[Constants.DDB_RESPONSE_ATTRIBUTE_NAME_ITEMS]), 3,
                         'Value not persisted as expected')

        initial_resource = query_results[Constants.DDB_RESPONSE_ATTRIBUTE_NAME_ITEMS][0]
        first_modification_resource = query_results[Constants.DDB_RESPONSE_ATTRIBUTE_NAME_ITEMS][1]
        second_modification_resource = query_results[Constants.DDB_RESPONSE_ATTRIBUTE_NAME_ITEMS][2]

        self.assertEqual(initial_resource[Constants.DDB_FIELD_CREATED_DATE],
                         time_created,
                         'Value not persisted as expected')
        self.assertEqual(first_modification_resource[Constants.DDB_FIELD_CREATED_DATE],
                         time_created,
                         'Value not persisted as expected')
        self.assertEqual(second_modification_resource[Constants.DDB_FIELD_CREATED_DATE],
                         time_created,
                         'Value not persisted as expected')
        self.assertEqual(initial_resource[Constants.DDB_FIELD_MODIFIED_DATE],
                         time_created,
                         'Value not persisted as expected')
        self.assertNotEqual(first_modification_resource[Constants.DDB_FIELD_MODIFIED_DATE],
                            time_created,
                            'Value not persisted as expected')
        self.assertNotEqual(second_modification_resource[Constants.DDB_FIELD_MODIFIED_DATE],
                            time_created,
                            'Value not persisted as expected')
        self.assertNotEqual(first_modification_resource[Constants.DDB_FIELD_MODIFIED_DATE],
                            second_modification_resource[Constants.DDB_FIELD_MODIFIED_DATE],
                            'Value not persisted as expected')
        self.assertNotEqual(initial_resource[Constants.DDB_FIELD_METADATA],
                            first_modification_resource[Constants.DDB_FIELD_METADATA],
                            'Value not persisted as expected')
        self.assertNotEqual(initial_resource[Constants.DDB_FIELD_METADATA],
                            second_modification_resource[Constants.DDB_FIELD_METADATA],
                            'Value not persisted as expected')
        self.assertNotEqual(first_modification_resource[Constants.DDB_FIELD_METADATA],
                            second_modification_resource[Constants.DDB_FIELD_METADATA],
                            'Value not persisted as expected')
        self.remove_mock_database(dynamodb)

    def test_app(self):
        from src import app
        self.assertRaises(ValueError, app.handler, None, None)
        event = {
            "body": "{\"operation\": \"UNKNOWN_OPERATION\"} "
        }
        handler_response = app.handler(event, None)
        self.assertEqual(handler_response[Constants.RESPONSE_STATUS_CODE], http.HTTPStatus.BAD_REQUEST,
                         'HTTP Status code not 400')


if __name__ == '__main__':
    unittest.main()


class Body:

    def __init__(self, operation: str, resource: Resource):
        self.operation = operation
        self.resource = resource


def encode_body(instance):
    if isinstance(instance, Body):
        temp_value = {
            Constants.JSON_ATTRIBUTE_NAME_OPERATION: instance.operation,
            Constants.JSON_ATTRIBUTE_NAME_RESOURCE: encode_resource(instance.resource)
        }
        return remove_none_values(temp_value)
    else:
        type_name = instance.__class__.__name__
        raise TypeError(f"Object of type '{type_name}' is not JSON serializable")
