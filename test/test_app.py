import boto3
from boto3.dynamodb.conditions import Key
from boto3_type_annotations.dynamodb import Table
from moto import mock_dynamodb2
import unittest
from src import app


class TestHandlerCase(unittest.TestCase):
    table_name = 'nva-test'
    dynamodb_client = boto3.client('dynamodb', region_name='eu-west-1', endpoint_url='http://localhost:8000')
    dynamodb_resource = boto3.resource('dynamodb', region_name='eu-west-1', endpoint_url='http://localhost:8000')

    table_connection = None

    def setUp(self):
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
    def test_insert_resource(self):
        test_uuid = '334aca5a-3fd4-4af3-9644-e3e80be58207'
        test_time = '2019-10-18T11:31:43.143158Z'
        test_resource = {
            'metadata': {
                'titles': {
                    'no': 'En tittel',
                    'en': 'A title'
                }
            }
        }

        result = app.insert_resource(test_uuid, test_time, test_resource)
        self.assertEqual(result['ResponseMetadata']['HTTPStatusCode'], 200, 'HTTP Status code not 200')

        results = self.table_connection.query(
            TableName=self.table_name,
            KeyConditionExpression=Key('resource_identifier').eq(
                test_uuid)
        )

        first_item = results['Items'][0]
        self.assertEqual(first_item['modifiedDate'], test_time, 'Value not persisted as expected')
        self.assertEqual(first_item['createdDate'], test_time, 'Value not persisted as expected')
        self.assertEqual(first_item['metadata'], test_resource['metadata'], 'Value not persisted as expected')


if __name__ == '__main__':
    unittest.main()
