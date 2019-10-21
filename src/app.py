import json
import decimal
import uuid

import arrow as arrow
import boto3
from boto3_type_annotations.dynamodb import Table
import os


class DynamoDBConnection:

    def __init__(self, table_name=None):
        self.table_name = table_name

    def get_table_connection(self):
        dynamodb = boto3.resource('dynamodb')
        return dynamodb.Table(os.environ.get("TABLE_NAME"))

    def get_local_table_connection(self):
        dynamodb = boto3.resource('dynamodb', endpoint_url='http://localhost:8000')
        return dynamodb.Table(self.table_name)


if os.environ.get("TABLE_NAME") is not None:
    table = DynamoDBConnection(os.environ.get("TABLE_NAME")).get_table_connection()
else:
    # Running as test
    table = DynamoDBConnection("nva-test").get_local_table_connection()


# Helper class to convert a DynamoDB item to JSON.
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if abs(o) % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)


def insert_resource(generated_uuid, current_time, resource):
    ddb_response = table.put_item(
        Item={
            'resource_identifier': generated_uuid,
            'modifiedDate': current_time,
            'createdDate': current_time,
            'metadata': resource['metadata']
        }
    )
    return ddb_response


def modify_resource(resource, current_time):
    ddb_response = table.update_item(
        Key={
            'resource_identifier': resource['resource_identifier']
        },
        UpdateExpression="set modifiedDate = :modifiedDate, metadata=:metadata",
        ExpressionAttributeValues={
            ':modifiedDate': current_time,
            ':metadata': resource['metadata']
        },
        ReturnValues="ALL_NEW"
    )
    print(json.dumps(ddb_response, indent=4, cls=DecimalEncoder))
    return ddb_response.get('Item')


def remove_resource(resource):
    ddb_response = table.delete_item(
        Key={
            'resource_identifier': resource['resource_identifier']
        }
    )
    print(json.dumps(ddb_response, indent=4, cls=DecimalEncoder))
    return ddb_response.get('HTTPStatusCode')


def handler(event, context):
    operation = event.get('operation')
    resource = event.get('resource')

    print('Operation - ' + operation)
    current_time = arrow.utcnow().isoformat().replace("+00:00", "Z")

    if operation == 'INSERT':
        generated_uuid = uuid.uuid4().__str__()
        item = insert_resource(generated_uuid, current_time, resource)
        return item
    elif operation == 'MODIFY':
        item = modify_resource(resource, current_time)
        return item
    elif operation == 'REMOVE':
        item = remove_resource(resource)
        return item
    else:
        raise ValueError("Unknown operation")
