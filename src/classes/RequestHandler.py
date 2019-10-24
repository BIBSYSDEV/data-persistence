import decimal
import json

import boto3
from boto3.dynamodb.conditions import Key
from boto3_type_annotations.dynamodb import Table
import os
import uuid
import arrow as arrow


class RequestHandler:

    def __init__(self, dynamodb=None, table_name=None):
        if dynamodb is None:
            self.dynamodb = boto3.resource('dynamodb')
        else:
            self.dynamodb = dynamodb

        if table_name is None:
            self.table_name = os.environ.get("TABLE_NAME")
        else:
            self.table_name = table_name

        self.table: Table = self.dynamodb.Table(self.table_name)

    def get_table_connection(self):
        return self.table

    def insert_resource(self, generated_uuid, current_time, resource):
        ddb_response = self.table.put_item(
            Item={
                'resource_identifier': generated_uuid,
                'modifiedDate': current_time,
                'createdDate': current_time,
                'metadata': resource['metadata']
            }
        )
        return ddb_response

    def modify_resource(self, current_time, modified_resource):
        ddb_response = self.table.query(
            KeyConditionExpression=Key('resource_identifier')
                .eq(modified_resource['resource_identifier']))

        previous_resource = ddb_response['Items'][0]

        ddb_response = self.table.put_item(
            Item={
                'resource_identifier': modified_resource['resource_identifier'],
                'modifiedDate': current_time,
                'createdDate': previous_resource['createdDate'],
                'metadata': modified_resource['metadata']
            }
        )

        return ddb_response

    def handler(self, event, context):
        operation = event.get('operation')
        resource = event.get('resource')

        print('Operation - ' + operation)
        current_time = arrow.utcnow().isoformat().replace("+00:00", "Z")

        if operation == 'INSERT':
            generated_uuid = uuid.uuid4().__str__()
            ddb_response = self.insert_resource(generated_uuid, current_time, resource)
            print(json.dumps(ddb_response, indent=4, cls=DecimalEncoder))
            return ddb_response
        elif operation == 'MODIFY':
            ddb_response = self.modify_resource(resource, current_time)
            return ddb_response
        else:
            raise ValueError("Unknown operation")


# Helper class to convert a DynamoDB item to JSON.
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if abs(o) % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)
