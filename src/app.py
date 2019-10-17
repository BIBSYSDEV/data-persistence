import json
import decimal
import os
import uuid

import arrow as arrow
import boto3
from boto3_type_annotations.dynamodb import Table

dynamodb = boto3.resource('dynamodb')
table: Table = dynamodb.Table(os.environ.get("TABLE_NAME"))


# Helper class to convert a DynamoDB item to JSON.
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if abs(o) % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)


def handler(event, context):
    operation = event.get('operation')
    resource = event.get('resource')

    print('Operation - ' + operation)
    current_time = arrow.utcnow().isoformat().replace("+00:00", "Z")

    if operation == 'RETRIEVE':
        response = table.get_item(
            Key={
                'resource_identifier': resource['resource_identifier']
            }
        )
        print(json.dumps(response, indent=4, cls=DecimalEncoder))
    elif operation == 'INSERT':
        generated_uuid = uuid.uuid4().__str__()
        print('Generated UUID: ' + generated_uuid)
        response = table.put_item(
            Item={
                'resource_identifier': generated_uuid,
                'modifiedDate': current_time,
                'createdDate': current_time,
                'metadata': resource['metadata']
            }
        )
        print(json.dumps(response, indent=4, cls=DecimalEncoder))
    elif operation == 'MODIFY':
        resource_identifier = resource['resource_identifier']
        response = table.update_item(
            Key={
                'resource_identifier': resource_identifier
            },
            UpdateExpression="set modifiedDate = :modifiedDate, metadata=:metadata",
            ExpressionAttributeValues={
                ':modifiedDate': current_time,
                ':metadata': resource['metadata']
            },
            ReturnValues="ALL_NEW"
        )
        print(json.dumps(response, indent=4, cls=DecimalEncoder))
    elif operation == 'REMOVE':
        response = table.delete_item(
            Key={
                'resource_identifier': resource['resource_identifier']
            }
        )
        print(json.dumps(response, indent=4, cls=DecimalEncoder))
    else:
        raise ValueError("Unknown operation")
