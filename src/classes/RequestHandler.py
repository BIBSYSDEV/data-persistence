import json

import boto3
from boto3.dynamodb.conditions import Key
from boto3_type_annotations.dynamodb import Table
import os
import uuid
import arrow as arrow


class RequestHandler:

    def __init__(self, dynamodb=None):
        if dynamodb is None:
            self.dynamodb = boto3.resource('dynamodb')
        else:
            self.dynamodb = dynamodb

        self.table_name = os.environ.get("TABLE_NAME")
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
            KeyConditionExpression=Key('resource_identifier').eq(modified_resource['resource_identifier']))

        if len(ddb_response['Items']) == 0:
            raise ValueError("resource with identifier " + modified_resource['resource_identifier'] + " not found")
        elif 'metadata' not in modified_resource:
            raise ValueError(
                "resource with identifier " + modified_resource['resource_identifier'] + " has no metadata")
        elif type(modified_resource['metadata']) is not dict:
            raise ValueError("resource with identifier " + modified_resource[
                'resource_identifier'] + " has invalid attribute type for metadata")
        else:
            previous_resource = ddb_response['Items'][0]
            if 'createdDate' not in previous_resource:
                raise ValueError("resource with identifier " + modified_resource[
                    'resource_identifier'] + " has no createdDate in DB")
            else:
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
        operation = json.loads(event['body']).get('operation')
        resource = json.loads(event['body']).get('resource')

        current_time = arrow.utcnow().isoformat().replace("+00:00", "Z")

        if operation == 'INSERT' and resource is not None:
            generated_uuid = uuid.uuid4().__str__()
            ddb_response = self.insert_resource(generated_uuid, current_time, resource)
            return {
                'statusCode': 201,
                'body': json.dumps(ddb_response),
                'headers': {'Content-Type': 'application/json'}
            }
        elif operation == 'MODIFY' and resource is not None:
            try:
                ddb_response = self.modify_resource(current_time, resource)
                return {
                    'statusCode': 200,
                    'body': json.dumps(ddb_response),
                    'headers': {'Content-Type': 'application/json'}
                }
            except ValueError as e:
                return {
                    'statusCode': 400,
                    'body': e.args[0]
                }
        else:
            return {
                'statusCode': 400,
                'body': 'insufficient parameters'
            }
