import http
import json
import os
import uuid

import arrow as arrow
import boto3
from boto3.dynamodb.conditions import Key
from boto3_type_annotations.dynamodb import Table

from common.constants import Constants
from common.validator import validate_resource
from data.resource import Resource


class RequestHandler:

    def __init__(self, dynamodb=None):
        if dynamodb is None:
            self.dynamodb = boto3.resource('dynamodb', region_name=os.environ[Constants.ENV_VAR_REGION])
            # self.dynamodb = boto3.resource('dynamodb', region_name='eu-west-1')
        else:
            self.dynamodb = dynamodb

        self.table_name = os.environ.get(Constants.ENV_VAR_TABLE_NAME)
        self.table: Table = self.dynamodb.Table(self.table_name)

    def get_table_connection(self):
        return self.table

    def insert_resource(self, generated_uuid, current_time, resource):
        ddb_response = self.table.put_item(
            Item={
                Constants.DDB_FIELD_RESOURCE_IDENTIFIER: generated_uuid,
                Constants.DDB_FIELD_MODIFIED_DATE: current_time,
                Constants.DDB_FIELD_CREATED_DATE: current_time,
                Constants.DDB_FIELD_METADATA: resource.metadata,
                Constants.DDB_FIELD_FILES: resource.files,
                Constants.DDB_FIELD_OWNER: resource.owner
            }
        )
        return ddb_response

    def modify_resource(self, current_time, modified_resource):
        ddb_response = self.table.query(
            KeyConditionExpression=Key(Constants.DDB_FIELD_RESOURCE_IDENTIFIER).eq(
                modified_resource.resource_identifier))

        if len(ddb_response[Constants.DDB_RESPONSE_ATTRIBUTE_NAME_ITEMS]) == 0:
            raise ValueError('Resource with identifier ' + modified_resource.resource_identifier + ' not found')
        else:
            previous_resource = ddb_response[Constants.DDB_RESPONSE_ATTRIBUTE_NAME_ITEMS][0]
            if Constants.DDB_FIELD_CREATED_DATE not in previous_resource:
                raise ValueError(
                    'Resource with identifier ' + modified_resource.resource_identifier + ' has no ' + Constants.DDB_FIELD_CREATED_DATE + ' in DB')
            else:
                ddb_response = self.table.put_item(
                    Item={
                        Constants.DDB_FIELD_RESOURCE_IDENTIFIER: modified_resource.resource_identifier,
                        Constants.DDB_FIELD_MODIFIED_DATE: current_time,
                        Constants.DDB_FIELD_CREATED_DATE: previous_resource[Constants.DDB_FIELD_CREATED_DATE],
                        Constants.DDB_FIELD_METADATA: modified_resource.metadata,
                        Constants.DDB_FIELD_FILES: modified_resource.files,
                        Constants.DDB_FIELD_OWNER: modified_resource.owner
                    }
                )
                return ddb_response

    def response(self, status_code, body):
        return {
            Constants.RESPONSE_STATUS_CODE: status_code,
            Constants.RESPONSE_BODY: body
        }

    def handler(self, event, context):
        if event is None or Constants.EVENT_BODY not in event:
            return self.response(http.HTTPStatus.BAD_REQUEST, 'Insufficient parameters')
        else:
            body = json.loads(event[Constants.EVENT_BODY])
            operation = body.get(Constants.JSON_ATTRIBUTE_NAME_OPERATION)
            resource_dict_from_json = body.get(Constants.JSON_ATTRIBUTE_NAME_RESOURCE)

            try:
                resource = Resource.from_dict(resource_dict_from_json)
            except TypeError as e:
                return self.response(http.HTTPStatus.BAD_REQUEST, e.args[0])

            current_time = arrow.utcnow().isoformat().replace('+00:00', 'Z')

            if operation == Constants.OPERATION_INSERT and resource is not None:
                try:
                    validate_resource(operation, resource)
                except ValueError as e:
                    return self.response(http.HTTPStatus.BAD_REQUEST, e.args[0])
                generated_uuid = uuid.uuid4().__str__()
                ddb_response = self.insert_resource(generated_uuid, current_time, resource)
                # TODO: Add resource identifier to response
                return self.response(http.HTTPStatus.CREATED, json.dumps(ddb_response))
            elif operation == Constants.OPERATION_MODIFY and resource is not None:
                try:
                    validate_resource(operation, resource)
                    ddb_response = self.modify_resource(current_time, resource)
                    return self.response(http.HTTPStatus.OK, json.dumps(ddb_response))
                except ValueError as e:
                    return self.response(http.HTTPStatus.BAD_REQUEST, e.args[0])
            else:
                return self.response(http.HTTPStatus.BAD_REQUEST, 'Insufficient parameters')
