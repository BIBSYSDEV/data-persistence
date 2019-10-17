from __future__ import print_function

import os

import boto3

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(os.environ.get("TABLE_NAME"))


def lambda_handler(event, context):
    for record in event['Records']:
        print('EventID: ' + record['eventID'])
        print('EventName: ' + record['eventName'])
    print('Successfully processed %s records.' % str(len(event['Records'])))


def event_type_handler(event):
    event_name = event.get('eventName')
    if event_name == 'INSERT':
        None
    elif event_name == 'MODIFY':
        None
    elif event_name == 'REMOVE':
        None
    else:
        raise ValueError("The passed event was of an unknown type")
