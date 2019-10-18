import pytest
from botocore.stub import Stubber, ANY

from src import app


@pytest.fixture(scope='function')
def ddb_stubber():
    ddb_stubber = Stubber(app.table.meta.client)
    ddb_stubber.activate()
    yield ddb_stubber
    ddb_stubber.deactivate()


def test_insert(ddb_stubber):
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

    put_item_params = {
        'TableName': ANY,
        'Item': {
            'resource_identifier': test_uuid,
            'modifiedDate': test_time,
            'createdDate': test_time,
            'metadata': test_resource['metadata']
        }
    }

    put_item_response = {
        {'Item':
             {'createdDate': {'S': test_time},
              'metadata':
                  {'M':
                       {'titles':
                            {'M':
                                 {'en': {'S': 'A title'},
                                  'no': {'S': 'En tittel'}
                                  }
                             }
                        }
                  },
              'modifiedDate': {'S': test_time},
              'resource_identifier': {'S': test_uuid}
              }
         }
    }

    ddb_stubber.add_response('put_item', put_item_response, put_item_params)

    result = app.insert_resource(test_uuid, test_time, test_resource)
    assert result is not None
    ddb_stubber.assert_no_pending_responses()
