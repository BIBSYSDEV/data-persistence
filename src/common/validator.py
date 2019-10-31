from common.constants import Constants


def validate_resource(operation, resource):
    if operation == Constants.OPERATION_MODIFY:
        if resource.resource_identifier is None:
            raise ValueError('Resource has no identifier')
        elif resource.metadata is None:
            raise ValueError('Resource with identifier ' + resource.resource_identifier + ' has no metadata')
        elif resource.files is None:
            raise ValueError('Resource with identifier ' + resource.resource_identifier + ' has no files')
        elif resource.owner is None:
            raise ValueError('Resource with identifier ' + resource.resource_identifier + ' has no owner')
        elif type(resource.metadata) is not dict:
            raise ValueError(
                'Resource with identifier ' + resource.resource_identifier + ' has invalid attribute type for metadata')
        elif type(resource.files) is not dict:
            raise ValueError(
                'Resource with identifier ' + resource.resource_identifier + ' has invalid attribute type for files')
    elif operation == Constants.OPERATION_INSERT:
        if resource.metadata is None:
            raise ValueError('Resource has no metadata')
        elif resource.files is None:
            raise ValueError('Resource has no files')
        elif resource.owner is None:
            raise ValueError('Resource has no owner')
        elif type(resource.metadata) is not dict:
            raise ValueError('Resource has invalid attribute type for metadata')
        elif type(resource.files) is not dict:
            raise ValueError('Resource has invalid attribute type for files')
