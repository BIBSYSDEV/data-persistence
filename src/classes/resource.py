from typing import List


class FileMetadata:
    def __init__(self, filename: str, mime_type: str, checksum: str, size: str):
        self.filename = filename
        self.mime_type = mime_type
        self.checksum = checksum
        self.size = size


class File:
    def __init__(self, identifier: str, file_metadata: FileMetadata):
        self.identifier = identifier
        self.file_metadata = file_metadata


class Creator:
    def __init__(self, identifier: str):
        self.identifier = identifier


class Title:
    def __init__(self, language_code: str, title: str):
        self.language_code = language_code
        self.title = title


class Metadata:
    def __init__(self, creators: List[Creator], handle: str, license_identifier: str, publication_year: str,
                 publisher: str, titles: dict, resource_type: str = None):
        self.resource_type = resource_type
        self.titles = titles
        self.publisher = publisher
        self.publication_year = publication_year
        self.license_identifier = license_identifier
        self.handle = handle
        self.creators = creators


class Resource:
    def __init__(self, resource_identifier: str, modified_date: str, created_date: str, metadata: Metadata,
                 files: dict, owner: str):
        self.resource_identifier = resource_identifier
        self.modified_date = modified_date
        self.created_date = created_date
        self.metadata = metadata
        self.files = files
        self.owner = owner


def encode_file_metadata(instance):
    if isinstance(instance, FileMetadata):

        temp_value = {
            'filename': instance.filename,
            'mimetype': instance.mime_type,
            'checksum': instance.checksum,
            'size': instance.size
        }
        return remove_none_values(temp_value)

        # return instance.filename, instance.mime_type, instance.checksum, instance.size
    else:
        type_name = instance.__class__.__name__
        raise TypeError(f"Object of type '{type_name}' is not JSON serializable")


def encode_files(instance):
    if isinstance(instance, dict):

        files = dict()
        for key, value in instance.items():
            files[key] = encode_file_metadata(value)

        return files
    else:
        type_name = instance.__class__.__name__
        raise TypeError(f"Object of type '{type_name}' is not JSON serializable")


def encode_creator(instance):
    if isinstance(instance, Creator):
        return instance.identifier
    else:
        type_name = instance.__class__.__name__
        raise TypeError(f"Object of type '{type_name}' is not JSON serializable")


def encode_title(instance):
    if isinstance(instance, Title):
        return instance.language_code, instance.title
    else:
        type_name = instance.__class__.__name__
        raise TypeError(f"Object of type '{type_name}' is not JSON serializable")


def encode_metadata(instance):
    if isinstance(instance, Metadata):

        creators = []
        for creator in instance.creators:
            creators.append(creator.identifier)

        titles = dict()
        for key, value in instance.titles.items():
            if value is not None:
                titles[key] = value
        if titles.keys() is 0:
            titles = None

        temp_value = {
            'creators': creators,
            'handle': instance.handle,
            'license': instance.license_identifier,
            'publicationYear': instance.publication_year,
            'publisher': instance.publisher,
            'titles': titles,
            'type': instance.resource_type
        }
        return remove_none_values(temp_value)
    else:
        type_name = instance.__class__.__name__
        raise TypeError(f"Object of type '{type_name}' is not JSON serializable")


def remove_none_values(temp_value):
    return_value = dict()
    for key, value in temp_value.items():
        if value is not None:
            return_value[key] = value
    return return_value


def encode_resource(instance):
    if isinstance(instance, Resource):

        temp_value = {
            'resource_identifier': instance.resource_identifier,
            'modifiedDate': instance.modified_date,
            'createdDate': instance.created_date,
            'metadata': encode_metadata(instance.metadata),
            'files': encode_files(instance.files),
            'owner': instance.owner
        }
        return remove_none_values(temp_value)
    else:
        type_name = instance.__class__.__name__
        raise TypeError(f"Object of type '{type_name}' is not JSON serializable")
