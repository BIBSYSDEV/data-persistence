from typing import List

from data.creator import Creator


class Metadata:
    def __init__(self, creators: List[Creator] = None, handle: str = None, license_identifier: str = None,
                 publication_year: str = None, publisher: str = None, titles: dict = None, resource_type: str = None):
        self.resource_type = resource_type
        self.titles = titles
        self.publisher = publisher
        self.publication_year = publication_year
        self.license_identifier = license_identifier
        self.handle = handle
        self.creators = creators
