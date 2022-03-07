import os


class MetadataConfig:
    metadata_uri = os.environ.get("METADATA_URI")
    taxonomy_uri = os.environ.get("TAXONOMY_URI")
