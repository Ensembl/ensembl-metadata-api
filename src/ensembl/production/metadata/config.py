import os


class MetadataConfig:
    METADATA_URI = os.environ.get("METADATA_URI", None)
    TAXONOMY_URI = os.environ.get("TAXONOMY_URI", None)
