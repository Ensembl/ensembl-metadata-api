import sqlalchemy as db
from sqlalchemy.orm import Session
import pymysql

from ensembl.production.metadata.config import MetadataRegistryConfig

pymysql.install_as_MySQLdb()
config = MetadataRegistryConfig()


def load_database(uri=None):
    if uri is None:
        uri = config.METADATA_URI

    try:
        engine = db.create_engine(uri)
    except AttributeError:
        raise ValueError(f'Could not connect to database. Check METADATA_URI env variable.')

    try:
        connection = engine.connect()
    except db.exc.OperationalError as err:
        raise ValueError(f'Could not connect to database {uri}: {err}.') from err

    connection.close()
    return engine
