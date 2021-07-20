from concurrent import futures
import grpc
import logging
import sqlalchemy as db
from sqlalchemy.orm import Session
import pymysql

import ensembl.production.metadata.ensembl_metadata_pb2 as ensembl__metadata__pb2
import ensembl.production.metadata.ensembl_metadata_pb2_grpc as ensembl_metadata_pb2_grpc
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


def get_genome_by_uuid(metadata_db, genome_uuid):
    if genome_uuid is None:
        return create_genome()

    # This is sqlalchemy's metadata, not Ensembl's!
    md = db.MetaData()
    session = Session(metadata_db, future=True)

    # Reflect existing tables, letting sqlalchemy load linked tables where possible.
    genome = db.Table('genome', md, autoload_with=metadata_db)
    genome_release = db.Table('genome_release', md, autoload_with=metadata_db)
    assembly = md.tables['assembly']
    organism = md.tables['organism']
    release = md.tables['ensembl_release']
    site = md.tables['ensembl_site']

    genome_select = db.select(
            organism.c.ensembl_name,
            organism.c.url_name,
            organism.c.display_name,
            organism.c.taxonomy_id,
            organism.c.scientific_name,
            organism.c.strain,
            assembly.c.accession.label('assembly_accession'),
            assembly.c.name.label('assembly_name'),
            assembly.c.ucsc_name.label('assembly_ucsc_name'),
            assembly.c.level.label('assembly_level')
        ).select_from(genome).filter_by(
            genome_uuid=genome_uuid
        ).join(assembly).join(organism)

    genome_results = session.execute(genome_select).all()

    if len(genome_results) == 1:
        genome_data = dict(genome_results[0])

        release_select = db.select(
                release.c.is_current
            ).select_from(genome).filter_by(
                genome_uuid=genome_uuid
            ).join(genome_release).join(release).filter_by(
                is_current=True
            ).join(site)

        release_results = session.execute(release_select).first()
        genome_data['genome_uuid'] = genome_uuid
        genome_data['is_current'] = 0 if release_results is None else 1
        return create_genome(genome_data)
    else:
        return create_genome()


def get_genome_by_name(metadata_db, ensembl_name, site_name, release_version):
    if ensembl_name is None and site_name is None:
        return create_genome()

    # This is sqlalchemy's metadata, not Ensembl's!
    md = db.MetaData()
    session = Session(metadata_db, future=True)

    # Reflect existing tables, letting sqlalchemy load linked tables where possible.
    genome = db.Table('genome', md, autoload_with=metadata_db)
    genome_release = db.Table('genome_release', md, autoload_with=metadata_db)
    assembly = md.tables['assembly']
    organism = md.tables['organism']
    release = md.tables['ensembl_release']
    site = md.tables['ensembl_site']

    genome_select = db.select(
            genome.c.genome_uuid,
            organism.c.url_name,
            organism.c.display_name,
            organism.c.taxonomy_id,
            organism.c.scientific_name,
            organism.c.strain,
            assembly.c.accession.label('assembly_accession'),
            assembly.c.name.label('assembly_name'),
            assembly.c.ucsc_name.label('assembly_ucsc_name'),
            assembly.c.level.label('assembly_level'),
            release.c.is_current
        ).select_from(genome).join(assembly).join(organism).filter_by(
            ensembl_name=ensembl_name
        )

    if release_version == 0:
        genome_select = genome_select.join(genome_release).join(release).filter_by(
            is_current=True)
    else:
        genome_select = genome_select.join(genome_release).join(release).filter_by(
            version=release_version)

    genome_select = genome_select.join(site).filter_by(
        name=site_name)

    genome_results = session.execute(genome_select).all()

    if len(genome_results) == 1:
        genome_data = dict(genome_results[0])
        genome_data['ensembl_name'] = ensembl_name
        return create_genome(genome_data)
    else:
        return create_genome()


def genome_sequence_iterator(metadata_db, genome_uuid, chromosomal_only):
    if genome_uuid is None:
        return

    # This is sqlalchemy's metadata, not Ensembl's!
    md = db.MetaData()
    session = Session(metadata_db, future=True)

    # Reflect existing tables, letting sqlalchemy load linked tables where possible.
    genome = db.Table('genome', md, autoload_with=metadata_db)
    assembly = md.tables['assembly']
    assembly_sequence = db.Table('assembly_sequence', md, autoload_with=metadata_db)

    seq_select = db.select(
            assembly_sequence.c.accession,
            assembly_sequence.c.name,
            assembly_sequence.c.sequence_location,
            assembly_sequence.c.length,
            assembly_sequence.c.chromosomal
        ).select_from(genome).filter_by(
            genome_uuid=genome_uuid
        ).join(assembly).join(assembly_sequence)
    if chromosomal_only == 1:
        seq_select = seq_select.filter_by(chromosomal=True)

    for result in session.execute(seq_select):
        yield create_genome_sequence(dict(result))


def release_iterator(metadata_db, site_name, release_version, current_only):
    # This is sqlalchemy's metadata, not Ensembl's!
    md = db.MetaData()
    session = Session(metadata_db, future=True)

    # Reflect existing tables, letting sqlalchemy load linked tables where possible.
    release = db.Table('ensembl_release', md, autoload_with=metadata_db)
    site = md.tables['ensembl_site']

    release_select = db.select(
            release.c.version.label('release_version'),
            db.cast(release.c.release_date, db.String),
            release.c.label.label('release_label'),
            release.c.is_current,
            site.c.name.label('site_name'),
            site.c.label.label('site_label'),
            site.c.uri.label('site_uri')
        ).select_from(release)
    if len(release_version) > 0:
        release_select = release_select.filter(release.c.version.in_(release_version))
    if current_only == 1:
        release_select = release_select.filter_by(is_current=1)

    release_select = release_select.join(site)
    if len(site_name) > 0:
        release_select = release_select.filter(site.c.name.in_(site_name))

    release_results = session.execute(release_select).all()

    for result in release_results:
        yield create_release(dict(result))


def release_by_uuid_iterator(metadata_db, genome_uuid):
    if genome_uuid is None:
        return

    # This is sqlalchemy's metadata, not Ensembl's!
    md = db.MetaData()
    session = Session(metadata_db, future=True)

    # Reflect existing tables, letting sqlalchemy load linked tables where possible.
    genome = db.Table('genome', md, autoload_with=metadata_db)
    genome_release = db.Table('genome_release', md, autoload_with=metadata_db)
    release = md.tables['ensembl_release']
    site = md.tables['ensembl_site']

    release_select = db.select(
            release.c.version.label('release_version'),
            db.cast(release.c.release_date, db.String),
            release.c.label.label('release_label'),
            release.c.is_current,
            site.c.name.label('site_name'),
            site.c.label.label('site_label'),
            site.c.uri.label('site_uri')
        ).select_from(genome).filter_by(
            genome_uuid=genome_uuid
        ).join(genome_release).join(release).join(site)

    release_results = session.execute(release_select).all()

    for result in release_results:
        yield create_release(dict(result))


def create_genome(data=None):
    if data is None:
        return ensembl_metadata_pb2.Genome()

    assembly = ensembl_metadata_pb2.Assembly(
        accession=data['assembly_accession'],
        name=data['assembly_name'],
        ucsc_name=data['assembly_ucsc_name'],
        level=data['assembly_level'],
    )

    taxon = ensembl_metadata_pb2.Taxon(
        taxonomy_id=data['taxonomy_id'],
        scientific_name=data['scientific_name'],
        strain=data['strain'],
    )
    # TODO: fetch common_name(s) from ncbi_taxonomy database

    genome = ensembl_metadata_pb2.Genome(
        genome_uuid=data['genome_uuid'],
        ensembl_name=data['ensembl_name'],
        url_name=data['url_name'],
        display_name=data['display_name'],
        is_current=data['is_current'],
        assembly=assembly,
        taxon=taxon,
    )

    return genome


def create_genome_sequence(data=None):
    if data is None:
        return ensembl_metadata_pb2.GenomeSequence()

    genome_sequence = ensembl_metadata_pb2.GenomeSequence()

    # The following relies on keys matching exactly the message attributes.
    for k, v in data.items():
        if v is not None:
            setattr(genome_sequence, k, v)

    return genome_sequence


def create_release(data=None):
    if data is None:
        return ensembl_metadata_pb2.Release()

    release = ensembl_metadata_pb2.Release()

    # The following relies on keys matching exactly the message attributes.
    for k, v in data.items():
        if v is not None:
            setattr(release, k, v)

    return release


class EnsemblMetadataServicer(ensembl_metadata_pb2_grpc.EnsemblMetadataServicer):
    def __init__(self):
        self.db = load_database()

    def GetGenomeByUUID(self, request, context):
        return get_genome_by_uuid(self.db,
                                  request.genome_uuid
                                  )

    def GetGenomeByName(self, request, context):
        return get_genome_by_name(self.db,
                                  request.ensembl_name,
                                  request.site_name,
                                  request.release_version
                                  )

    def GetRelease(self, request, context):
        return release_iterator(self.db,
                                request.site_name,
                                request.release_version,
                                request.current_only
                                )

    def GetReleaseByUUID(self, request, context):
        return release_by_uuid_iterator(self.db,
                                        request.genome_uuid
                                        )

    def GetGenomeSequence(self, request, context):
        return genome_sequence_iterator(self.db,
                                        request.genome_uuid,
                                        request.chromosomal_only
                                        )


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    ensembl_metadata_pb2_grpc.add_EnsemblMetadataServicer_to_server(
        EnsemblMetadataServicer(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    server.wait_for_termination()


if __name__ == '__main__':
    logging.basicConfig()
    serve()
