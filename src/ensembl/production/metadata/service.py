#  See the NOTICE file distributed with this work for additional information
#  regarding copyright ownership.
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#      http://www.apache.org/licenses/LICENSE-2.0
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
import itertools
from concurrent import futures
import grpc
import logging
import sqlalchemy as db
from sqlalchemy import or_, func
from sqlalchemy.orm import Session

from ensembl.production.metadata import ensembl_metadata_pb2_grpc
from ensembl.production.metadata import ensembl_metadata_pb2

from ensembl.production.metadata.config import MetadataConfig as cfg

from ensembl.production.metadata.api.genome import GenomeAdaptor
from ensembl.production.metadata.api.release import ReleaseAdaptor


# This function will be replaced with connect_to_db() below
def load_database(uri=None):
    if uri is None:
        uri = cfg.metadata_uri
        taxonomy_uri = cfg.taxon_uri

    try:
        engine = db.create_engine(
            uri, pool_size=cfg.pool_size, max_overflow=cfg.max_overflow
        )
        taxonomy_engine = db.create_engine(
            taxonomy_uri, pool_size=cfg.pool_size, max_overflow=cfg.max_overflow
        )
    except AttributeError:
        raise ValueError(
            f"Could not connect to database. Check metadata_uri env variable."
        )

    try:
        connection = engine.connect()
        taxonomy_connection = taxonomy_engine.connect()
    except db.exc.OperationalError as err:
        raise ValueError(f"Could not connect to database {uri}: {err}.") from err

    connection.close()
    taxonomy_connection.close()
    return engine, taxonomy_engine


def connect_to_db():
    conn = GenomeAdaptor(
        metadata_uri=cfg.metadata_uri,
        taxonomy_uri=cfg.taxon_uri
    )
    return conn  # conn.metadata_db, conn.taxonomy_db


def get_karyotype_information(metadata_db, genome_uuid):
    if genome_uuid is None:
        return create_genome()

    conn = connect_to_db()
    karyotype_info_result = conn.fetch_sequences(
        genome_uuid=genome_uuid
    )

    if len(karyotype_info_result) == 1:
        return create_karyotype(karyotype_info_result[0])

    return create_karyotype()


def get_top_level_statistics(metadata_db, organism_uuid):
    if organism_uuid is None:
        return create_top_level_statistics()

    conn = connect_to_db()
    stats_results = conn.fetch_genome_datasets(
        organism_uuid=organism_uuid,
        dataset_name="all"
    )

    statistics = []
    if len(stats_results) > 0:
        for result in stats_results:
            statistics.append({
                    'name': result.Attribute.name,
                    'label': result.Attribute.label,
                    'statistic_type': result.Attribute.type,
                    'statistic_value': result.DatasetAttribute.value
                })
        return create_top_level_statistics({
            'organism_uuid': organism_uuid,
            'statistics': statistics
        })

    return create_top_level_statistics()


def get_top_level_statistics_by_uuid(metadata_db, genome_uuid):
    if genome_uuid is None:
        return create_top_level_statistics_by_uuid()

    conn = connect_to_db()
    stats_results = conn.fetch_genome_datasets(
        genome_uuid=genome_uuid,
        dataset_name="all"
    )

    statistics = []
    if len(stats_results) > 0:
        for result in stats_results:
            statistics.append({
                'name': result.Attribute.name,
                'label': result.Attribute.label,
                'statistic_type': result.Attribute.type,
                'statistic_value': result.DatasetAttribute.value
            })
        return create_top_level_statistics_by_uuid(
            ({"genome_uuid": genome_uuid, "statistics": statistics})
        )

    return create_top_level_statistics_by_uuid()


def get_assembly_information(metadata_db, assembly_uuid):
    if assembly_uuid is None:
        return create_assembly()

    conn = connect_to_db()
    assembly_results = conn.fetch_sequences(
        assembly_uuid=assembly_uuid
    )
    if len(assembly_results) > 0:
        return create_assembly(assembly_results[0])

    return create_assembly()


def get_genomes_from_assembly_accession_iterator(metadata_db, assembly_accession):
    if assembly_accession is None:
        return create_genome()

    conn = connect_to_db()
    genome_results = conn.fetch_genomes(
        assembly_accession=assembly_accession
    )
    for genome in genome_results:
        yield create_genome(genome)


def get_species_information(metadata_db, taxonomy_db, genome_uuid):
    if genome_uuid is None:
        return create_species()

    conn = connect_to_db()
    species_results = conn.fetch_genomes(
        genome_uuid=genome_uuid
    )
    if len(species_results) == 1:
        tax_id = species_results[0].Organism.taxonomy_id
        taxo_results = conn.fetch_taxonomy_names(tax_id)
        return create_species(species_results[0], taxo_results[tax_id])

    return create_species()


def get_sub_species_info(metadata_db, organism_uuid):
    if organism_uuid is None:
        return create_sub_species()

    conn = connect_to_db()
    sub_species_results = conn.fetch_genomes(
        organism_uuid=organism_uuid
    )

    species_name = []
    species_type = []
    if len(sub_species_results) > 0:
        for result in sub_species_results:
            if result.OrganismGroup.type not in species_type:
                species_type.append(result.OrganismGroup.type)
            if result.OrganismGroup.name not in species_name:
                species_name.append(result.OrganismGroup.name)

        return create_sub_species({
            'organism_uuid': organism_uuid,
            'species_type': species_type,
            'species_name': species_name
        })

    return create_sub_species()


def get_grouping_info(metadata_db, organism_id):
    if organism_id is None:
        return create_grouping()

    md = db.MetaData()
    with Session(metadata_db, future=True) as session:
        # Reflect existing tables, letting sqlalchemy load linked tables where possible.
        organism_group_member = db.Table('organism_group_member', md, autoload_with=metadata_db)
        organism_group = db.Table('organism_group', md, autoload_with=metadata_db)

        grouping_select = db.select(
            organism_group.c.type,
            organism_group.c.name,
        ).select_from(organism_group_member).filter_by(
            organism_id=organism_id
        ).join(organism_group)

        grouping_results = session.execute(grouping_select).all()

        species_name = []
        species_type = []
        if len(grouping_results) > 0:
            for key, value in grouping_results:
                species_type.append(key)
                species_name.append(value)

            return create_grouping({
                'organism_id': organism_id,
                'species_type': species_type,
                'species_name': species_name
            })
        else:
            return create_grouping()


def get_genome_uuid(metadata_db, ensembl_name, assembly_name):
    if ensembl_name is None or assembly_name is None:
        return create_genome_uuid()

    conn = connect_to_db()
    genome_uuid_result = conn.fetch_genomes(
        ensembl_name=ensembl_name,
        assembly_name=assembly_name
    )

    if len(genome_uuid_result) == 1:
        return create_genome_uuid(
            {"genome_uuid": genome_uuid_result[0].Genome.genome_uuid}
        )

    return create_genome_uuid()


def get_genome_by_uuid(metadata_db, genome_uuid, release_version):
    if genome_uuid is None:
        return create_genome()

    conn = connect_to_db()
    genome_results = conn.fetch_genomes(
        genome_uuid=genome_uuid,
        release_version=release_version
    )

    if len(genome_results) == 1:
        return create_genome(genome_results[0])
    return create_genome()


def get_genomes_by_keyword_iterator(metadata_db, keyword, release_version):
    # TODO: implement an API for this function in the metadata API
    if not keyword:
        return
    sqlalchemy_md = db.MetaData()
    with Session(metadata_db, future=True) as session:

        # Reflect existing tables, letting sqlalchemy load linked tables where possible.
        genome = db.Table('genome', sqlalchemy_md, autoload_with=metadata_db)
        genome_release = db.Table('genome_release', sqlalchemy_md, autoload_with=metadata_db)
        release = sqlalchemy_md.tables['ensembl_release']
        assembly = sqlalchemy_md.tables['assembly']
        organism = sqlalchemy_md.tables['organism']

        genome_query = get_genome_query(genome, genome_release, release, assembly, organism).select_from(genome) \
            .outerjoin(assembly) \
            .outerjoin(organism) \
            .outerjoin(genome_release) \
            .outerjoin(release) \
            .where(or_(func.lower(assembly.c.tol_id) == keyword.lower(),
                       func.lower(assembly.c.accession) == keyword.lower(),
                       func.lower(assembly.c.name) == keyword.lower(),
                       func.lower(assembly.c.ensembl_name) == keyword.lower(),
                       func.lower(organism.c.display_name) == keyword.lower(),
                       func.lower(organism.c.scientific_name) == keyword.lower(),
                       func.lower(organism.c.scientific_parlance_name) == keyword.lower(),
                       func.lower(organism.c.species_taxonomy_id) == keyword.lower()))
        if release_version == 0:
            genome_query = genome_query.where(release.c.is_current == 1)
        else:
            genome_query = genome_query.where(release.c.version <= release_version)
        genome_results = session.execute(genome_query).all()
        # print(str(genome_query))
        most_recent_genomes = []
        for _, genome_release_group in itertools.groupby(genome_results, lambda r: r["assembly_accession"]):
            most_recent_genome = sorted(genome_release_group, key=lambda g: g["release_version"], reverse=True)[0]
            most_recent_genomes.append(most_recent_genome)

        for genome_row in most_recent_genomes:
            yield create_genome(genome_row)


def get_genome_by_name(metadata_db, ensembl_name, site_name, release_version):
    if ensembl_name is None and site_name is None:
        return create_genome()

    conn = connect_to_db()
    genome_results = conn.fetch_genomes(
        ensembl_name=ensembl_name,
        site_name=site_name,
        release_version=release_version
    )
    if len(genome_results) == 1:
        return create_genome(genome_results[0])
    return create_genome()


def populate_dataset_info(data):
    return ensembl_metadata_pb2.DatasetInfos.DatasetInfo(
        dataset_uuid=data.Dataset.dataset_uuid,
        dataset_name=data.Dataset.name,
        dataset_version=data.Dataset.version,
        dataset_label=data.Dataset.label,
        version=int(data.EnsemblRelease.version),
    )


def get_datasets_list_by_uuid(metadata_db, genome_uuid, release_version=0):
    if genome_uuid is None:
        return create_datasets()

    conn = connect_to_db()
    datasets_results = conn.fetch_genome_datasets(
        genome_uuid=genome_uuid,
        # fetch all datasets, default is 'assembly' only
        dataset_name="all",
        release_version=release_version
    )

    if len(datasets_results) > 0:
        # ds_obj_dict where all datasets are stored as:
        # { dataset_type_1: [datasets_dt1_1, datasets_dt1_2], dataset_type_2: [datasets_dt2_1] }
        ds_obj_dict = {}
        for result in datasets_results:
            dataset_type = result.Dataset.name
            # Populate the objects bottom up
            datasets_info = populate_dataset_info(result)
            # Construct the datasets dictionary
            if dataset_type in ds_obj_dict:
                ds_obj_dict[dataset_type].append(datasets_info)
            else:
                ds_obj_dict[dataset_type] = [datasets_info]

        dataset_object_dict = {}
        # map each datasets list (e.g: [datasets_dt1_1, datasets_dt1_2]) to DatasetInfos
        for dataset_type_key in ds_obj_dict:
            dataset_object_dict[dataset_type_key] = ensembl_metadata_pb2.DatasetInfos(
                dataset_infos=ds_obj_dict[dataset_type_key]
            )

        return create_datasets({
            'genome_uuid': genome_uuid,
            'datasets': dataset_object_dict
        })

    return create_datasets()


def get_genome_query(genome, genome_release, release, assembly, organism):
    return db.select(
        genome.c.genome_uuid,
        genome.c.created,
        organism.c.ensembl_name,
        organism.c.url_name,
        organism.c.display_name,
        organism.c.taxonomy_id,
        organism.c.scientific_name,
        organism.c.strain,
        organism.c.scientific_parlance_name,
        organism.c.organism_id,
        assembly.c.accession.label("assembly_accession"),
        assembly.c.name.label("assembly_name"),
        assembly.c.ucsc_name.label("assembly_ucsc_name"),
        assembly.c.level.label("assembly_level"),
        assembly.c.ensembl_name.label("assembly_ensembl_name"),
        release.c.version.label("release_version"),
        release.c.release_date,
        release.c.label.label("release_label"),
        release.c.is_current,
    ).where(genome_release.c.is_current == 1)


def get_genome_uuid_query(genome, assembly, organism):
    return db.select(
        genome.c.genome_uuid,
        organism.c.ensembl_name,
        assembly.c.name.label("assembly_name"),
    )


def genome_sequence_iterator(metadata_db, genome_uuid, chromosomal_only):
    if genome_uuid is None:
        return

    conn = connect_to_db()
    assembly_sequence_results = conn.fetch_sequences(
        genome_uuid=genome_uuid,
        chromosomal_only=chromosomal_only,
    )
    for result in assembly_sequence_results:
        yield create_genome_sequence(result)


def release_iterator(metadata_db, site_name, release_version, current_only):
    conn = ReleaseAdaptor(metadata_uri=cfg.metadata_uri)

    # set release_version/site_name to None if it's an empty list
    release_version = release_version or None
    site_name = site_name or None

    release_results = conn.fetch_releases(
        release_version=release_version,
        current_only=current_only,
        site_name=site_name
    )

    for result in release_results:
        yield create_release(result)


def release_by_uuid_iterator(metadata_db, genome_uuid):
    if genome_uuid is None:
        return

    conn = ReleaseAdaptor(metadata_uri=cfg.metadata_uri)
    release_results = conn.fetch_releases_for_genome(
        genome_uuid=genome_uuid,
    )

    for result in release_results:
        yield create_release(result)


def get_dataset_by_genome_id(metadata_db, genome_uuid, requested_dataset_type):
    if genome_uuid is None:
        return create_dataset_infos()

    conn = connect_to_db()
    dataset_results = conn.fetch_genome_datasets(
        genome_uuid=genome_uuid,
        dataset_type=requested_dataset_type
    )
    return create_dataset_infos(genome_uuid, requested_dataset_type, dataset_results)


def create_species(species_data=None, taxo_info=None):
    if species_data is None:
        return ensembl_metadata_pb2.Species()

    species = ensembl_metadata_pb2.Species(
        genome_uuid=species_data.Genome.genome_uuid,
        taxon_id=species_data.Organism.taxonomy_id,
        scientific_name=species_data.Organism.scientific_name,
        scientific_parlance_name=species_data.Organism.scientific_parlance_name,
        ncbi_common_name=taxo_info["ncbi_common_name"],
        common_name=taxo_info["common_name"],
        alternative_names=taxo_info["alternative_names"],
    )
    return species


def create_top_level_statistics(data=None):
    if data is None:
        return ensembl_metadata_pb2.TopLevelStatistics()
    species = ensembl_metadata_pb2.TopLevelStatistics(
        organism_uuid=data["organism_uuid"],
        statistics=data["statistics"],
    )
    return species


def create_top_level_statistics_by_uuid(data=None):
    if data is None:
        return ensembl_metadata_pb2.TopLevelStatisticsByUUID()
    species = ensembl_metadata_pb2.TopLevelStatisticsByUUID(
        genome_uuid=data["genome_uuid"],
        statistics=data["statistics"],
    )
    return species


def create_karyotype(data=None):
    if data is None:
        return ensembl_metadata_pb2.Karyotype()

    karyotype = ensembl_metadata_pb2.Karyotype(
        genome_uuid=data.Genome.genome_uuid,
        code=data.Assembly.level,
        chromosomal=str(data.AssemblySequence.chromosomal),
        location=data.AssemblySequence.sequence_location,
    )
    return karyotype


def create_grouping(data=None):
    if data is None:
        return ensembl_metadata_pb2.Grouping()
    grouping = ensembl_metadata_pb2.Grouping(
        organism_id=data["organism_id"],
        species_name=data["species_name"],
        species_type=data["species_type"],
    )
    return grouping


def create_sub_species(data=None):
    if data is None:
        return ensembl_metadata_pb2.SubSpecies()
    sub_species = ensembl_metadata_pb2.SubSpecies(
        # Todo: change id to uuid
        organism_id=data["organism_uuid"],
        species_name=data["species_name"],
        species_type=data["species_type"],
    )
    return sub_species


def create_assembly(data=None):
    if data is None:
        return ensembl_metadata_pb2.AssemblyInfo()

    assembly = ensembl_metadata_pb2.AssemblyInfo(
        assembly_uuid=data.Assembly.assembly_uuid,
        accession=data.Assembly.accession,
        level=data.Assembly.level,
        name=data.Assembly.name,
        chromosomal=data.AssemblySequence.chromosomal,
        length=data.AssemblySequence.length,
        sequence_location=data.AssemblySequence.sequence_location,
        sequence_checksum=data.AssemblySequence.sequence_checksum,
        ga4gh_identifier=data.AssemblySequence.ga4gh_identifier,
    )
    return assembly


def create_genome_uuid(data=None):
    if data is None:
        return ensembl_metadata_pb2.GenomeUUID()

    genome_uuid = ensembl_metadata_pb2.GenomeUUID(
        genome_uuid=data["genome_uuid"]
    )
    return genome_uuid


def create_genome(data=None):
    if data is None:
        return ensembl_metadata_pb2.Genome()

    try:
        # try to construct the object using Metadata API models
        assembly = ensembl_metadata_pb2.Assembly(
            accession=data.Assembly.accession,
            name=data.Assembly.name,
            ucsc_name=data.Assembly.ucsc_name,
            level=data.Assembly.level,
            ensembl_name=data.Assembly.ensembl_name,
        )

        taxon = ensembl_metadata_pb2.Taxon(
            taxonomy_id=data.Organism.taxonomy_id,
            scientific_name=data.Organism.scientific_name,
            strain=data.Organism.strain,
        )
        # TODO: fetch common_name(s) from ncbi_taxonomy database

        organism = ensembl_metadata_pb2.Organism(
            display_name=data.Organism.display_name,
            strain=data.Organism.strain,
            scientific_name=data.Organism.scientific_name,
            url_name=data.Organism.url_name,
            ensembl_name=data.Organism.ensembl_name,
            scientific_parlance_name=data.Organism.scientific_parlance_name,
        )

        release = ensembl_metadata_pb2.Release(
            release_version=data.EnsemblRelease.version,
            release_date=str(data.EnsemblRelease.release_date),
            release_label=data.EnsemblRelease.label,
            is_current=data.EnsemblRelease.is_current,
        )

        genome = ensembl_metadata_pb2.Genome(
            genome_uuid=data.Genome.genome_uuid,
            created=str(data.Genome.created),
            assembly=assembly,
            taxon=taxon,
            organism=organism,
            release=release,
        )
    except AttributeError:
        # Otherwise (e.g: when calling get_genomes_by_keyword_iterator())
        # use the old approach
        # TODO: get rid of this section
        assembly = ensembl_metadata_pb2.Assembly(
            accession=data["assembly_accession"],
            name=data["assembly_name"],
            ucsc_name=data["assembly_ucsc_name"],
            level=data["assembly_level"],
            ensembl_name=data["assembly_ensembl_name"],
        )

        taxon = ensembl_metadata_pb2.Taxon(
            taxonomy_id=data["taxonomy_id"],
            scientific_name=data["scientific_name"],
            strain=data["strain"],
        )
        # TODO: fetch common_name(s) from ncbi_taxonomy database

        organism = ensembl_metadata_pb2.Organism(
            display_name=data["display_name"],
            strain=data["strain"],
            scientific_name=data["scientific_name"],
            url_name=data["url_name"],
            ensembl_name=data["ensembl_name"],
            scientific_parlance_name=data["scientific_parlance_name"],
        )

        release = ensembl_metadata_pb2.Release(
            release_version=data["release_version"],
            release_date=str(data["release_date"]),
            release_label=data["release_label"],
            is_current=data["is_current"],
        )

        genome = ensembl_metadata_pb2.Genome(
            genome_uuid=data["genome_uuid"],
            created=str(data["created"]),
            assembly=assembly,
            taxon=taxon,
            organism=organism,
            release=release,
        )
    return genome


def create_genome_sequence(data=None):
    if data is None:
        return ensembl_metadata_pb2.GenomeSequence()

    genome_sequence = ensembl_metadata_pb2.GenomeSequence(
        accession=data.AssemblySequence.accession,
        name=data.AssemblySequence.name,
        sequence_location=data.AssemblySequence.sequence_location,
        length=data.AssemblySequence.length,
        chromosomal=data.AssemblySequence.chromosomal
    )

    return genome_sequence


def create_release(data=None):
    if data is None:
        return ensembl_metadata_pb2.Release()

    release = ensembl_metadata_pb2.Release(
        release_version=data.EnsemblRelease.version,
        release_date=str(data.EnsemblRelease.release_date),
        release_label=data.EnsemblRelease.label,
        is_current=data.EnsemblRelease.is_current,
        site_name=data.EnsemblSite.name,
        site_label=data.EnsemblSite.label,
        site_uri=data.EnsemblSite.uri
    )

    return release


def create_datasets(data=None):
    if data is None:
        return ensembl_metadata_pb2.Datasets()

    return ensembl_metadata_pb2.Datasets(
        genome_uuid=data["genome_uuid"], datasets=data["datasets"]
    )


def create_dataset_info(data=None):
    if data is None:
        return ensembl_metadata_pb2.DatasetInfos.DatasetInfo()

    return ensembl_metadata_pb2.DatasetInfos.DatasetInfo(
        dataset_uuid=data.Dataset.dataset_uuid,
        dataset_name=data.Dataset.name,
        name=data.Attribute.name,
        type=data.Attribute.type,
        dataset_version=data.Dataset.version,
        dataset_label=data.Dataset.label,
        version=int(data.EnsemblRelease.version),
        value=data.DatasetAttribute.value,
    )


def create_dataset_infos(genome_uuid=None, requested_dataset_type=None, data=None):
    if data is None or data == []:
        return ensembl_metadata_pb2.DatasetInfos()

    dataset_infos = [create_dataset_info(result) for result in data]
    return ensembl_metadata_pb2.DatasetInfos(
        genome_uuid=genome_uuid,
        dataset_type=requested_dataset_type,
        dataset_infos=dataset_infos,
    )


class EnsemblMetadataServicer(ensembl_metadata_pb2_grpc.EnsemblMetadataServicer):
    def __init__(self):
        self.db, self.taxo_db = load_database()

    def GetSpeciesInformation(self, request, context):
        return get_species_information(self.db, self.taxo_db, request.genome_uuid)

    def GetAssemblyInformation(self, request, context):
        return get_assembly_information(self.db, request.assembly_uuid)

    def GetGenomesByAssemblyAccessionID(self, request, context):
        return get_genomes_from_assembly_accession_iterator(
            self.db, request.assembly_accession
        )

    def GetSubSpeciesInformation(self, request, context):
        return get_sub_species_info(self.db, request.organism_uuid)

    def GetGroupingInformation(self, request, context):
        return get_grouping_info(self.db, request.organism_uuid)

    def GetKaryotypeInformation(self, request, context):
        return get_karyotype_information(self.db, request.genome_uuid)

    def GetTopLevelStatistics(self, request, context):
        return get_top_level_statistics(self.db, request.organism_uuid)

    def GetTopLevelStatisticsByUUID(self, request, context):
        return get_top_level_statistics_by_uuid(self.db, request.genome_uuid)

    def GetGenomeUUID(self, request, context):
        return get_genome_uuid(self.db, request.ensembl_name, request.assembly_name)

    def GetGenomeByUUID(self, request, context):
        return get_genome_by_uuid(self.db, request.genome_uuid, request.release_version)

    def GetGenomesByKeyword(self, request, context):
        return get_genomes_by_keyword_iterator(
            self.db, request.keyword, request.release_version
        )

    def GetGenomeByName(self, request, context):
        return get_genome_by_name(
            self.db, request.ensembl_name, request.site_name, request.release_version
        )

    def GetRelease(self, request, context):
        return release_iterator(
            self.db, request.site_name, request.release_version, request.current_only
        )

    def GetReleaseByUUID(self, request, context):
        return release_by_uuid_iterator(self.db, request.genome_uuid)

    def GetGenomeSequence(self, request, context):
        return genome_sequence_iterator(
            self.db, request.genome_uuid, request.chromosomal_only
        )

    def GetDatasetsListByUUID(self, request, context):
        return get_datasets_list_by_uuid(
            self.db, request.genome_uuid, request.release_version
        )

    def GetDatasetInformation(self, request, context):
        return get_dataset_by_genome_id(
            self.db, request.genome_uuid, request.dataset_type
        )


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    ensembl_metadata_pb2_grpc.add_EnsemblMetadataServicer_to_server(
        EnsemblMetadataServicer(), server
    )
    server.add_insecure_port("[::]:50051")
    server.start()
    server.wait_for_termination()


if __name__ == "__main__":
    logging.basicConfig()
    serve()
