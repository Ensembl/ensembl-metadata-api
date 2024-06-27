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
import logging

import ensembl.production.metadata.grpc.protobuf_msg_factory as msg_factory
from ensembl.production.metadata.api.models import Genome
from ensembl.production.metadata.grpc import ensembl_metadata_pb2
from ensembl.production.metadata.grpc.adaptors.genome import GenomeAdaptor
from ensembl.production.metadata.grpc.adaptors.release import ReleaseAdaptor
from ensembl.production.metadata.grpc.config import MetadataConfig

logger = logging.getLogger(__name__)


def connect_to_db():
    conn = GenomeAdaptor(
        metadata_uri=MetadataConfig().metadata_uri,
        taxonomy_uri=MetadataConfig().taxon_uri
    )
    return conn


def get_alternative_names(db_conn, taxon_id):
    """ Get alternative names for a given taxon ID """
    taxon_ifo = db_conn.fetch_taxonomy_names(taxon_id)
    alternative_names = taxon_ifo[taxon_id].get('synonym')
    genbank_common_name = taxon_ifo[taxon_id].get('genbank_common_name')

    if genbank_common_name is not None:
        alternative_names.append(genbank_common_name)

    # remove duplicates
    unique_alternative_names = list(set(alternative_names))
    # sort before returning (otherwise the test breaks)
    sorted_unique_alternative_names = sorted(unique_alternative_names)
    logger.debug(sorted_unique_alternative_names)
    return sorted_unique_alternative_names


def get_top_level_statistics(db_conn, organism_uuid):
    if not organism_uuid:
        logger.warning("Missing or Empty Organism UUID field.")
        return msg_factory.create_top_level_statistics()
    # FIXME get best genome for organism and fetch from genome
    genomes = db_conn.fetch_genomes(organism_uuid=organism_uuid)
    #Todo fetch_genome returns duplicate genome uuids  if a genome assigned to multiple release and param allow_unreleased set to true
    stats_results = db_conn.fetch_genome_datasets(genome_uuid=list({genome.Genome.genome_uuid for genome in genomes}),
                                                  dataset_type_name="all")

    if len(stats_results) > 0:
        stats_by_genome_uuid = msg_factory.create_stats_by_genome_uuid(stats_results)
        response_data = msg_factory.create_top_level_statistics({
            'organism_uuid': organism_uuid,
            'stats_by_genome_uuid': stats_by_genome_uuid
        })
        # logger.debug(f"Response data: \n{response_data}")
        return response_data

    logger.debug("No top level stats found.")
    return msg_factory.create_top_level_statistics()

def get_top_level_statistics_by_uuid(db_conn, genome_uuid):
    if not genome_uuid:
        logger.warning("Missing or Empty Genome UUID field.")
        return msg_factory.create_top_level_statistics_by_uuid()

    stats_results = db_conn.fetch_genome_datasets(genome_uuid=genome_uuid, dataset_type_name="all")

    statistics = []
    # FIXME stats_results can contain multiple entries
    if len(stats_results) > 0:

        for dataset in stats_results[0].datasets:
            for attribute in dataset.attributes:
                statistics.append({
                    'name': attribute.name,
                    'label': attribute.label,
                    'statistic_type': attribute.type,
                    'statistic_value': attribute.value
                })

        statistics.sort(key=lambda x: x['name'])
        response_data = msg_factory.create_top_level_statistics_by_uuid(
            ({"genome_uuid": genome_uuid, "statistics": statistics})
        )
        # logger.debug(f"Response data: \n{response_data}")
        return response_data

    logger.debug("No top level stats found.")
    return msg_factory.create_top_level_statistics_by_uuid()


def get_assembly_information(db_conn, assembly_uuid):
    if not assembly_uuid:
        logger.warning("Missing or Empty Assembly UUID field.")
        return msg_factory.create_assembly_info()

    assembly_results = db_conn.fetch_sequences(
        assembly_uuid=assembly_uuid
    )
    if len(assembly_results) > 0:
        response_data = msg_factory.create_assembly_info(assembly_results[0])
        # logger.debug(f"Response data: \n{response_data}")
        return response_data

    logger.debug("No assembly information was found.")
    return msg_factory.create_assembly_info()


# TODO: move this function to protobuf_msg_factory.py file
def create_genome_with_attributes_and_count(db_conn, genome, release_version):
    # we fetch attributes related to that genome
    # TODO This is not sure that this fetch_genome_datasets is always needed, depending on message wanted to be returned
    #   A more specialized approached is to be defined to simplify the whole stack
    attrib_data_results = db_conn.fetch_genome_datasets(genome_uuid=genome.Genome.genome_uuid,
                                                        dataset_type_name="all",
                                                        release_version=release_version)

    logger.debug(f"Genome Datasets Retrieved: {attrib_data_results}")
    attribs = []
    if len(attrib_data_results) > 0:
        for dataset in attrib_data_results[0].datasets:
            attribs.extend(dataset.attributes)

    # fetch related assemblies count
    related_assemblies_count = db_conn.fetch_assemblies_count(None)

    alternative_names = get_alternative_names(db_conn, genome.Organism.taxonomy_id)

    return msg_factory.create_genome(
        data=genome,
        attributes=attribs,
        count=related_assemblies_count,
        alternative_names=alternative_names
    )


def get_genomes_from_assembly_accession_iterator(db_conn, assembly_accession, release_version):
    if not assembly_accession:
        logger.warning("Missing or Empty Assembly accession field.")
        return msg_factory.create_genome()
    # TODO: Add try except to the other functions as well
    try:
        genome_results = db_conn.fetch_genomes(assembly_accession=assembly_accession)
    except Exception as e:
        logger.error(f"Error fetching genomes: {e}")
        raise

    for genome in genome_results:
        yield msg_factory.create_genome(data=genome)

    return msg_factory.create_genome()


def get_species_information(db_conn, genome_uuid):
    if not genome_uuid:
        logger.warning("Missing or Empty Genome UUID field.")
        return msg_factory.create_species()
    species_results = db_conn.fetch_genomes(genome_uuid=genome_uuid)
    # TODO Patchy updates to fetch only the first one from results in case the genome is in multiple
    #  release == strongly based on assertion that results are ordered by EnsemblRelease.version.desc()
    if len(species_results) == 0:
        logger.error(f"Species not found for genome {genome_uuid}")
        return msg_factory.create_species()
    else:
        if len(species_results) > 1:
            logger.warning(f"Multiple results returned for {genome_uuid}.")
        tax_id = species_results[0].Organism.taxonomy_id
        taxo_results = db_conn.fetch_taxonomy_names(tax_id)
        response_data = msg_factory.create_species(species_results[0], taxo_results[tax_id])
        return response_data


def get_sub_species_info(db_conn, organism_uuid, group):
    if not organism_uuid:
        logger.warning("Missing or Empty Organism UUID field.")
        return msg_factory.create_sub_species()
    sub_species_results = db_conn.fetch_genomes(organism_uuid=organism_uuid, group=group)

    species_name = []
    species_type = []
    if len(sub_species_results) > 0:
        for result in sub_species_results:
            if result.OrganismGroup.type not in species_type:
                species_type.append(result.OrganismGroup.type)
            if result.OrganismGroup.name not in species_name:
                species_name.append(result.OrganismGroup.name)

        response_data = msg_factory.create_sub_species({
            'organism_uuid': organism_uuid,
            'species_type': species_type,
            'species_name': species_name
        })
        # logger.debug(f"Response data: \n{response_data}")
        return response_data

    logger.debug("No sub-species information was found.")
    return msg_factory.create_sub_species()


def get_genome_uuid(db_conn: GenomeAdaptor,
                    production_name: str,
                    assembly_name: str,
                    genebuild_date: str = None,
                    use_default: bool = False,
                    release_version: str = None):
    if production_name and assembly_name:
        genome_uuid_result = db_conn.fetch_genomes_by_assembly_name_genebuild(assembly=assembly_name,
                                                                              genebuild=genebuild_date,
                                                                              production_name=production_name,
                                                                              use_default=use_default,
                                                                              release_version=release_version)

        if len(genome_uuid_result) == 0:
            logger.error(f"No Genome found for params {production_name}")
        else:
            if len(genome_uuid_result) > 1:
                logger.warning(f"Multiple results returned. {genome_uuid_result}")
            response_data = msg_factory.create_genome_uuid(
                {"genome_uuid": genome_uuid_result[0].Genome.genome_uuid}
            )
            return response_data
    logger.warning("Missing or Empty production_name or assembly_name field.")
    return msg_factory.create_genome_uuid()


def get_genome_by_uuid(db_conn, genome_uuid, release_version):
    if not genome_uuid:
        logger.warning("Missing or Empty Genome UUID field.")
        return msg_factory.create_genome()
    genome_results = db_conn.fetch_genomes(genome_uuid=genome_uuid, release_version=release_version)
    if len(genome_results) == 0:
        logger.error(f"No Genome/Release found: {genome_uuid}/{release_version}")
    else:
        if len(genome_results) > 1:
            logger.warning(f"Multiple results returned. {genome_results}")
        response_data = create_genome_with_attributes_and_count(
            db_conn=db_conn, genome=genome_results[0], release_version=release_version
        )
        return response_data
    return msg_factory.create_genome()


def get_genomes_by_keyword_iterator(db_conn, keyword, release_version=None):
    if not keyword:
        logger.warning("Missing or Empty Keyword field.")
        return msg_factory.create_genome()

    genome_results = db_conn.fetch_genome_by_keyword(
        keyword=keyword,
        release_version=release_version
    )

    if len(genome_results) > 0:
        # Create an empty list to store the most recent genomes
        most_recent_genomes = []
        # Group `genome_results` based on the `assembly_accession` field
        for _, genome_release_group in itertools.groupby(genome_results, lambda r: r.Assembly.accession):
            # Sort the genomes in each group based on the `release_version` field in descending order
            sorted_genomes = sorted(genome_release_group, key=lambda
                g: g.EnsemblRelease.version if g.EnsemblRelease is not None else g.Genome.genome_uuid, reverse=True)
            # Select the most recent genome from the sorted group (first element)
            most_recent_genome = sorted_genomes[0]
            # Add the most recent genome to the `most_recent_genomes` list
            most_recent_genomes.append(most_recent_genome)

        for genome_row in most_recent_genomes:
            yield msg_factory.create_genome(data=genome_row)

    logger.debug("No genomes were found.")
    return msg_factory.create_genome()


def get_genome_by_name(db_conn, biosample_id, site_name, release_version):
    if not biosample_id and not site_name:
        logger.warning("Missing or Empty ensembl_name and site_name field.")
        return msg_factory.create_genome()
    genome_results = db_conn.fetch_genomes(biosample_id=biosample_id, site_name=site_name,
                                           release_version=release_version)
    if len(genome_results) == 0:
        logger.error(f"Genome not found for biosample. {biosample_id}")
    else:
        if len(genome_results) > 1:
            logger.warning(f"Multiple results returned. {genome_results}")
        response_data = create_genome_with_attributes_and_count(
            db_conn=db_conn, genome=genome_results[0], release_version=release_version
        )
        return response_data
    return msg_factory.create_genome()


def get_datasets_list_by_uuid(db_conn, genome_uuid, release_version):
    if not genome_uuid:
        logger.warning("Missing or Empty Genome UUID field.")
        return msg_factory.create_datasets()
    datasets_results = db_conn.fetch_genome_datasets(genome_uuid=genome_uuid, dataset_type_name="all",
                                                     release_version=release_version)
    # FIXME dataset_results can contain multiple genomes
    if len(datasets_results) > 0:
        ds_obj_dict = {}
        for result in datasets_results[0].datasets:
            dataset_type = result.dataset.dataset_type_id
            # Populate the objects bottom up
            datasets_info = msg_factory.populate_dataset_info(result)
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

        response_data = msg_factory.create_datasets({
            'genome_uuid': genome_uuid,
            'datasets': dataset_object_dict
        })
        # logger.debug(f"Response data: \n{response_data}")
        return response_data

    logger.debug("No datasets found.")
    return msg_factory.create_datasets()


def genome_sequence_iterator(db_conn, genome_uuid, chromosomal_only):
    if not genome_uuid:
        logger.warning("Missing or Empty Genome UUID field.")
        return

    assembly_sequence_results = db_conn.fetch_sequences(
        genome_uuid=genome_uuid,
        chromosomal_only=chromosomal_only,
    )
    for result in assembly_sequence_results:
        logger.debug(f"Processing assembly: {result.AssemblySequence.name}")
        yield msg_factory.create_genome_sequence(result)


def assembly_region_iterator(db_conn, genome_uuid, chromosomal_only):
    if not genome_uuid:
        logger.warning("Missing or Empty Genome UUID field.")
        return

    assembly_sequence_results = db_conn.fetch_sequences(
        genome_uuid=genome_uuid,
        chromosomal_only=chromosomal_only,
    )
    for result in assembly_sequence_results:
        logger.debug(f"Processing assembly: {result.AssemblySequence.name}")
        yield msg_factory.create_assembly_region(result)


def genome_assembly_sequence_region(db_conn, genome_uuid, sequence_region_name):
    if not genome_uuid or not sequence_region_name:
        logger.warning("Missing or Empty Genome UUID or Sequence region name field.")
        return msg_factory.create_genome_assembly_sequence_region()

    assembly_sequence_results = db_conn.fetch_sequences(
        genome_uuid=genome_uuid,
        assembly_sequence_name=sequence_region_name
    )
    if len(assembly_sequence_results) == 0:
        logger.error(f"Assembly sequence not found for {genome_uuid}/{sequence_region_name}")
    else:
        if len(assembly_sequence_results) > 1:
            logger.warning(f"Multiple results returned for {genome_uuid}/{sequence_region_name}")
        response_data = msg_factory.create_genome_assembly_sequence_region(assembly_sequence_results[0])
        return response_data
    return msg_factory.create_genome_assembly_sequence_region()


def release_iterator(metadata_db, site_name, release_version, current_only):
    conn = ReleaseAdaptor(metadata_uri=MetadataConfig().metadata_uri)

    # set release_version/site_name to None if it's an empty list
    release_version = release_version or None
    site_name = site_name or None

    release_results = conn.fetch_releases(release_version=release_version, current_only=current_only)

    for result in release_results:
        logger.debug(
            f"Processing release: {result.EnsemblRelease.version if hasattr(result, 'EnsemblRelease') else None}")
        yield msg_factory.create_release(result)


def release_by_uuid_iterator(metadata_db, genome_uuid):
    if not genome_uuid:
        return

    conn = ReleaseAdaptor(metadata_uri=MetadataConfig().metadata_uri)
    release_results = conn.fetch_releases_for_genome(
        genome_uuid=genome_uuid,
    )

    for result in release_results:
        logger.debug(
            f"Processing release: {result.EnsemblRelease.version if hasattr(result, 'EnsemblRelease') else None}")
        yield msg_factory.create_release(result)


def get_dataset_by_genome_and_dataset_type(db_conn, genome_uuid, requested_dataset_type='assembly'):
    if not genome_uuid:
        logger.warning("Missing or Empty Genome UUID field.")
        return msg_factory.create_dataset_infos()

    dataset_results = db_conn.fetch_genome_datasets(genome_uuid=genome_uuid,
                                                    dataset_type_name=requested_dataset_type)
    logger.debug("dataset Results %s", dataset_results)
    if len(dataset_results) == 0:
        logger.error(f"No data for {genome_uuid} / {requested_dataset_type}")
        return {}
    else:
        # FIXME it's possible that multiple datasets are returned here. released multiple times.
        if len(dataset_results) > 1:
            logger.warning(f"Multiple results for {genome_uuid} / {requested_dataset_type}")
        response_data = msg_factory.create_dataset_infos(genome_uuid, requested_dataset_type, dataset_results[0])
        return response_data


def get_organisms_group_count(db_conn, release_version):
    count_result = db_conn.fetch_organisms_group_counts(release_version=release_version)
    response_data = msg_factory.create_organisms_group_count(count_result, release_version)
    # logger.debug(f"Response data: \n{response_data}")
    return response_data


def get_genome_uuid_by_tag(db_conn, genome_tag):
    if not genome_tag:
        logger.warning("Missing or Empty Genome tag field.")
        return msg_factory.create_genome_uuid()

    genome_uuid_result = db_conn.fetch_genomes(genome_tag=genome_tag)
    if len(genome_uuid_result) == 0:
        logger.error(f"No Genome UUID found. {genome_tag}")
    else:
        if len(genome_uuid_result) > 1:
            logger.warning(f"Multiple results returned. {genome_uuid_result}")
        response_data = msg_factory.create_genome_uuid(
            {"genome_uuid": genome_uuid_result[0].Genome.genome_uuid}
        )
        return response_data
    return msg_factory.create_genome_uuid()


def get_ftp_links(db_conn, genome_uuid, dataset_type, release_version):
    # Request is sending an empty string '' instead of None when
    # an input parameter is not supplied by the user
    if not genome_uuid:
        logger.warning("Missing or Empty Genome UUID field.")
        return msg_factory.create_paths()
    if not dataset_type:
        dataset_type = 'all'
    if not release_version:
        release_version = None

    # Find the Genome
    with db_conn.metadata_db.session_scope() as session:
        genome = session.query(Genome).filter(Genome.genome_uuid == genome_uuid).first()

        # Return empty links if Genome is not found
        if genome is None:
            logger.debug("No Genome found.")
            return msg_factory.create_paths()

        # Find the links for the given dataset.
        # Note: release_version filtration is not implemented in the API yet
        try:
            links = genome.get_public_path(dataset_type=dataset_type, release=release_version)
        except (ValueError, RuntimeError) as error:
            # log the errors to error log and return empty list of links
            logger.error(f"Error fetching links: {error}")
            return msg_factory.create_paths()

    if len(links) > 0:
        response_data = msg_factory.create_paths(data=links)
        # logger.debug(f"Response data: \n{response_data}")
        return response_data

    logger.debug("No Genome found.")
    return msg_factory.create_paths()


def get_release_version_by_uuid(db_conn, genome_uuid, dataset_type, release_version):
    if not genome_uuid:
        logger.warning("Missing or Empty Genome UUID field.")
        return msg_factory.create_release_version()

    release_version_result = db_conn.fetch_genome_datasets(genome_uuid=genome_uuid,
                                                           dataset_type_name=dataset_type,
                                                           release_version=release_version)

    if len(release_version_result) == 0:
        logger.error(f"No result found for {genome_uuid}/{dataset_type}/{release_version}")
    else:
        if len(release_version_result) > 1:
            logger.warning(f"Multiple results returned. {release_version_result}")
        response_data = msg_factory.create_release_version(release_version_result[0])
        return response_data
    return msg_factory.create_release_version()


def get_attributes_values_by_uuid(db_conn, genome_uuid, dataset_type, release_version):
    if not genome_uuid:
        logger.warning("Missing or Empty Genome UUID field.")
        return msg_factory.create_attribute_value()

    dataset_results = db_conn.fetch_genome_datasets(
        genome_uuid=genome_uuid,
        dataset_type_name=dataset_type,
        release_version=release_version
    )

    if len(dataset_results) == 1:
        response_data = msg_factory.create_attribute_value(data=dataset_results)
        logger.debug(f"Response data: \n{response_data}")
        return response_data

    elif len(dataset_results) > 1:
        logger.debug("Multiple results returned.")
    else:
        logger.debug("Genome not found.")

    logger.debug("No attribute values were found.")
    return msg_factory.create_attribute_value()
