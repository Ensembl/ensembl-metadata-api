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
import uuid
from typing import Type, Any
from datetime import datetime

import ensembl.production.metadata.grpc.protobuf_msg_factory as msg_factory
from ensembl.production.metadata.api.adaptors import GenomeAdaptor, BaseAdaptor
from ensembl.production.metadata.api.adaptors import ReleaseAdaptor
from ensembl.production.metadata.api.models import Genome
from ensembl.production.metadata.grpc.config import MetadataConfig

logger = logging.getLogger(__name__)


def connect_to_db(adaptor_class: Type[BaseAdaptor], **kwargs):
    """
    Connect to the database using the specified adaptor class.

    :param adaptor_class: The class of the adaptor to instantiate (e.g., GenomeAdaptor, VepAdaptor).
    :param kwargs: Additional arguments to pass to the adaptor's constructor.
    :return: An instance of the specified adaptor class.
    """
    conn = adaptor_class(
        metadata_uri=MetadataConfig().metadata_uri,
        **kwargs
    )
    return conn


def is_valid_uuid(value):
    try:
        # Attempt to create a UUID object from the input
        uuid_obj = uuid.UUID(value)
        return True
    except (ValueError, AttributeError, TypeError):
        return False


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


# TODO: move this function to protobuf_msg_factory.py file?
def create_genome_with_attributes_and_count(db_conn, genome, release_version):
    attrib_data_results = db_conn.fetch_genome_datasets(genome_uuid=genome.Genome.genome_uuid,
                                                        dataset_type_name="all",
                                                        release_version=release_version)

    logger.debug(f"Genome Datasets Retrieved: {attrib_data_results}")
    attribs = []
    if len(attrib_data_results) > 0:
        for dataset in attrib_data_results[0].datasets:
            attribs.extend(dataset.attributes)

    # fetch related assemblies count
    related_assemblies_count = db_conn.fetch_assemblies_count(genome.Organism.species_taxonomy_id)

    alternative_names = get_alternative_names(db_conn, genome.Organism.species_taxonomy_id)

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
            db_conn=db_conn,
            genome=genome_results[0],
            release_version=release_version
        )
        return response_data
    return msg_factory.create_genome()


def get_brief_genome_details_by_uuid(db_conn, genome_uuid_or_tag, release_version):
    """
    Fetch brief genome details by UUID or tag and release version.

    Args:
        db_conn: Database connection object.
        genome_uuid_or_tag: Genome UUID or tag.
        release_version: Release version to fetch.

    Returns:
        A dictionary containing brief genome details.
    """
    if not genome_uuid_or_tag:
        logger.warning("Missing or Empty Genome UUID field.")
        return msg_factory.create_brief_genome_details()

    # If genome_uuid_or_tag is not a valid UUID, assume it's a tag and fetch genome_uuid
    if not is_valid_uuid(genome_uuid_or_tag):
        logger.debug(f"Invalid genome_uuid {genome_uuid_or_tag}, assuming it's a tag and using it to fetch genome_uuid")
        # For tag (URL name), we only care about the latest integrated release.
        # For archives, we will need to keep in mind the combination of release and tag
        # that will take the user to the archived version of the genome.
        genome_results = db_conn.fetch_genomes(
            genome_tag=genome_uuid_or_tag,
            # release_type="integrated", #  Add this once we have tags linked only to integrated releases
            release_version=release_version
        )
    else:
        genome_uuid = genome_uuid_or_tag
        genome_results = db_conn.fetch_genomes(genome_uuid=genome_uuid, release_version=release_version)

    if not genome_results:
        logger.error(f"No Genome/Release found: {genome_uuid_or_tag}/{release_version}")
        return msg_factory.create_brief_genome_details()

    if len(genome_results) > 1:
        logger.warning(f"Multiple results found for Genome UUID/Release version: {genome_uuid_or_tag}/{release_version}")
        # means that this genome is released in both a partial and integrated release
        # we get the integrated release specifically since it's the one we are interested in
        genome_results = [res for res in genome_results if res.EnsemblRelease.release_type == "integrated"]

    # Get the current (requested) genome
    current_genome = genome_results[0]
    assembly_name = current_genome.Assembly.name
    # Fetch all genomes with the same assembly name, sorted by release date
    all_genomes_with_same_assembly = db_conn.fetch_genomes(assembly_name=assembly_name)
    
    # Find the genome with the most recent release date
    latest_genome = None
    if all_genomes_with_same_assembly:
        # First genome should be the latest due to ordering in fetch_genomes
        if all_genomes_with_same_assembly[0].Genome.genome_uuid != current_genome.Genome.genome_uuid:
            latest_genome = all_genomes_with_same_assembly[0]
            logger.debug(f"Found newer genome: {latest_genome.Genome.genome_uuid}")
    
    # Return the requested genome together with the latest genome details (or None if current is latest)
    return msg_factory.create_brief_genome_details(current_genome, latest_genome)


def get_attributes_by_genome_uuid(db_conn, genome_uuid, release_version):
    if not genome_uuid:
        logger.warning("Missing or Empty Genome UUID field.")
        return msg_factory.create_attributes_by_genome_uuid()

    attrib_data_results = db_conn.fetch_genome_datasets(genome_uuid=genome_uuid,
                                                        dataset_type_name="all",
                                                        release_version=release_version)

    logger.debug(f"Genome Datasets Retrieved: {attrib_data_results}")

    if len(attrib_data_results) == 0:
        logger.error(f"No Attributes were found: {genome_uuid}/{release_version}")

    else:
        if len(attrib_data_results) > 0:
            attribs = []
            for dataset in attrib_data_results[0].datasets:
                attribs.extend(dataset.attributes)

            attributes_info = msg_factory.create_attributes_info(attribs)
            return msg_factory.create_attributes_by_genome_uuid(
                genome_uuid=genome_uuid,
                attributes_info=attributes_info
            )
    return msg_factory.create_attributes_by_genome_uuid()


def get_genomes_by_specific_keyword_iterator(
    db_conn, tolid, assembly_accession_id, assembly_name, ensembl_name,
    common_name, scientific_name, scientific_parlance_name, species_taxonomy_id,
    release_version=None
):
    if (not tolid and assembly_accession_id and assembly_name and ensembl_name and
            common_name and scientific_name and scientific_parlance_name and species_taxonomy_id):
        logger.warning("Missing required field")
        return msg_factory.create_genome()

    try:
        genome_results = db_conn.fetch_genome_by_specific_keyword(
            tolid, assembly_accession_id, assembly_name, ensembl_name,
            common_name, scientific_name, scientific_parlance_name,
            species_taxonomy_id, release_version
        )

        if len(genome_results) > 0:
            # Create an empty list to store the genomes list
            genomes_list = []
            # sort genomes based on the `assembly_accession` field since we are going to group by it
            genome_results.sort(key=lambda r: r.Assembly.accession)
            # Group `genome_results` based on the `assembly_accession` field
            for _, genome_release_group in itertools.groupby(genome_results, lambda r: r.Assembly.accession):
                # Sort the genomes in each group based on the `genome_uuid` field to prepare for grouping
                sorted_genomes = sorted(genome_release_group, key=lambda g: g.Genome.genome_uuid)
                # group by genome uuid incase of partial and integrated releases
                for _, genome_uuid_group in itertools.groupby(sorted_genomes, lambda g: g.Genome.genome_uuid):
                    genome_uuid_group = list(genome_uuid_group)
                    if len(genome_uuid_group) > 1:                    
                        # sort by release date descending. The last code checked if EnsemblRelease exists. If it doesn't it uses a default date and not genome uuid
                        sorted_genome_uuid_group = sorted(
                            genome_uuid_group,
                            key=lambda g: getattr(g.EnsemblRelease, 'release_date', datetime.strptime('1900-01-01', '%Y-%m-%d')) if g.EnsemblRelease else datetime.strptime('1900-01-01', '%Y-%m-%d'),
                            reverse=True
                        )
                        # check for integrated release in group
                        integrated_genome = [
                            g for g in sorted_genome_uuid_group
                            if g.EnsemblRelease and getattr(g.EnsemblRelease, 'release_type', None) == 'integrated'
                        ]
                        if len(integrated_genome) > 0:
                            genomes_list.append(integrated_genome[0])
                        
                        # if no integrated release, just take the first one, which is the most recent partial release
                        else:                            
                            genomes_list.append(sorted_genome_uuid_group[0])
                    # if only one genome in the group, just add it to the list
                    else:
                        genomes_list.append(list(genome_uuid_group)[0])                                                        

            for genome_row in genomes_list:
                yield msg_factory.create_genome(data=genome_row)
                
    except Exception as e:
        logger.error(f"Error fetching genomes: {e}")
        return msg_factory.create_genome()

    logger.debug("No genomes were found.")
    return msg_factory.create_genome()


def get_genomes_by_release_version_iterator(
    db_conn,release_version
):
    if (not release_version):
        logger.warning("Missing required release_version")
        return msg_factory.create_brief_genome_details()

    genome_results = db_conn.fetch_genome_by_release_version(release_version)

    if len(genome_results) > 0:
        for genome_row in genome_results:
            yield msg_factory.create_brief_genome_details(data=genome_row)
    else:
        logger.debug("No genomes were found.")
        return msg_factory.create_brief_genome_details()


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
    if len(datasets_results) > 0:
        # FIXME dataset_results can contain multiple genomes?
        datasets_info = msg_factory.populate_dataset_info(datasets_results[0])
        response_data = msg_factory.create_datasets({
            'genome_uuid': genome_uuid,
            'datasets': datasets_info
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


def release_iterator(metadata_db, site_name, release_label, current_only):
    conn = ReleaseAdaptor(metadata_uri=MetadataConfig().metadata_uri)

    # set release_label and site_name to None if it's an empty list
    release_label = release_label or None
    site_name = site_name or None

    release_results = conn.fetch_releases(
        site_name=site_name,
        release_label=release_label,
        current_only=current_only
    )

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
        return msg_factory.populate_dataset_info()

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

        datasets_info = msg_factory.populate_dataset_info(dataset_results[0])
        response_data = msg_factory.create_datasets({
            'genome_uuid': genome_uuid,
            'datasets': datasets_info
        })
        return response_data


def get_organisms_group_count(db_conn, release_label):
    count_result = db_conn.fetch_organisms_group_counts(release_label=release_label)
    response_data = msg_factory.create_organisms_group_count(count_result, release_label)
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
        # Find the links for the given dataset.
        # Note: release_version filtration is not implemented in the API yet
        try:
            links = db_conn.get_public_path(
                genome_uuid=genome_uuid,
                dataset_type=dataset_type
            )
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


def get_attributes_values_by_uuid(db_conn, genome_uuid, dataset_type, release_version, attribute_names, latest_only):
    """
    Retrieve attribute values for a given genome UUID from the database.

    This function fetches genome datasets based on the provided genome UUID, dataset type, and release version.
    If a single dataset result is found, it creates and returns the attribute values. If no or multiple datasets
    are found, appropriate warnings or debug messages are logged.

    Args:
        db_conn: Database connection object.
        genome_uuid (str): The UUID of the genome to fetch data for. Must not be empty.
        dataset_type (str): The type of dataset to retrieve.
        release_version (str): The release version of the dataset to retrieve.
        attribute_names (list): A list of attribute names to filter the results by.
        latest_only (bool): Whether to fetch the latest dataset or not (default is `False`).

    Returns:
        object: A response object containing the attribute values. If no valid dataset is found,
                an empty attribute value object is returned.
    """
    if not genome_uuid:
        logger.warning("Missing or Empty Genome UUID field.")
        return msg_factory.create_attribute_value()

    genome_datasets_results = db_conn.fetch_genome_datasets(
        genome_uuid=genome_uuid,
        dataset_type_name=dataset_type,
        release_version=release_version
    )

    if len(genome_datasets_results) > 1:
        logger.debug("Multiple results returned.")
        # if we get more than one genome, it means it's attached to both partial and integrated releases
        # we pick the integrated genome because it's the one taking precedence
        genome_datasets_results = [gd for gd in genome_datasets_results if gd.release.release_type == 'integrated']

    if len(genome_datasets_results) == 1:
        response_data = msg_factory.create_attribute_value(
            data=genome_datasets_results,
            # There is no point in filtering by attribute_names in the API because it returns the whole dataset object
            # which will contain all the attributes (we should be altering them from within the API)
            attribute_names=attribute_names,
            latest_only=latest_only
        )
        logger.debug(f"Response data: \n{response_data}")
        return response_data
    else:
        logger.debug("Genome not found.")

    logger.debug("No attribute values were found.")
    return msg_factory.create_attribute_value()


def get_vep_paths_by_uuid(db_conn, genome_uuid):
    if not genome_uuid:
        logger.warning("Missing or Empty Genome UUID field.")
        return msg_factory.create_vep_file_paths()

    try:
        vep_paths = db_conn.fetch_vep_locations(genome_uuid=genome_uuid)
        if vep_paths:
            return msg_factory.create_vep_file_paths(vep_paths)
    except (ValueError, RuntimeError) as error:
        logger.error(error)

    return msg_factory.create_vep_file_paths()


def get_genome_groups_by_reference(
    db_conn: Any,
    group_type: str,
    release_label: str | None = None,
):
    if not group_type or group_type != 'structural_variant': # accepting only structural_variant for now
        logger.warning("Missing or Wrong Group type field.")
        return msg_factory.create_genome_groups_by_reference()

    # The logic calling the ORM and fetching data from the DB
    # will go here, we are returning dummy data for now
    # /!\ Remember to handle the release label

    try:
        # The logic calling the ORM and fetching data from the DB
        # will go here. For now, we return dummy data.
        dummy_data = [
            {
                "group_id": "grch38-group",
                "group_type": group_type,
                "group_name": "",
                "reference_genome": {
                    "genome_uuid": "a7335667-93e7-11ec-a39d-005056b38ce3",
                    "assembly": {
                        "accession": "GCA_000001405.29",
                        "name": "GRCh38.p14",
                        "ucsc_name": "hg38",
                        "level": "chromosome",
                        "ensembl_name": "GRCh38.p14",
                        "assembly_uuid": "fd7fea38-981a-4d73-a879-6f9daef86f08",
                        "is_reference": True,
                        "url_name": "grch38",
                        "tol_id": "",
                    },
                    "taxon": {
                        "taxonomy_id": 9606,
                        "scientific_name": "Homo sapiens",
                        "strain": "",
                        "alternative_names": [],
                    },
                    "created": "2023-09-22 15:04:45",
                    "organism": {
                        "common_name": "Human",
                        "strain": "",
                        "scientific_name": "Homo sapiens",
                        "ensembl_name": "SAMN12121739",
                        "scientific_parlance_name": "Human",
                        "organism_uuid": "1d336185-affe-4a91-85bb-04ebd73cbb56",
                        "strain_type": "",
                        "taxonomy_id": 9606,
                        "species_taxonomy_id": 9606,
                    },
                    "release": {
                        "release_version": 1,
                        "release_date": "2025-02-27",
                        "release_label": "2025-02",
                        "release_type": "integrated",
                        "is_current": True,
                        "site_name": "Ensembl",
                        "site_label": "MVP ENsembl",
                        "site_uri": "https://beta.ensembl.org",
                    },
                },
            },
            {
                "group_id": "t2t-group",
                "group_type": group_type,
                "group_name": "",
                "reference_genome": {
                    "genome_uuid": "4c07817b-c7c5-463f-8624-982286bc4355",
                    "assembly": {
                        "accession": "GCA_009914755.4",
                        "name": "T2T-CHM13v2.0",
                        "ucsc_name": "",
                        "level": "primary_assembly",
                        "ensembl_name": "T2T-CHM13v2.0",
                        "assembly_uuid": "fc20ebd6-f756-45da-b941-b3b17e11515f",
                        "is_reference": False,
                        "url_name": "t2t-chm13",
                        "tol_id": "",
                    },
                    "taxon": {
                        "taxonomy_id": 9606,
                        "scientific_name": "Homo sapiens",
                        "strain": "",
                        "alternative_names": [],
                    },
                    "created": "2023-09-22 15:06:39",
                    "organism": {
                        "common_name": "Human",
                        "strain": "",
                        "scientific_name": "Homo sapiens",
                        "ensembl_name": "SAMN03255769",
                        "scientific_parlance_name": "Human",
                        "organism_uuid": "9df68864-e9fe-4c02-ab8c-8190baad16c6",
                        "strain_type": "",
                        "taxonomy_id": 9606,
                        "species_taxonomy_id": 9606,
                    },
                    "release": {
                        "release_version": 1,
                        "release_date": "2025-02-27",
                        "release_label": "2025-02",
                        "release_type": "integrated",
                        "is_current": True,
                        "site_name": "Ensembl",
                        "site_label": "MVP ENsembl",
                        "site_uri": "https://beta.ensembl.org",
                    },
                },
            },
        ]

        # Very simple use of release_label even in dummy mode
        # TODO: move this filtering into the ORM query once the real implementation is added.
        if release_label:
            dummy_data = [
                g
                for g in dummy_data
                if g["reference_genome"]["release"]["release_label"] == release_label
            ]

        return msg_factory.create_genome_groups_by_reference(dummy_data)

    except Exception:
        # Dummy error handling until the real ORM logic is in place
        logger.exception(
            "Unexpected error while fetching genome groups "
            "(group_type=%r, release_label=%r)",
            group_type,
            release_label,
        )
        # Return an empty message to avoid propagating the error to callers.
        return msg_factory.create_genome_groups_by_reference([])


def get_genomes_in_group(
    db_conn: Any,
    group_id: str,
    release_label: str | None,
):
    if not group_id:
        logger.warning("Missing or Empty Group type field.")
        return msg_factory.create_genomes_in_group()

    try:
        # The logic calling the ORM and fetching data from the DB using group_id
        # will go here. We return dummy data for now.
        # /!\ Remember to handle the release label in the real query.

        # TODO: remove this once we have the real data from the DB.
        dummy_data = [
            {
                "genome_uuid": "a7335667-93e7-11ec-a39d-005056b38ce3",
                "assembly": {
                    "accession": "GCA_000001405.29",
                    "name": "GRCh38.p14",
                    "ucsc_name": "hg38",
                    "level": "chromosome",
                    "ensembl_name": "GRCh38.p14",
                    "assembly_uuid": "fd7fea38-981a-4d73-a879-6f9daef86f08",
                    "is_reference": True,
                    "url_name": "grch38",
                    "tol_id": "",
                },
                "taxon": {
                    "taxonomy_id": 9606,
                    "scientific_name": "Homo sapiens",
                    "strain": "",
                    "alternative_names": [],
                },
                "created": "2023-09-22 15:04:45",
                "organism": {
                    "common_name": "Human",
                    "strain": "",
                    "scientific_name": "Homo sapiens",
                    "ensembl_name": "SAMN12121739",
                    "scientific_parlance_name": "Human",
                    "organism_uuid": "1d336185-affe-4a91-85bb-04ebd73cbb56",
                    "strain_type": "",
                    "taxonomy_id": 9606,
                    "species_taxonomy_id": 9606,
                },
                "release": {
                    "release_version": 1,
                    "release_date": "2025-02-27",
                    "release_label": "2025-02",
                    "release_type": "integrated",
                    "is_current": True,
                    "site_name": "Ensembl",
                    "site_label": "MVP ENsembl",
                    "site_uri": "https://beta.ensembl.org",
                },
            },
            {
                "genome_uuid": "4c07817b-c7c5-463f-8624-982286bc4355",
                "assembly": {
                    "accession": "GCA_009914755.4",
                    "name": "T2T-CHM13v2.0",
                    "ucsc_name": "",
                    "level": "primary_assembly",
                    "ensembl_name": "T2T-CHM13v2.0",
                    "assembly_uuid": "fc20ebd6-f756-45da-b941-b3b17e11515f",
                    "is_reference": False,
                    "url_name": "t2t-chm13",
                    "tol_id": "",
                },
                "taxon": {
                    "taxonomy_id": 9606,
                    "scientific_name": "Homo sapiens",
                    "strain": "",
                    "alternative_names": [],
                },
                "created": "2023-09-22 15:06:39",
                "organism": {
                    "common_name": "Human",
                    "strain": "",
                    "scientific_name": "Homo sapiens",
                    "ensembl_name": "SAMN03255769",
                    "scientific_parlance_name": "Human",
                    "organism_uuid": "9df68864-e9fe-4c02-ab8c-8190baad16c6",
                    "strain_type": "",
                    "taxonomy_id": 9606,
                    "species_taxonomy_id": 9606,
                },
                "release": {
                    "release_version": 1,
                    "release_date": "2025-02-27",
                    "release_label": "2025-02",
                    "release_type": "integrated",
                    "is_current": True,
                    "site_name": "Ensembl",
                    "site_label": "MVP ENsembl",
                    "site_uri": "https://beta.ensembl.org",
                },
            }
        ]

        # Use release_label even in dummy mode: filter to matching releases.
        if release_label:
            dummy_data = [
                g
                for g in dummy_data
                if "release" in g
                   and g["release"].get("release_label") == release_label
            ]

        return msg_factory.create_genomes_in_group(dummy_data)

    except Exception:
        # Dummy error handling until ORM logic is implemented.
        logger.exception(
            "Unexpected error while fetching genomes in group "
            "(group_id=%r, release_label=%r)",
            group_id,
            release_label,
        )
        return msg_factory.create_genomes_in_group([])