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
from ensembl.production.metadata.grpc import ensembl_metadata_pb2

from ensembl.production.metadata.grpc.config import MetadataConfig as cfg

from ensembl.production.metadata.api.genome import GenomeAdaptor
from ensembl.production.metadata.api.release import ReleaseAdaptor

from ensembl.production.metadata.grpc.protobuf_msg_factory import create_genome, create_karyotype, \
    create_top_level_statistics, create_top_level_statistics_by_uuid, create_assembly_info, create_species, \
    create_sub_species, create_genome_uuid, create_datasets, create_genome_sequence, create_release, \
    create_dataset_infos, populate_dataset_info, create_genome_assembly_sequence, \
    create_genome_assembly_sequence_region, create_organisms_group_count, create_stats_by_genome_uuid


def connect_to_db():
    conn = GenomeAdaptor(
        metadata_uri=cfg.metadata_uri,
        taxonomy_uri=cfg.taxon_uri
    )
    return conn


def get_karyotype_information(db_conn, genome_uuid):
    if genome_uuid is None:
        return create_karyotype()

    karyotype_info_result = db_conn.fetch_sequences(
        genome_uuid=genome_uuid
    )
    # Wow! this returns 109583 rows for 'a7335667-93e7-11ec-a39d-005056b38ce3'
    # TODO: Understand what's going on
    if len(karyotype_info_result) == 1:
        return create_karyotype(karyotype_info_result[0])

    return create_karyotype()


def get_top_level_statistics(db_conn, organism_uuid, group):
    if organism_uuid is None:
        return create_top_level_statistics()

    stats_results = db_conn.fetch_genome_datasets(
        organism_uuid=organism_uuid,
        dataset_name="all",
        dataset_attributes=True
    )

    if len(stats_results) > 0:
        stats_by_genome_uuid = create_stats_by_genome_uuid(stats_results)
        return create_top_level_statistics({
            'organism_uuid': organism_uuid,
            'stats_by_genome_uuid': stats_by_genome_uuid
        })

    return create_top_level_statistics()


def get_top_level_statistics_by_uuid(db_conn, genome_uuid):
    if genome_uuid is None:
        return create_top_level_statistics_by_uuid()

    stats_results = db_conn.fetch_genome_datasets(
        genome_uuid=genome_uuid,
        dataset_name="all",
        dataset_attributes=True
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


def get_assembly_information(db_conn, assembly_uuid):
    if assembly_uuid is None:
        return create_assembly_info()

    assembly_results = db_conn.fetch_sequences(
        assembly_uuid=assembly_uuid
    )
    if len(assembly_results) > 0:
        return create_assembly_info(assembly_results[0])

    return create_assembly_info()


def create_genome_with_attributes_and_count(db_conn, genome, release_version):
    # we fetch attributes related to that genome
    attrib_data_results = db_conn.fetch_genome_datasets(
        genome_uuid=genome.Genome.genome_uuid,
        release_version=release_version,
        dataset_attributes=True
    )
    # fetch related assemblies count
    related_assemblies_count = db_conn.fetch_organisms_group_counts(
        species_taxonomy_id=genome.Organism.species_taxonomy_id
    )[0].count

    return create_genome(
        data=genome,
        attributes=attrib_data_results,
        count=related_assemblies_count
    )


def get_genomes_from_assembly_accession_iterator(db_conn, assembly_accession, release_version):
    if assembly_accession is None:
        return create_genome()

    genome_results = db_conn.fetch_genomes(
        assembly_accession=assembly_accession,
        allow_unreleased=cfg.allow_unreleased
    )
    for genome in genome_results:
        yield create_genome_with_attributes_and_count(
            db_conn=db_conn, genome=genome, release_version=release_version
        )

def get_species_information(db_conn, genome_uuid):
    if genome_uuid is None:
        return create_species()

    species_results = db_conn.fetch_genomes(
        genome_uuid=genome_uuid,
        allow_unreleased=cfg.allow_unreleased
    )
    if len(species_results) == 1:
        tax_id = species_results[0].Organism.taxonomy_id
        taxo_results = db_conn.fetch_taxonomy_names(tax_id)
        return create_species(species_results[0], taxo_results[tax_id])

    return create_species()


def get_sub_species_info(db_conn, organism_uuid, group):
    if organism_uuid is None:
        return create_sub_species()

    sub_species_results = db_conn.fetch_genomes(
        organism_uuid=organism_uuid,
        group=group,
        allow_unreleased=cfg.allow_unreleased
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


def get_genome_uuid(db_conn, ensembl_name, assembly_name, use_default=False):
    if ensembl_name is None or assembly_name is None:
        return create_genome_uuid()

    genome_uuid_result = db_conn.fetch_genomes(
        ensembl_name=ensembl_name,
        assembly_name=assembly_name,
        use_default_assembly=use_default,
        allow_unreleased=cfg.allow_unreleased
    )

    if len(genome_uuid_result) == 1:
        return create_genome_uuid(
            {"genome_uuid": genome_uuid_result[0].Genome.genome_uuid}
        )
    # PATCH: This is a special case, see EA-1112 for more details
    elif len(genome_uuid_result) == 0:
        # Try looking using only assembly_default (no ensembl_name is needed)
        using_default_assembly_only_result = db_conn.fetch_genomes(
            assembly_name=assembly_name,
            use_default_assembly=True,
            allow_unreleased=cfg.allow_unreleased
        )
        if len(using_default_assembly_only_result) == 1:
            return create_genome_uuid(
                {"genome_uuid": using_default_assembly_only_result[0].Genome.genome_uuid}
            )

    return create_genome_uuid()


def get_genome_by_uuid(db_conn, genome_uuid, release_version):
    if genome_uuid is None:
        return create_genome()

    # We first get the genome info
    genome_results = db_conn.fetch_genomes(
        genome_uuid=genome_uuid,
        release_version=release_version,
        allow_unreleased=cfg.allow_unreleased
    )

    if len(genome_results) == 1:
        return create_genome_with_attributes_and_count(
            db_conn=db_conn, genome=genome_results[0], release_version=release_version
        )

    return create_genome()


def get_genomes_by_keyword_iterator(db_conn, keyword, release_version):
    if not keyword:
        return create_genome()

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
            sorted_genomes = sorted(genome_release_group, key=lambda g: g.EnsemblRelease.version, reverse=True)
            # Select the most recent genome from the sorted group (first element)
            most_recent_genome = sorted_genomes[0]
            # Add the most recent genome to the `most_recent_genomes` list
            most_recent_genomes.append(most_recent_genome)

        for genome_row in most_recent_genomes:
            yield create_genome_with_attributes_and_count(
                db_conn=db_conn, genome=genome_row, release_version=release_version
            )

        return create_genome()


def get_genome_by_name(db_conn, ensembl_name, site_name, release_version):
    if ensembl_name is None and site_name is None:
        return create_genome()

    genome_results = db_conn.fetch_genomes(
        ensembl_name=ensembl_name,
        site_name=site_name,
        release_version=release_version,
        allow_unreleased=cfg.allow_unreleased
    )
    if len(genome_results) == 1:
        return create_genome_with_attributes_and_count(
            db_conn=db_conn, genome=genome_results[0], release_version=release_version
        )

    return create_genome()


def get_datasets_list_by_uuid(db_conn, genome_uuid, release_version):
    if genome_uuid is None:
        return create_datasets()

    datasets_results = db_conn.fetch_genome_datasets(
        genome_uuid=genome_uuid,
        # fetch all datasets, default is 'assembly' only
        dataset_name="all",
        release_version=release_version,
        allow_unreleased=cfg.allow_unreleased,
        dataset_attributes=True
    )

    if len(datasets_results) > 0:
        # ds_obj_dict where all datasets are stored as:
        # { dataset_type_1: [datasets_dt1_1, datasets_dt1_2], dataset_type_2: [datasets_dt2_1] }
        ds_obj_dict = {}
        for result in datasets_results:
            dataset_type = result.DatasetType.name
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


def genome_sequence_iterator(db_conn, genome_uuid, chromosomal_only):
    if genome_uuid is None:
        return

    assembly_sequence_results = db_conn.fetch_sequences(
        genome_uuid=genome_uuid,
        chromosomal_only=chromosomal_only,
    )
    for result in assembly_sequence_results:
        yield create_genome_sequence(result)


def genome_assembly_sequence_iterator(db_conn, genome_uuid, chromosomal_only):
    if genome_uuid is None:
        return

    assembly_sequence_results = db_conn.fetch_sequences(
        genome_uuid=genome_uuid,
        chromosomal_only=chromosomal_only,
    )
    for result in assembly_sequence_results:
        yield create_genome_assembly_sequence(result)


def genome_assembly_sequence_region(db_conn, genome_uuid, sequence_region_name, chromosomal_only):
    if genome_uuid is None or sequence_region_name is None:
        return create_genome_assembly_sequence_region()

    assembly_sequence_results = db_conn.fetch_sequences(
        genome_uuid=genome_uuid,
        assembly_sequence_name=sequence_region_name,
        chromosomal_only=chromosomal_only,
    )
    if len(assembly_sequence_results) == 1:
        return create_genome_assembly_sequence_region(assembly_sequence_results[0])

    return create_genome_assembly_sequence_region()


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


def get_dataset_by_genome_and_dataset_type(db_conn, genome_uuid, requested_dataset_type):
    if genome_uuid is None:
        return create_dataset_infos()

    dataset_results = db_conn.fetch_genome_datasets(
        genome_uuid=genome_uuid,
        dataset_type=requested_dataset_type,
        dataset_attributes=True
    )
    return create_dataset_infos(genome_uuid, requested_dataset_type, dataset_results)


def get_organisms_group_count(db_conn, release_version):
    count_result = db_conn.fetch_organisms_group_counts(release_version=release_version)
    return create_organisms_group_count(count_result, release_version)
