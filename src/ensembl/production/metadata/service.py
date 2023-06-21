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


def get_karyotype_information(metadata_db, genome_uuid):
    if genome_uuid is None:
        return create_genome()

    md = db.MetaData()
    with Session(metadata_db, future=True) as session:
        # Reflect existing tables, letting sqlalchemy load linked tables where possible.
        genome = db.Table('genome', md, autoload_with=metadata_db)
        assembly = db.Table('assembly', md, autoload_with=metadata_db)
        assembly_sequence = db.Table('assembly_sequence', md, autoload_with=metadata_db)
        karyotype_info = db.select([
            assembly.c.level,
            assembly_sequence.c.chromosomal,
            assembly_sequence.c.sequence_location,
        ]).where(genome.c.genome_uuid == genome_uuid) \
            .where(genome.c.assembly_id == assembly_sequence.c.assembly_id) \
            .where(assembly_sequence.c.assembly_id == assembly.c.assembly_id)

        karyotype_results = session.execute(karyotype_info).all()
        if len(karyotype_results) == 1:
            karyotype_data = dict(karyotype_results[0])
            karyotype_data['genome_uuid'] = genome_uuid
            return create_karyotype(karyotype_data)
        else:
            return create_karyotype()


def get_top_level_statistics(metadata_db, organism_uuid):
    if organism_uuid is None:
        return create_assembly()
    md = db.MetaData()
    with Session(metadata_db, future=True) as session:

        # Reflect existing tables, letting sqlalchemy load linked tables where possible.
        genome = db.Table('genome', md, autoload_with=metadata_db)
        genome_dataset = db.Table('genome_dataset', md, autoload_with=metadata_db)
        dataset_attribute = db.Table('dataset_attribute', md, autoload_with=metadata_db)
        attribute = db.Table('attribute', md, autoload_with=metadata_db)
        organism = db.Table('organism', md, autoload_with=metadata_db)

        stats_info = db.select([
            attribute.c.type,
            dataset_attribute.c.value,
            attribute.c.name,
            attribute.c.label
        ]).select_from(genome).select_from(organism).select_from(genome_dataset).select_from(dataset_attribute) \
            .where(genome.c.organism_id == organism.c.organism_id) \
            .where(genome.c.genome_id == genome_dataset.c.genome_id) \
            .where(genome_dataset.c.dataset_id == dataset_attribute.c.dataset_id) \
            .where(dataset_attribute.c.attribute_id == attribute.c.attribute_id) \
            .where(organism.c.organism_uuid == organism_uuid)
        print(stats_info)
        stats_results = session.execute(stats_info).all()
        statistics = []
        if len(stats_results) > 0:
            for stat_type, stat_value, name, label in stats_results:
                statistics.append({
                    'name': name,
                    'label': label,
                    'statistic_type': stat_type,
                    'statistic_value': stat_value
                })
            return create_top_level_statistics({
                'organism_uuid': organism_uuid,
                'statistics': statistics
            })
        else:
            return create_top_level_statistics()


def get_top_level_statistics_by_uuid(metadata_db, genome_uuid):
    if genome_uuid is None:
        return create_genome()
    md = db.MetaData()
    with Session(metadata_db, future=True) as session:

        # Reflect existing tables, letting sqlalchemy load linked tables where possible.
        genome = db.Table("genome", md, autoload_with=metadata_db)
        genome_dataset = db.Table("genome_dataset", md, autoload_with=metadata_db)
        dataset_attribute = db.Table("dataset_attribute", md, autoload_with=metadata_db)
        attribute = db.Table("attribute", md, autoload_with=metadata_db)

        stats_info = (
            db.select(
                [
                    attribute.c.type,
                    dataset_attribute.c.value,
                    attribute.c.name,
                    attribute.c.label,
                ]
            )
            .select_from(genome)
            .select_from(genome_dataset)
            .select_from(dataset_attribute)
            .where(genome.c.genome_uuid == genome_uuid)
            .where(genome.c.genome_id == genome_dataset.c.genome_id)
            .where(genome_dataset.c.dataset_id == dataset_attribute.c.dataset_id)
            .where(dataset_attribute.c.attribute_id == attribute.c.attribute_id)
        )

        stats_results = session.execute(stats_info).all()
        statistics = []
        if len(stats_results) > 0:
            for stat_type, stat_value, name, label in stats_results:
                statistics.append(
                    {
                        "name": name,
                        "label": label,
                        "statistic_type": stat_type,
                        "statistic_value": stat_value,
                    }
                )
            return create_top_level_statistics_by_uuid(
                ({"genome_uuid": genome_uuid, "statistics": statistics})
            )
        else:
            return create_top_level_statistics_by_uuid()


def get_assembly_information(metadata_db, assembly_uuid):
    if assembly_uuid is None:
        return create_assembly()
    md = db.MetaData()
    with Session(metadata_db, future=True) as session:
        # Reflect existing tables, letting sqlalchemy load linked tables where possible.
        assembly = db.Table('assembly', md, autoload_with=metadata_db)
        assembly_sequence = db.Table('assembly_sequence', md, autoload_with=metadata_db)
        assembly_info = db.select([
            assembly.c.accession,
            assembly.c.level,
            assembly.c.name,
            assembly_sequence.c.chromosomal,
            assembly_sequence.c.length,
            assembly_sequence.c.sequence_location,
            assembly_sequence.c.sequence_checksum,
            assembly_sequence.c.ga4gh_identifier
        ]).join(assembly_sequence).where(assembly.c.assembly_uuid == assembly_uuid)

        assembly_results = session.execute(assembly_info).all()
        print(assembly_info)
        if len(assembly_results) > 0:
            assembly_results = dict(assembly_results[0])
            assembly_results['assembly_uuid'] = assembly_uuid
            return create_assembly(assembly_results)
        else:
            return create_assembly()


def get_genomes_from_assembly_accession_iterator(metadata_db, assembly_accession):
    if assembly_accession is None:
        return
    sqlalchemy_md = db.MetaData()
    with Session(metadata_db, future=True) as session:
        genome = db.Table('genome', sqlalchemy_md, autoload_with=metadata_db)
        genome_release = db.Table('genome_release', sqlalchemy_md, autoload_with=metadata_db)
        release = sqlalchemy_md.tables['ensembl_release']
        assembly = sqlalchemy_md.tables['assembly']
        organism = sqlalchemy_md.tables['organism']

        genome_select = get_genome_query(genome, genome_release, release, assembly, organism).select_from(genome).join(
            genome_release).join(release).join(assembly).join(organism).where(
            assembly.c.accession == assembly_accession)
        genome_results = session.execute(genome_select).all()
        # print(genome_select)
        for genome in genome_results:
            yield create_genome(genome)


def get_species_information(metadata_db, taxonomy_db, genome_uuid):
    if genome_uuid is None:
        return create_genome()

    md = db.MetaData()
    with Session(metadata_db, future=True) as session:

        genome = db.Table('genome', md, autoload_with=metadata_db)
        organism = md.tables['organism']

        genome_select = db.select(
            organism.c.ensembl_name,
            organism.c.display_name,
            organism.c.taxonomy_id,
            organism.c.scientific_name,
            organism.c.strain,
            organism.c.scientific_parlance_name
        ).select_from(genome).filter_by(
            genome_uuid=genome_uuid
        ).join(organism)

        species_results = session.execute(genome_select).all()
        if len(species_results) == 1:
            species_data = dict(species_results[0])
            species_data['genome_uuid'] = genome_uuid
            td = db.MetaData()
            with Session(metadata_db, future=True) as session_tax:
                tax_name = db.Table('ncbi_taxa_name', td, autoload_with=taxonomy_db)
                tax_names = db.select([tax_name.c.name, tax_name.c.name_class]).where(
                    tax_name.c.taxon_id == species_data['taxonomy_id'])
                taxo_results = session_tax.execute(tax_names).all()
                common_names = []
                # Get the common name and alternative names
                species_data['ncbi_common_name'] = None
                if len(taxo_results) > 0:
                    for item in taxo_results:
                        if item[1] is not None and item[0] is not None:
                            if item[1] == 'genbank common name':
                                species_data['ncbi_common_name'] = item[0]
                            if item[1] == 'common name':
                                common_names.append(item[1])
                    species_data['alternative_names'] = common_names
                    if len(common_names) > 0:
                        species_data['common_name'] = common_names[0]
                    else:
                        species_data['common_name'] = None
                return create_species(species_data)
        else:
            return create_species()


def get_sub_species_info(metadata_db, organism_id):
    if organism_id is None:
        return create_sub_species()

    md = db.MetaData()
    with Session(metadata_db, future=True) as session:

        # Reflect existing tables, letting sqlalchemy load linked tables where possible.
        organism_group_member = db.Table('organism_group_member', md, autoload_with=metadata_db)
        organism_group = db.Table('organism_group', md, autoload_with=metadata_db)

        sub_species_select = db.select(
            organism_group.c.type,
            organism_group.c.name,
        ).select_from(organism_group_member).filter_by(
            organism_id=organism_id
        ).join(organism_group)

        sub_species_results = session.execute(sub_species_select).fetchall()
        species_name = []
        species_type = []
        if len(sub_species_results) > 0:
            for key, value in sub_species_results:
                species_type.append(key)
                species_name.append(value)

            return create_sub_species({
                'organism_id': organism_id,
                'species_type': species_type,
                'species_name': species_name
            })
        else:
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

    sqlalchemy_md = db.MetaData()
    with Session(metadata_db, future=True) as session:
        # Reflect existing tables, letting sqlalchemy load linked tables where possible.
        genome = db.Table('genome', sqlalchemy_md, autoload_with=metadata_db)
        assembly = sqlalchemy_md.tables['assembly']
        organism = sqlalchemy_md.tables['organism']

        genome_uuid_query = get_genome_uuid_query(genome, assembly, organism).select_from(genome) \
            .join(assembly).join(organism) \
            .where(organism.c.ensembl_name == ensembl_name) \
            .where(assembly.c.name == assembly_name)

        genome_uuid_result = session.execute(genome_uuid_query).all()

        if len(genome_uuid_result) == 1:
            return create_genome_uuid(genome_uuid_result[0])

        return create_genome_uuid()


def get_genome_by_uuid(metadata_db, genome_uuid, release_version):
    if genome_uuid is None:
        return create_genome()

    sqlalchemy_md = db.MetaData()
    with Session(metadata_db, future=True) as session:

        # Reflect existing tables, letting sqlalchemy load linked tables where possible.
        genome = db.Table('genome', sqlalchemy_md, autoload_with=metadata_db)
        genome_release = db.Table('genome_release', sqlalchemy_md, autoload_with=metadata_db)
        release = sqlalchemy_md.tables['ensembl_release']
        assembly = sqlalchemy_md.tables['assembly']
        organism = sqlalchemy_md.tables['organism']

        genome_query = get_genome_query(genome, genome_release, release, assembly, organism).select_from(genome) \
            .join(assembly).join(organism).outerjoin(genome_release).outerjoin(release) \
            .where(genome.c.genome_uuid == genome_uuid)

        if release_version == 0:
            genome_query = genome_query.where(release.c.is_current == 1)
        else:
            genome_query = genome_query.where(release.c.version == release_version)
        genome_results = session.execute(genome_query).all()
        if len(genome_results) == 1:
            return create_genome(genome_results[0])
        return create_genome()


def get_genomes_by_keyword_iterator(metadata_db, keyword, release_version):
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

    # This is sqlalchemy's metadata, not Ensembl's!
    md = db.MetaData()
    with Session(metadata_db, future=True) as session:

        # Reflect existing tables, letting sqlalchemy load linked tables where possible.
        genome = db.Table('genome', md, autoload_with=metadata_db)
        genome_release = db.Table('genome_release', md, autoload_with=metadata_db)
        assembly = md.tables['assembly']
        organism = md.tables['organism']
        release = md.tables['ensembl_release']
        site = md.tables['ensembl_site']

        genome_select = get_genome_query(genome, genome_release, release, assembly, organism).select_from(genome).join(
            assembly).join(organism).filter_by(ensembl_name=ensembl_name)

        if release_version == 0:
            genome_select = genome_select.join(genome_release).join(release).filter_by(
                is_current=True)
        else:
            genome_select = genome_select.join(genome_release).join(release).filter_by(
                version=release_version)

        genome_select = genome_select.join(site).filter_by(name=site_name).distinct()
        print(genome_select)
        genome_results = session.execute(genome_select).all()

        if len(genome_results) == 1:
            return create_genome(genome_results[0])
        else:
            return create_genome()


def populate_dataset_info(data):
    return ensembl_metadata_pb2.DatasetInfos.DatasetInfo(
        dataset_uuid=data["dataset_uuid"],
        dataset_name=data["dataset_name"],
        dataset_version=data["dataset_version"],
        dataset_label=data["dataset_label"],
        version=int(data["version"]),
    )


def get_datasets_list_by_uuid(metadata_db, genome_uuid, release_version=0):
    # This is sqlalchemy's metadata, not Ensembl's!
    md = db.MetaData()
    with Session(metadata_db, future=True) as session:
        # Reflect existing tables, letting sqlalchemy load linked tables where possible.
        genome = db.Table('genome', md, autoload_with=metadata_db)
        dataset = db.Table('dataset', md, autoload_with=metadata_db)
        genome_dataset = db.Table('genome_dataset', md, autoload_with=metadata_db)
        dataset_type = db.Table('dataset_type', md, autoload_with=metadata_db)
        ensembl_release = db.Table('ensembl_release', md, autoload_with=metadata_db)

        datasets_select = db.select(
            genome.c.genome_uuid,
            dataset.c.dataset_uuid,
            # The label here should be identical to what's in proto file
            dataset_type.c.name.label('data_set_type'),
            dataset.c.name.label('dataset_name'),
            dataset.c.version.label('dataset_version'),
            dataset.c.label.label('dataset_label'),
            ensembl_release.c.version
        ).select_from(genome).join(genome_dataset).join(dataset) \
            .join(dataset_type).join(ensembl_release, isouter=True) \
            .where(genome.c.genome_uuid == genome_uuid) \
            .where(genome_dataset.c.is_current == 1) \
            .distinct()

        if release_version > 0:
            datasets_select = datasets_select.filter_by(version=release_version)
            # TODO: Marc comment: Might be interesting to do a <=, because I am still not sure whether
            # we'll replicate version attachment for everything every time we do a new release.
        else:
            # if the release is not specified, we return the latest by default
            datasets_select = datasets_select.filter_by(is_current=1)

        # print("SQL QUERY ===> ", str(datasets_select))
        datasets_results = session.execute(datasets_select).all()
        # print("len of datasets_results ===> ", len(datasets_results))

        if len(datasets_results) > 0:
            # ds_obj_dict where all datasets are stored as:
            # { dataset_type_1: [datasets_dt1_1, datasets_dt1_2], dataset_type_2: [datasets_dt2_1] }
            ds_obj_dict = {}
            for result in datasets_results:
                dataset_type = result['data_set_type']
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

        else:
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

    # This is sqlalchemy's metadata, not Ensembl's!
    md = db.MetaData()
    with Session(metadata_db, future=True) as session:
        # Reflect existing tables, letting sqlalchemy load linked tables where possible.
        genome = db.Table('genome', md, autoload_with=metadata_db)
        assembly = md.tables['assembly']
        assembly_sequence = db.Table('assembly_sequence', md, autoload_with=metadata_db)

        seq_select = (
            db.select(
                assembly_sequence.c.accession,
                assembly_sequence.c.name,
                assembly_sequence.c.sequence_location,
                assembly_sequence.c.length,
                assembly_sequence.c.chromosomal,
            )
            .select_from(genome)
            .filter_by(genome_uuid=genome_uuid)
            .join(assembly)
            .join(assembly_sequence)
        )
        if chromosomal_only == 1:
            seq_select = seq_select.filter_by(chromosomal=True)

        for result in session.execute(seq_select):
            yield create_genome_sequence(dict(result))


def release_iterator(metadata_db, site_name, release_version, current_only):
    # This is sqlalchemy's metadata, not Ensembl's!
    md = db.MetaData()
    with Session(metadata_db, future=True) as session:
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
    with Session(metadata_db, future=True) as session:

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


def get_dataset_by_genome_id(metadata_db, genome_uuid, requested_dataset_type):
    # This is sqlalchemy's metadata, not Ensembl's!
    md = db.MetaData()
    with Session(metadata_db, future=True) as session:

        # Reflect existing tables, letting sqlalchemy load linked tables where possible.
        genome = db.Table('genome', md, autoload_with=metadata_db)
        genome_dataset = db.Table('genome_dataset', md, autoload_with=metadata_db)
        dataset = db.Table('dataset', md, autoload_with=metadata_db)
        dataset_type = db.Table('dataset_type', md, autoload_with=metadata_db)
        attribute = db.Table('attribute', md, autoload_with=metadata_db)
        dataset_attribute = db.Table('dataset_attribute', md, autoload_with=metadata_db)
        ensembl_release = db.Table('ensembl_release', md, autoload_with=metadata_db)

        dataset_select = db.select(
            dataset.c.dataset_uuid,
            dataset.c.name.label('dataset_name'),
            attribute.c.name,
            attribute.c.type,
            dataset.c.version.label('dataset_version'),
            dataset.c.label.label('dataset_label'),
            ensembl_release.c.version,
            dataset_attribute.c.value
        ) \
            .select_from(genome).filter_by(genome_uuid=genome_uuid) \
            .join(genome_dataset) \
            .join(ensembl_release) \
            .join(dataset) \
            .join(dataset_attribute) \
            .join(attribute) \
            .join(dataset_type) \
            .where(dataset_type.c.name == requested_dataset_type) \
            .order_by(dataset.c.name, attribute.c.name) \
            .distinct()

        dataset_results = session.execute(dataset_select).all()
        return create_dataset_infos(genome_uuid, requested_dataset_type, dataset_results)


def create_species(data=None):
    if data is None:
        return ensembl_metadata_pb2.Species()
    species = ensembl_metadata_pb2.Species(
        genome_uuid=data["genome_uuid"],
        common_name=data["common_name"],
        ncbi_common_name=data["ncbi_common_name"],
        scientific_name=data["scientific_name"],
        alternative_names=data["alternative_names"],
        taxon_id=data["taxonomy_id"],
        scientific_parlance_name=data["scientific_parlance_name"],
    )
    return species
    # return json_format.MessageToJson(species)


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
        genome_uuid=data["genome_uuid"],
        code=data["code"],
        chromosomal=data["chromosomal"],
        location=data["location"],
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
        organism_id=data["organism_id"],
        species_name=data["species_name"],
        species_type=data["species_type"],
    )
    return sub_species


def create_assembly(data=None):
    if data is None:
        return ensembl_metadata_pb2.AssemblyInfo()
    assembly = ensembl_metadata_pb2.AssemblyInfo(
        assembly_uuid=data["assembly_uuid"],
        accession=data["accession"],
        level=data["level"],
        name=data["name"],
        chromosomal=data["chromosomal"],
        length=data["length"],
        sequence_location=data["sequence_location"],
        sequence_checksum=data["sequence_checksum"],
        ga4gh_identifier=data["ga4gh_identifier"],
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


def create_datasets(data=None):
    if data is None:
        return ensembl_metadata_pb2.Datasets()

    return ensembl_metadata_pb2.Datasets(
        genome_uuid=data["genome_uuid"], datasets=data["datasets"]
    )


def create_dataset_info(data=None):
    if data is None:
        return ensembl_metadata_pb2.DatasetInfos.DatasetInfo()
    return ensembl_metadata_pb2.DatasetInfos.DatasetInfo(**dict(data))


def create_dataset_infos(genome_uuid, requested_dataset_type, data=None):
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
        return get_assembly_information(self.db, request.assembly_id)

    def GetGenomesByAssemblyAccessionID(self, request, context):
        return get_genomes_from_assembly_accession_iterator(
            self.db, request.assembly_accession
        )

    def GetSubSpeciesInformation(self, request, context):
        return get_sub_species_info(self.db, request.organism_id)

    def GetGroupingInformation(self, request, context):
        return get_sub_species_info(self.db, request.organism_id)

    def GetKaryotypeInformation(self, request, context):
        return get_sub_species_info(self.db, request.genome_uuid)

    def GetTopLevelStatistics(self, request, context):
        return get_top_level_statistics(self.db, request.organism_id)

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
