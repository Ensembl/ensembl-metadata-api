# See the NOTICE file distributed with this work for additional information
#   regarding copyright ownership.
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#       http://www.apache.org/licenses/LICENSE-2.0
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.`
import logging
import re
import uuid
from collections import defaultdict

import sqlalchemy as db
import sqlalchemy.exc
from ensembl.core.models import Meta, CoordSystem, SeqRegionAttrib, SeqRegion, \
    SeqRegionSynonym, AttribType
from ensembl.ncbi_taxonomy.api.utils import Taxonomy
from sqlalchemy import or_, func
from sqlalchemy import select, and_
from sqlalchemy.exc import NoResultFound
from sqlalchemy.orm import aliased

from ensembl.production.metadata.api import exceptions
from ensembl.production.metadata.api.factories.datasets import DatasetFactory
from ensembl.production.metadata.api.models import *
from ensembl.production.metadata.updater.base import BaseMetaUpdater
from ensembl.production.metadata.updater.updater_utils import update_attributes

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)


class CoreMetaUpdater(BaseMetaUpdater):
    def __init__(self, db_uri, metadata_uri, release=None, force=None):
        super().__init__(db_uri, metadata_uri, release, force)
        self.db_type = 'core'
        # Single query to get all of the metadata information.
        self.meta_dict = {}
        with self.db.session_scope() as session:
            results = session.query(Meta).all()
            for result in results:
                species_id = result.species_id
                meta_key = result.meta_key
                meta_value = result.meta_value

                if species_id not in self.meta_dict:
                    self.meta_dict[species_id] = {}
                # WARNING! Duplicated meta_keys for a species_id will not error out!. A datacheck is necessary for key values.
                self.meta_dict[species_id][meta_key] = meta_value

    # Basic API for the meta table in the submission database.
    def get_meta_single_meta_key(self, species_id, parameter):
        species_meta = self.meta_dict.get(species_id)
        if species_meta is None:
            return None
        return species_meta.get(parameter)

    def get_meta_list_from_prefix_meta_key(self, species_id, prefix):
        species_meta = self.meta_dict.get(species_id)
        if species_meta is None:
            return None
        result_dict = {k: v for k, v in species_meta.items() if k.startswith(prefix)}
        return result_dict

    def process_core(self, **kwargs):
        # Special case for loading a single species from a collection database. Can be removed in a future release
        sel_species = kwargs.get('species', None)
        if sel_species:
            with self.db.session_scope() as session:
                multi_species = session.execute(
                    select(Meta.species_id).filter(Meta.meta_key == "species.production_name").filter(
                        Meta.meta_value == sel_species).distinct()
                )
        else:
            # Normal handling of collections from here
            # Handle multi-species databases and run an update for each species
            with self.db.session_scope() as session:
                multi_species = session.execute(
                    select(Meta.species_id).filter(Meta.meta_key == "species.production_name").distinct()
                )
        multi_species = [multi_species for multi_species, in multi_species]

        for species in multi_species:
            self.process_species(species)

    def process_species(self, species_id):
        """
        Process an individual species from a core database to update the metadata db.
        This method contains the logic for updating the metadata
        """

        with self.metadata_db.session_scope() as meta_session:
            organism, division, organism_group_member = self.get_or_new_organism(species_id, meta_session)
            assembly, assembly_dataset, assembly_dataset_attributes, assembly_sequences, dataset_source = self.get_or_new_assembly(
                species_id, meta_session)
            genebuild_dataset, genebuild_dataset_attributes = self.get_or_new_genebuild(species_id, meta_session,
                                                                                        dataset_source)

            # Checking for an existing genome uuid:
            old_genome_uuid = self.get_meta_single_meta_key(species_id, "genome.genome_uuid")
            if old_genome_uuid is not None:
                old_genome = meta_session.query(Genome).filter(
                    Genome.genome_uuid == old_genome_uuid).one_or_none()
                # Logic for existing key in database.
                if old_genome is not None:
                    if self.force is False:
                        raise exceptions.MetadataUpdateException(
                            "Core database contains a genome.genome_uuid which matches an entry in the meta table. "
                            "The force flag was not specified so the core was not updated.")
                    elif self.is_object_new(organism) or self.is_object_new(assembly):
                        raise exceptions.ExistingGenomeIdCoreException(
                            f"Core contains a genome.genome_uuid {old_genome_uuid} which matches an existing entry. "
                            "The assembly data or organism data is new and requires the creation a new uuid. Delete "
                            "the old uuid from the core to continue")
                else:
                    raise exceptions.MetadataUpdateException(
                        "Database contains a Genome.genome_uuid, but the corresponding data is not in"
                        "the meta table. Please remove it from the meta key and resubmit")

            if self.is_object_new(organism):
                logger.info('New organism')
                # ###############################Checks that dataset is new ##################
                if not self.is_object_new(genebuild_dataset):
                    raise exceptions.MetadataUpdateException(
                        "New organism, but existing assembly accession and/or genebuild version")
                ###############################################
                # Create genome and populate the database with organism, assembly and dataset
                new_genome, assembly_genome_dataset, genebuild_genome_dataset = self.new_genome(meta_session,
                                                                                                species_id,
                                                                                                organism,
                                                                                                assembly,
                                                                                                assembly_dataset,
                                                                                                genebuild_dataset)
                self.concurrent_commit_genome_uuid(meta_session, species_id, new_genome.genome_uuid)

            elif self.is_object_new(assembly):
                logger.info('New assembly')

                # ###############################Checks that dataset and update are new ##################
                if not self.is_object_new(genebuild_dataset):
                    raise exceptions.MetadataUpdateException("New assembly, but existing genebuild version")
                ###############################################

                new_genome, assembly_genome_dataset, genebuild_genome_dataset = self.new_genome(meta_session,
                                                                                                species_id,
                                                                                                organism,
                                                                                                assembly,
                                                                                                assembly_dataset,
                                                                                                genebuild_dataset)
                self.concurrent_commit_genome_uuid(meta_session, species_id, new_genome.genome_uuid)

                # Create genome and populate the database with assembly and dataset
            elif self.is_object_new(genebuild_dataset):
                # Check that genest update or provider name has changed from last time.

                dataset_attr_alias1 = aliased(DatasetAttribute)
                attribute_alias1 = aliased(Attribute)
                dataset_attr_alias2 = aliased(DatasetAttribute)
                attribute_alias2 = aliased(Attribute)
                provider_name = self.get_meta_single_meta_key(species_id, "genebuild.provider_name")
                geneset_update = self.get_meta_single_meta_key(species_id, "genebuild.last_geneset_update")
                query = meta_session.query(Assembly).join(
                    Genome, Assembly.genomes
                ).join(GenomeDataset, Genome.genome_datasets
                       ).join(Dataset, GenomeDataset.dataset
                              ).join(dataset_attr_alias1, Dataset.dataset_attributes
                                     ).join(attribute_alias1, dataset_attr_alias1.attribute
                                            ).join(dataset_attr_alias2, Dataset.dataset_attributes
                                                   ).join(attribute_alias2, dataset_attr_alias2.attribute
                                                          ).filter(Assembly.accession == assembly.accession,
                                                                   Dataset.dataset_type.has(name="genebuild"),
                                                                   and_(
                                                                       attribute_alias1.name == "genebuild.provider_name",
                                                                       dataset_attr_alias1.value == provider_name,
                                                                       attribute_alias2.name == "genebuild.last_geneset_update",
                                                                       dataset_attr_alias2.value == geneset_update
                                                                   )
                                                                   )
                if meta_session.query(query.exists()).scalar():
                    raise exceptions.MetadataUpdateException(
                        "genebuild.provider_name or genebuild.last_geneset_update must be updated.")

                logger.info('New genebuild')
                # Create genome and populate the database with genebuild dataset
                new_genome, assembly_genome_dataset, genebuild_genome_dataset = self.new_genome(meta_session,
                                                                                                species_id,
                                                                                                organism,
                                                                                                assembly,
                                                                                                assembly_dataset,
                                                                                                genebuild_dataset)
                self.concurrent_commit_genome_uuid(meta_session, species_id, new_genome.genome_uuid)

            else:
                # Check if the data has been released:
                if check_release_status(self.metadata_db, genebuild_dataset.dataset_uuid) and not self.force:
                    raise exceptions.WrongReleaseException(
                        "Existing Organism, Assembly, and Datasets within a release. "
                        "To update released data set force=True. This will force assembly "
                        "and genebuild"
                        "dataset updates and assembly sequences.")
                else:
                    logger.info('Rewrite of existing datasets. Only assembly dataset attributes, genebuild '
                                'dataset, dataset attributes, and assembly sequences are modified.')
                    # TODO: We need to review this process, because if some Variation / Regulation / Compara datasets
                    #  exists we'll expect either to refuse the updates - imagine this was a fix in sequences! OR we
                    #  decide to delete the other datasets to force their recompute. In this case, we want to rewrite
                    #  the existing datasets with new data, but keep the dataset_uuid Update genebuild_dataset
                    meta_session.query(DatasetAttribute).filter(
                        DatasetAttribute.dataset_id == genebuild_dataset.dataset_id).delete()
                    self.get_or_new_genebuild(species_id,
                                              meta_session,
                                              source=dataset_source,
                                              existing=genebuild_dataset)

                    # #Update assembly_dataset
                    meta_session.query(DatasetAttribute).filter(
                        DatasetAttribute.dataset_id == assembly_dataset.dataset_id).delete()
                    self.get_or_new_assembly(
                        species_id, meta_session, source=dataset_source, existing=assembly_dataset)

    def concurrent_commit_genome_uuid(self, meta_session, species_id, genome_uuid):
        # Currently impossible with myisam without two phase commit (requires full refactor)
        # This is a workaround and should be sufficient.
        with self.db.session_scope() as session:
            meta_session.commit()
            try:
                existing_row = session.query(Meta).filter(
                    and_(
                        Meta.species_id == species_id,
                        Meta.meta_key == 'genome.genome_uuid',
                    )
                ).first()

                if existing_row:
                    session.delete(existing_row)
                new_row = Meta(
                    species_id=species_id,
                    meta_key='genome.genome_uuid',
                    meta_value=genome_uuid
                )
                session.add(new_row)
                session.commit()
            except sqlalchemy.exc.DatabaseError as e:
                raise exceptions.UpdateBackCoreException(
                    f"Metadata-api failed to insert {genome_uuid} into {self.db_uri} "
                    f"but it successfully updated the metadata. ")

    def new_genome(self, meta_session, species_id, organism, assembly, assembly_dataset, genebuild_dataset):
        production_name = self.get_meta_single_meta_key(species_id, "species.production_name")
        genebuild_version = self.get_meta_single_meta_key(species_id, "genebuild.version")
        genebuild_date = self.get_meta_single_meta_key(species_id, "genebuild.last_geneset_update")
        if genebuild_date is None:
            start_date_str = self.get_meta_single_meta_key(species_id, "genebuild.start_date")
            match = re.search(r'^(\d{4}-\d{2})', start_date_str)
            if match:
                genebuild_date = match.group(0)
            else:
                raise exceptions.MetadataUpdateException(f"Unable to parse genebuild.start_date from meta")
        # get next release inline to attach the genome to
        planned_release = get_or_new_release(self.metadata_uri)
        new_genome = Genome(
            genome_uuid=str(uuid.uuid4()),
            assembly=assembly,
            organism=organism,
            genebuild_date=genebuild_date,
            genebuild_version=genebuild_version,
            created=func.now(),
            is_best=0,
            production_name=production_name,
        )
        logger.debug(f"Assigning genome {new_genome.genome_uuid} to {planned_release.version}")
        meta_session.add(new_genome)
        genome_release = GenomeRelease(
            genome=new_genome,
            ensembl_release=planned_release
        )
        new_genome.genome_releases.append(genome_release)
        assembly_genome_dataset = GenomeDataset(
            genome=new_genome,
            dataset=assembly_dataset,
            is_current=True,
        )
        meta_session.add(assembly_genome_dataset)
        genebuild_genome_dataset = GenomeDataset(
            genome=new_genome,
            dataset=genebuild_dataset,
            is_current=True,
        )
        meta_session.add(genebuild_genome_dataset)
        # Homology dataset creation
        homology_uuid, homology_dataset, homology_dataset_attributes, homology_genome_dataset = self.new_homology(
            meta_session, species_id, genome=new_genome)
        meta_session.add(homology_genome_dataset)

        # Create children datasets here!
        meta_session.commit()
        dataset_factory = DatasetFactory(self.metadata_uri)
        dataset_factory.create_all_child_datasets(genebuild_dataset.dataset_uuid, meta_session)
        dataset_factory.create_all_child_datasets(homology_dataset.dataset_uuid, meta_session)

        return new_genome, assembly_genome_dataset, genebuild_genome_dataset

    def get_or_new_organism(self, species_id, meta_session):
        """
        Get an existing Organism instance or create a new one, depending on the information from the metadata database.
        """
        # Fetch the Ensembl name of the organism from metadata using either 'species.biosample_id'
        # or 'species.production_name' as the key.
        biosample_id = self.get_meta_single_meta_key(species_id, "organism.biosample_id")
        if biosample_id is None:
            biosample_id = self.get_meta_single_meta_key(species_id, "species.production_name")

        # Getting the common name from the meta table, otherwise we grab it from ncbi.
        common_name = self.get_meta_single_meta_key(species_id, "species.common_name")
        taxid = self.get_meta_single_meta_key(species_id, "species.taxonomy_id")
        if common_name is None or common_name == "":

            with self.metadata_db.session_scope() as session:
                common_name = session.query(NCBITaxaName).filter(
                    NCBITaxaName.taxon_id == taxid,
                    NCBITaxaName.name_class == "genbank common name"
                ).one_or_none()
                common_name = common_name.name if common_name is not None else '-'
        # Ensure that the first character is upper case.
        common_name = common_name[0].upper() + common_name[1:]
        species_taxonomy_id = self.get_meta_single_meta_key(species_id, "species.species_taxonomy_id")
        if species_taxonomy_id is None:
            species_taxonomy_id = taxid
        # Instantiate a new Organism object using data fetched from metadata.
        new_organism = Organism(
            species_taxonomy_id=species_taxonomy_id,
            taxonomy_id=self.get_meta_single_meta_key(species_id, "species.taxonomy_id"),
            common_name=common_name,
            scientific_name=self.get_meta_single_meta_key(species_id, "species.scientific_name"),
            biosample_id=biosample_id,
            strain=self.get_meta_single_meta_key(species_id, "species.strain"),
            strain_type=self.get_meta_single_meta_key(species_id, "strain.type"),
            scientific_parlance_name=self.get_meta_single_meta_key(species_id, "species.parlance_name")
        )

        # Query the metadata database to find if an Organism with the same Ensembl name already exists.
        old_organism = meta_session.query(Organism).filter(
            Organism.biosample_id == new_organism.biosample_id).one_or_none()
        division_name = self.get_meta_single_meta_key(species_id, "species.division")
        division = meta_session.query(OrganismGroup).filter(OrganismGroup.name == division_name).one_or_none()

        # If an existing Organism is found, return it and indicate that it already existed.
        if old_organism:
            organism_group_member = meta_session.query(OrganismGroupMember).filter(
                OrganismGroupMember.organism_id == old_organism.organism_id,
                OrganismGroupMember.organism_group_id == division.organism_group_id).one_or_none()

            return old_organism, division, organism_group_member
        else:
            # If no existing Organism is found, conduct additional checks before creating a new one.

            # Check if the new organism's taxonomy ID exists in the taxonomy database.
            with self.metadata_db.session_scope() as session:
                try:
                    Taxonomy.fetch_node_by_id(session, new_organism.taxonomy_id)
                except NoResultFound:
                    raise exceptions.TaxonNotFoundException(
                        f"taxon id {new_organism.taxonomy_id} not found in taxonomy database for scientific name")

            # Check if an Assembly with the same accession already exists in the metadata database.
            accession = self.get_meta_single_meta_key(species_id, "assembly.accession")
            assembly_test = meta_session.query(Assembly).filter(Assembly.accession == accession).one_or_none()
            if assembly_test is not None:
                logger.info("Assembly Accession already exists for a different organism.")

            # Fetch the division name of the new organism from metadata.
            if division_name is None:
                exceptions.MissingMetaException("No species.division found in meta table")

            # Query the metadata database to find if an OrganismGroup with the same division name already exists.
            if division is None:
                # If no such OrganismGroup exists, create a new one.
                division = OrganismGroup(
                    type="Division",
                    name=division_name,
                )
                meta_session.add(division)

            # Create a new OrganismGroupMember linking the new Organism to the division group.
            organism_group_member = OrganismGroupMember(
                is_reference=0,
                organism=new_organism,
                organism_group=division,
            )
            meta_session.add(new_organism)
            meta_session.add(organism_group_member)
            # Return the newly created Organism and indicate that it is new.
            return new_organism, division, organism_group_member

    def get_assembly_sequences(self, species_id, assembly):
        """
        Get the assembly sequences and the values that correspond to the metadata table
        """
        assembly_sequences = []
        with self.db.session_scope() as session:
            circular_seq_attrib = aliased(SeqRegionAttrib)
            results = (session.query(SeqRegion.name, SeqRegion.length, CoordSystem.name.label("coord_system_name"),
                                     SeqRegionSynonym.synonym, circular_seq_attrib.value.label("is_circular"))
                       .outerjoin(SeqRegion.coord_system)
                       .outerjoin(SeqRegionSynonym, SeqRegionSynonym.seq_region_id == SeqRegion.seq_region_id)
                       .join(SeqRegion.seq_region_attrib)  # For other attributes
                       .outerjoin(circular_seq_attrib,
                                  and_(circular_seq_attrib.seq_region_id == SeqRegion.seq_region_id,
                                       circular_seq_attrib.attrib_type.has(code="circular_seq")))
                       .join(SeqRegionAttrib.attrib_type)
                       .filter(CoordSystem.species_id == species_id)
                       .filter(AttribType.code == "toplevel")
                       .filter(CoordSystem.name != "lrg")
                       .all())
            attributes = (session.query(SeqRegion.name, AttribType.code, SeqRegionAttrib.value)
                          .select_from(SeqRegion)
                          .join(SeqRegionAttrib)
                          .join(AttribType)
                          .filter(or_(AttribType.code == "sequence_location",
                                      AttribType.code == "karyotype_rank")).all())
            attribute_dict = {}
            for name, code, value in attributes:
                if name not in attribute_dict:
                    attribute_dict[name] = {}
                attribute_dict[name][code] = value

            accession_info = defaultdict(
                # The None's here are improper, but they break far too much for this update if they are changed.
                # When accession is decided I will fix them.
                lambda: {
                    "names": set(), "accession": None, "length": None, "location": None, "chromosomal": None,
                    "karyotype_rank": None
                })

            for seq_region_name, seq_region_length, coord_system_name, synonym, is_circular in results:
                accession_info[seq_region_name]["names"].add(seq_region_name)
                if synonym:
                    accession_info[seq_region_name]["names"].add(synonym)

                # Save the sequence location, length, and chromosomal flag.
                location_mapping = {
                    'nuclear_chromosome': 'SO:0000738',
                    'mitochondrial_chromosome': 'SO:0000737',
                    'chloroplast_chromosome': 'SO:0000745',
                    None: 'SO:0000738',
                }
                # Try to get the sequence location
                location = attribute_dict.get(seq_region_name, {}).get("sequence_location", None)

                # Using the retrieved location to get the sequence location
                sequence_location = location_mapping[location]

                # Try to get the karyotype rank
                karyotype_rank = attribute_dict.get(seq_region_name, {}).get("karyotype_rank", None)

                # Test if chromosomal:
                if karyotype_rank is not None:
                    chromosomal = 1
                else:
                    chromosomal = 1 if coord_system_name == "chromosome" else 0

                # Assign the values to the dictionary
                if not accession_info[seq_region_name]["length"]:
                    accession_info[seq_region_name]["length"] = seq_region_length

                if not accession_info[seq_region_name]["location"]:
                    accession_info[seq_region_name]["location"] = sequence_location

                if accession_info[seq_region_name]["chromosomal"] is None:  # Assuming default is None
                    accession_info[seq_region_name]["chromosomal"] = chromosomal

                if not accession_info[seq_region_name]["karyotype_rank"]:
                    accession_info[seq_region_name]["karyotype_rank"] = karyotype_rank

                accession_info[seq_region_name]["type"] = coord_system_name
                accession_info[seq_region_name]["is_circular"] = 1 if is_circular == "1" else 0

            for accession, info in accession_info.items():
                seq_region_name = accession
                assembly_sequence = AssemblySequence(
                    name=seq_region_name,
                    assembly=assembly,
                    accession=accession,
                    chromosomal=info["chromosomal"],
                    length=info["length"],
                    sequence_location=info["location"],
                    chromosome_rank=info["karyotype_rank"],
                    # md5="", Populated after checksums are ran.
                    # sha512t4u="", Populated after checksums are ran.
                    type=info["type"],
                    is_circular=info["is_circular"]
                )

                assembly_sequences.append(assembly_sequence)
        return assembly_sequences

    def get_or_new_assembly(self, species_id, meta_session, source=None, existing=None):
        # Get the new assembly accession  from the core handed over
        assembly_accession = self.get_meta_single_meta_key(species_id, "assembly.accession")
        assembly = meta_session.query(Assembly).filter(Assembly.accession == assembly_accession).one_or_none()
        if source is None:
            dataset_source = self.get_or_new_source(meta_session, "core")
        else:
            dataset_source = source

        # This should return the existing objects
        if assembly is not None and existing is None:
            # Get the existing assembly dataset
            assembly_dataset = meta_session.query(Dataset).filter(Dataset.label == assembly_accession).one_or_none()
            # I should not need this, but double check on database updating.
            assembly_dataset_attributes = assembly_dataset.dataset_attributes
            assembly_sequences = assembly.assembly_sequences
            return assembly, assembly_dataset, assembly_dataset_attributes, assembly_sequences, dataset_source

        else:

            attributes = self.get_meta_list_from_prefix_meta_key(species_id, "assembly")

            if existing is None:
                is_reference = 1 if self.get_meta_single_meta_key(species_id, "assembly.is_reference") else 0
                with self.db.session_scope() as session:
                    level = (session.execute(db.select(CoordSystem.name).filter(
                        CoordSystem.species_id == species_id).order_by(CoordSystem.rank)).all())[0][0]
                    tol_id = self.get_meta_single_meta_key(species_id, "assembly.tol_id")
                assembly = Assembly(
                    ucsc_name=self.get_meta_single_meta_key(species_id, "assembly.ucsc_alias"),
                    accession=self.get_meta_single_meta_key(species_id, "assembly.accession"),
                    level=level,
                    name=self.get_meta_single_meta_key(species_id, "assembly.name"),
                    accession_body=self.get_meta_single_meta_key(species_id, "assembly.provider"),
                    assembly_default=self.get_meta_single_meta_key(species_id, "assembly.default"),
                    tol_id=tol_id,
                    alt_accession=self.get_meta_single_meta_key(species_id, "assembly.alt_accession"),
                    created=func.now(),
                    assembly_uuid=str(uuid.uuid4()),
                    url_name=self.get_meta_single_meta_key(species_id, "assembly.url_name"),
                    is_reference=is_reference
                )
                dataset_factory = DatasetFactory(self.metadata_uri)
                dataset_type = meta_session.query(DatasetType).filter(DatasetType.name == "assembly").first()
                (dataset_uuid, assembly_dataset, assembly_dataset_attributes,
                 new_genome_dataset) = dataset_factory.create_dataset(meta_session, None, dataset_source,
                                                                      dataset_type, attributes, "assembly",
                                                                      assembly.accession, None,
                                                                      DatasetStatus.PROCESSED)
                meta_session.add(assembly)
                meta_session.add(assembly_dataset)
                assembly_sequences = self.get_assembly_sequences(species_id, assembly)
                meta_session.add_all(assembly_sequences)
            else:
                assembly_dataset = existing
                assembly_dataset.dataset_source = dataset_source
                for dataset_attribute in assembly_dataset.dataset_attributes:
                    meta_session.delete(dataset_attribute)
                assembly_dataset_attributes = update_attributes(assembly_dataset, attributes, meta_session)
                assembly_sequences = meta_session.query(AssemblySequence).filter(
                    AssemblySequence.assembly_id == assembly.assembly_id)
            meta_session.add_all(assembly_dataset_attributes)
            return assembly, assembly_dataset, assembly_dataset_attributes, assembly_sequences, dataset_source

    def get_or_new_genebuild(self, species_id, meta_session, source=None, existing=False):
        """
        Process an individual species from a core database to update the metadata db.
        This method contains the logic for updating the metadata
        This is not a get, as we don't update the metadata for genebuild, only replace it if it is not released.
        """
        assembly_accession = self.get_meta_single_meta_key(species_id, "assembly.accession")
        genebuild_version = self.get_meta_single_meta_key(species_id, "genebuild.version")
        provider_name = self.get_meta_single_meta_key(species_id, "genebuild.provider_name")
        last_geneset_update = self.get_meta_single_meta_key(species_id, "genebuild.last_geneset_update")
        start_date = self.get_meta_single_meta_key(species_id, "genebuild.start_date")

        if None in (provider_name, last_geneset_update, start_date):
            exceptions.MissingMetaException(
                "genebuild.provider_name, genebuild.last_geneset_update, genebuild.start_date are required keys")
        # There should not exist an existing genome with assembly_accesion/provider_name/last_geneset_update and start_date.
        provider_name_attr = aliased(DatasetAttribute)
        last_geneset_update_attr = aliased(DatasetAttribute)
        start_date_attr = aliased(DatasetAttribute)

        existing_combination = (
            meta_session.query(Genome)
            .join(GenomeDataset, Genome.genome_id == GenomeDataset.genome_id)
            .join(Dataset, GenomeDataset.dataset_id == Dataset.dataset_id)
            .join(Assembly, Genome.assembly_id == Assembly.assembly_id)
            .join(provider_name_attr, Dataset.dataset_id == provider_name_attr.dataset_id)
            .join(last_geneset_update_attr, Dataset.dataset_id == last_geneset_update_attr.dataset_id)
            .join(start_date_attr, Dataset.dataset_id == start_date_attr.dataset_id)
            .filter(
                Dataset.name == "genebuild",
                Assembly.accession == assembly_accession,  # Correctly linking the assembly_accession
                provider_name_attr.value == provider_name,
                last_geneset_update_attr.value == last_geneset_update,
                start_date_attr.value == start_date,
                provider_name_attr.attribute.has(Attribute.name == "genebuild.provider_name"),
                last_geneset_update_attr.attribute.has(Attribute.name == "genebuild.last_geneset_update"),
                start_date_attr.attribute.has(Attribute.name == "genebuild.start_date")
            )
            .exists()
        )
        if meta_session.query(existing_combination).scalar():
            exceptions.MetaException(
                "genebuild.provider_name, genebuild.last_geneset_update, genebuild.start_date and assembly.accession can"
                " not match existing records. If this is an update, please update genebuild.last_geneset_update with the "
                "current date. "
            )


        # The genebuild accession is formed by combining the assembly accession and the genebuild version
        genebuild_accession = assembly_accession + "_" + genebuild_version
        if source is None:
            dataset_source = self.get_or_new_source(meta_session, "core")
        else:
            dataset_source = source

        dataset_type = meta_session.query(DatasetType).filter(DatasetType.name == "genebuild").first()
        test_status = meta_session.query(Dataset).filter(Dataset.label == genebuild_accession).one_or_none()

        # Return existing data if no update is required
        if test_status is not None and existing is False:
            genebuild_dataset = test_status
            genebuild_dataset_attributes = genebuild_dataset.dataset_attributes
            return genebuild_dataset, genebuild_dataset_attributes
        attributes = self.get_meta_list_from_prefix_meta_key(species_id, "genebuild.")
        if existing is False:
            dataset_factory = DatasetFactory(self.metadata_uri)
            (dataset_uuid, genebuild_dataset, genebuild_dataset_attributes,
             new_genome_dataset) = dataset_factory.create_dataset(meta_session, None, dataset_source,
                                                                  dataset_type, attributes, "genebuild",
                                                                  genebuild_accession, genebuild_version)
        else:
            genebuild_dataset = existing
            genebuild_dataset.label = genebuild_accession
            genebuild_dataset.dataset_source = dataset_source
            genebuild_dataset.version = genebuild_version
            for dataset_attribute in genebuild_dataset.dataset_attributes:
                meta_session.delete(dataset_attribute)
            genebuild_dataset_attributes = update_attributes(genebuild_dataset, attributes, meta_session)

        return genebuild_dataset, genebuild_dataset_attributes

    def new_homology(self, meta_session, species_id, genome=None, source=None, dataset_attributes=None, version="1.0"):
        if source is None:
            production_name = self.get_meta_single_meta_key(species_id, "species.production_name")
            db_version = self.get_meta_single_meta_key(None, "schema_version")
            compara_name = production_name + "_compara_" + db_version
            dataset_source = self.get_or_new_source(meta_session, "compara", name=compara_name)
        else:
            dataset_source = source
        dataset_type = meta_session.query(DatasetType).filter(DatasetType.name == "homologies").first()
        dataset_factory = DatasetFactory(self.metadata_uri)
        (dataset_uuid, homology_dataset, homology_dataset_attributes,
         homology_genome_dataset) = dataset_factory.create_dataset(meta_session, genome, dataset_source,
                                                                   dataset_type, dataset_attributes,
                                                                   "compara_homologies",
                                                                   "Compara homologies", version)
        return dataset_uuid, homology_dataset, homology_dataset_attributes, homology_genome_dataset
