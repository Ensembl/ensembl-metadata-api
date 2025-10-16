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
    def __init__(self, db_uri, metadata_uri, taxonomy_uri, release=None):
        super().__init__(db_uri, metadata_uri, taxonomy_uri, release)
        self.db_type = 'core'
        # Single query to get all of the metadata information.
        self.meta_dict = {}
        self._load_meta_dict()
        self._validate_required_attributes()

    def _load_meta_dict(self):
        """Load metadata into meta_dict from the database."""
        with self.db.session_scope() as session:
            results = session.query(Meta).filter(Meta.meta_value.isnot(None),
                                                 Meta.meta_value.notin_(['', 'Null', 'NULL'])).all()
            for result in results:
                species_id = result.species_id
                meta_key = result.meta_key
                meta_value = result.meta_value
                if species_id not in self.meta_dict:
                    self.meta_dict[species_id] = {}
                # WARNING! Duplicated meta_keys for a species_id will not error out!. A datacheck is necessary for key values.
                self.meta_dict[species_id][meta_key] = meta_value

    def _validate_required_attributes(self):
        """Check if all required attributes are present in the meta_dict for each species."""
        required_attribute_names = []
        with self.metadata_db.session_scope() as session:
            # Query the attribute table to get all required attributes
            required_attributes = session.query(Attribute.name).filter(Attribute.required == 1).all()
            required_attribute_names = {attr.name for attr in required_attributes}

        with self.db.session_scope() as session:
            # Check each species_id in meta_dict
            missing_attributes = {}
            for species_id, meta in self.meta_dict.items():
                missing = required_attribute_names - set(meta.keys())
                if missing:
                    missing_attributes[species_id] = missing

            if missing_attributes:
                exceptions.MissingMetaException(
                    "Species ID {species_id} is missing required attributes: {missing_attributes}")

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
        # Special case for loading a single species from a collection database. Use the production name as an argument.
        # Not implemented in handover.
        sel_species = kwargs.get('organism', None)
        if sel_species:
            with self.db.session_scope() as session:
                multi_species = session.execute(
                    select(Meta.species_id).filter(Meta.meta_key == "organism.production_name").filter(
                        Meta.meta_value == sel_species).distinct()
                )
        else:
            # Handling of collections from here
            with self.db.session_scope() as session:
                multi_species = session.execute(
                    select(Meta.species_id).filter(Meta.meta_key == "organism.production_name").distinct()
                )
        multi_species = [multi_species for multi_species, in multi_species]

        # Track results for each species
        successful_species = []
        failed_species = []
        already_loaded_species = []

        if len(multi_species) > 1:
            logger.info(f"Processing {len(multi_species)} species in collection database")

        for species_id in multi_species:
            production_name = self.get_meta_single_meta_key(species_id, "organism.production_name")
            if len(multi_species) > 1:
                logger.info(f"Processing species {species_id}: {production_name}")

            try:
                # Check if this species already has a genome_uuid
                existing_genome_uuid = self.get_meta_single_meta_key(species_id, "genome.genome_uuid")
                if existing_genome_uuid is not None:
                    logger.warning(
                        f"Species {species_id} ({production_name}) already has genome_uuid: {existing_genome_uuid}")
                    already_loaded_species.append((species_id, production_name))
                    continue

                # Process each species in its own transaction
                with self.metadata_db.session_scope() as meta_session:
                    self.process_species(species_id, meta_session)
                    # If we get here without exception, the species was successful
                    successful_species.append((species_id, production_name))
                    if len(multi_species) > 1:
                        logger.info(f"Successfully processed species {species_id}: {production_name}")

            except Exception as e:
                logger.error(f"Failed to process species {species_id} ({production_name}): {str(e)}")
                failed_species.append((species_id, production_name, str(e)))
                # Continue to next species rather than failing entirely

        # Log summary for multi-species databases
        if len(multi_species) > 1:
            self._log_processing_summary(successful_species, failed_species, already_loaded_species)

        # If any species failed or were already loaded, raise an exception to prevent ignoring any errors
        if failed_species or already_loaded_species:
            error_msg = f"""
            Handover Failed For Genomes With Error:
            {chr(10).join([f"- {fspecies[1]}: {fspecies[-1]}" for fspecies in failed_species])}

            Collection processing completed with issues:
            - {len(failed_species)} failed
            - {len(already_loaded_species)} already loaded
            """

            raise exceptions.MetadataUpdateException(error_msg)

    def _log_processing_summary(self, successful_species, failed_species, already_loaded_species):
        """Log a summary of collection processing results."""
        logger.info("=" * 80)
        logger.info("COLLECTION PROCESSING SUMMARY")
        logger.info("=" * 80)

        if successful_species:
            logger.info(f"SUCCESSFULLY PROCESSED ({len(successful_species)} species):")
            for species_id, production_name in successful_species:
                logger.info(f"  - {species_id}: {production_name}")

        if already_loaded_species:
            logger.warning(f"ALREADY LOADED ({len(already_loaded_species)} species):")
            for species_id, production_name in already_loaded_species:
                logger.warning(f"  - {species_id}: {production_name}")

        if failed_species:
            logger.error(f"FAILED TO PROCESS ({len(failed_species)} species):")
            for species_id, production_name, error in failed_species:
                logger.error(f"  - {species_id}: {production_name} - {error}")

        logger.info("=" * 80)

    def process_species(self, species_id, meta_session):
        """
        Process an individual species from a core database to update the metadata db.
        This method contains the logic for updating the metadata
        """
        organism = self.get_or_new_organism(species_id, meta_session)
        assembly, assembly_dataset, assembly_dataset_attributes, assembly_sequences, dataset_source = self.get_or_new_assembly(
            species_id, meta_session)
        genebuild_dataset, genebuild_dataset_attributes = self.get_or_new_genebuild(species_id, meta_session,
                                                                                    dataset_source)

        # Checking for an existing genome uuid:
        old_genome_uuid = self.get_meta_single_meta_key(species_id, "genome.genome_uuid")
        if old_genome_uuid is not None:
            old_genome = meta_session.query(Genome).filter(
                Genome.genome_uuid == old_genome_uuid).one_or_none()
            if old_genome is not None:
                raise exceptions.MetadataUpdateException(
                    f"Species {species_id}: Core database contains a genome.genome_uuid which matches an entry in the meta table.")
            else:
                raise exceptions.MetadataUpdateException(
                    f"Species {species_id}: Database contains a Genome.genome_uuid, but corresponding data is not in meta table.")

        if self.is_object_new(organism):
            logger.info(f'Species {species_id}: New organism')
            if not self.is_object_new(genebuild_dataset):
                raise exceptions.MetadataUpdateException(
                    f"Species {species_id}: New organism, but existing assembly accession and/or genebuild version")
            new_genome, assembly_genome_dataset, genebuild_genome_dataset = self.new_genome(meta_session,
                                                                                            species_id,
                                                                                            organism,
                                                                                            assembly,
                                                                                            assembly_dataset,
                                                                                            genebuild_dataset)
            self.concurrent_commit_genome_uuid(meta_session, species_id, new_genome.genome_uuid)



        elif self.is_object_new(assembly):
            logger.info(f'Species {species_id}: New assembly')
            if not self.is_object_new(genebuild_dataset):
                raise exceptions.MetadataUpdateException(
                    f"Species {species_id}: New assembly, but existing genebuild version")
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

            logger.info(f'Species {species_id}: New genebuild')
            new_genome, assembly_genome_dataset, genebuild_genome_dataset = self.new_genome(meta_session,
                                                                                            species_id,
                                                                                            organism,
                                                                                            assembly,
                                                                                            assembly_dataset,
                                                                                            genebuild_dataset)
            self.concurrent_commit_genome_uuid(meta_session, species_id, new_genome.genome_uuid)


        else:
            # Check if the data has been released
            if check_release_status(self.metadata_db, genebuild_dataset.dataset_uuid):
                raise exceptions.WrongReleaseException(
                    f"Species {species_id}: Existing Organism, Assembly, and Datasets within a release.")
            else:
                logger.info(f'Species {species_id}: Rewrite of existing datasets attempted')
                raise exceptions.MetadataUpdateException(
                    f"Species {species_id}: This looks like a reload of data that hasn't been released.")

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
        production_name = self.get_meta_single_meta_key(species_id, "organism.production_name")
        genebuild_version = self.get_meta_single_meta_key(species_id, "genebuild.version")
        genebuild_date = self.get_meta_single_meta_key(species_id, "genebuild.last_geneset_update")
        url_name = self.get_meta_single_meta_key(species_id, "assembly.url_name")
        annotation_source = self.get_meta_single_meta_key(species_id, "genebuild.annotation_source")
        if genebuild_date is None:  ##TODO Make this so any of the above are none it fails!
            raise exceptions.MetadataUpdateException(f"Unable to parse genebuild.last_geneset_update from meta")
        # get next release inline to attach the genome to
        planned_release = get_or_new_release(self.metadata_uri)
        new_genome = Genome(
            genome_uuid=str(uuid.uuid4()),
            assembly=assembly,
            organism=organism,
            genebuild_date=genebuild_date,
            genebuild_version=genebuild_version,
            created=func.now(),
            production_name=production_name,
            url_name=url_name,
            annotation_source=annotation_source
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
        # or 'organism.production_name' as the key.
        biosample_id = self.get_meta_single_meta_key(species_id, "organism.biosample_id")
        if biosample_id is None:
            biosample_id = self.get_meta_single_meta_key(species_id, "organism.production_name")

        # Getting the common name from the meta table, otherwise we grab it from ncbi.
        common_name = self.get_meta_single_meta_key(species_id, "organism.common_name")
        taxid = self.get_meta_single_meta_key(species_id, "organism.taxonomy_id")
        if taxid is None:
            raise exceptions.MissingMetaException("organism.taxid is required")
        if common_name is None:
            with self.taxonomy_db.session_scope() as session:
                common_name = session.query(NCBITaxaName).filter(
                    NCBITaxaName.taxon_id == taxid,
                    NCBITaxaName.name_class == "genbank common name"
                ).one_or_none()
                if common_name is not None:
                    common_name = common_name.name
        # Ensure that the first character is upper case.
        if common_name is not None:
            common_name = common_name[0].upper() + common_name[1:]
        species_taxonomy_id = self.get_meta_single_meta_key(species_id, "organism.species_taxonomy_id")
        if species_taxonomy_id is None:
            species_taxonomy_id = taxid
        # Instantiate a new Organism object using data fetched from metadata.
        new_organism = Organism(
            species_taxonomy_id=species_taxonomy_id,
            taxonomy_id=self.get_meta_single_meta_key(species_id, "organism.taxonomy_id"),
            common_name=common_name,
            scientific_name=self.get_meta_single_meta_key(species_id, "organism.scientific_name"),
            biosample_id=biosample_id,
            strain=self.get_meta_single_meta_key(species_id, "organism.strain"),
            strain_type=self.get_meta_single_meta_key(species_id, "organism.type"),
            scientific_parlance_name=self.get_meta_single_meta_key(species_id, "organism.scientific_parlance_name")
        )

        # Query the metadata database to find if an Organism with the same Ensembl name already exists.
        old_organism = meta_session.query(Organism).filter(
            Organism.biosample_id == new_organism.biosample_id).one_or_none()

        # If an existing Organism is found, return it and indicate that it already existed.
        if old_organism:
            return old_organism
        else:
            # If no existing Organism is found, conduct additional checks before creating a new one.

            # Check if the new organism's taxonomy ID exists in the taxonomy database.
            with self.taxonomy_db.session_scope() as session:
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

            meta_session.add(new_organism)
            # Return the newly created Organism and indicate that it is new.
            return new_organism

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
                # TODO: Just delete the comment. No one cares about the assembly sequence table.
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
                    'apicoplast_chromosome': 'SO:0001259',
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

    def get_or_new_assembly(self, species_id, meta_session, source=None):
        # Get the new assembly accession  from the core handed over
        assembly_accession = self.get_meta_single_meta_key(species_id, "assembly.accession")
        assembly = meta_session.query(Assembly).filter(Assembly.accession == assembly_accession).one_or_none()
        if source is None:
            dataset_source = self.get_or_new_source(meta_session, "core")
        else:
            dataset_source = source

        # This should return the existing objects
        if assembly is not None:
            # Get the existing assembly dataset
            assembly_dataset = meta_session.query(Dataset).filter(Dataset.label == assembly_accession).one_or_none()
            # I should not need this, but double check on database updating.
            assembly_dataset_attributes = assembly_dataset.dataset_attributes
            assembly_sequences = assembly.assembly_sequences
            return assembly, assembly_dataset, assembly_dataset_attributes, assembly_sequences, dataset_source
        else:
            attributes = self.get_meta_list_from_prefix_meta_key(species_id, "assembly")
            is_reference = 1 if self.get_meta_single_meta_key(species_id, "assembly.is_reference") else 0
            with self.db.session_scope() as session:
                level = (session.execute(db.select(CoordSystem.name).filter(
                    CoordSystem.species_id == species_id).order_by(CoordSystem.rank)).all())[0][0]
                tol_id = self.get_meta_single_meta_key(species_id, "assembly.tol_id")
                accession_body = self.get_meta_single_meta_key(species_id,
                                                               "assembly.accession_body") if self.get_meta_single_meta_key(
                    species_id, "assembly.accession_body") else "INSDC"
            assembly = Assembly(
                ucsc_name=self.get_meta_single_meta_key(species_id, "assembly.ucsc_alias"),
                accession=self.get_meta_single_meta_key(species_id, "assembly.accession"),
                level=level,
                name=self.get_meta_single_meta_key(species_id, "assembly.name"),
                accession_body=accession_body,
                assembly_default=self.get_meta_single_meta_key(species_id, "assembly.default"),
                tol_id=tol_id,
                created=func.now(),
                assembly_uuid=str(uuid.uuid4()),
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

        provider_name_attr = aliased(DatasetAttribute, name="provider_name_attr")
        last_geneset_update_attr = aliased(DatasetAttribute, name="last_geneset_update_attr")

        # Query for an existing combination
        existing_combination = (
            meta_session.query(Genome.genome_id)
            .join(GenomeDataset, Genome.genome_id == GenomeDataset.genome_id)
            .join(Dataset, GenomeDataset.dataset_id == Dataset.dataset_id)
            .join(Assembly, Genome.assembly_id == Assembly.assembly_id)
            .join(provider_name_attr, Dataset.dataset_id == provider_name_attr.dataset_id)
            .join(last_geneset_update_attr, Dataset.dataset_id == last_geneset_update_attr.dataset_id)
            .filter(
                Dataset.name == "genebuild",
                Assembly.accession == assembly_accession,
                provider_name_attr.value == provider_name,
                last_geneset_update_attr.value == last_geneset_update,
                provider_name_attr.attribute.has(Attribute.name == "genebuild.provider_name"),
                last_geneset_update_attr.attribute.has(Attribute.name == "genebuild.last_geneset_update"),
            )
        )

        test_for_existing = meta_session.query(existing_combination.exists()).scalar()
        # Check if the combination exists
        if test_for_existing:
            raise exceptions.MetaException(
                "genebuild.provider_name, genebuild.last_geneset_update, and assembly.accession cannot match existing records."
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
            genebuild_dataset_attributes = update_attributes(genebuild_dataset, attributes, meta_session, replace=True)

        return genebuild_dataset, genebuild_dataset_attributes

    def new_homology(self, meta_session, species_id, genome=None, source=None, dataset_attributes=None, version="1.0"):
        if source is None:
            production_name = self.get_meta_single_meta_key(species_id, "organism.production_name")
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