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

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)


class CoreMetaUpdater(BaseMetaUpdater):
    def __init__(self, db_uri, metadata_uri, taxonomy_uri, release=None):
        super().__init__(db_uri, metadata_uri, taxonomy_uri, release)
        self.db_type = 'core'
        self.meta_dict = {}
        self._load_meta_dict()
        self._validate_required_attributes()

    def _load_meta_dict(self):
        """Load metadata into meta_dict from the database.
        Stores all values for each meta_key as a list to handle potential duplicates.
        """
        with self.db.session_scope() as session:
            results = session.query(Meta).filter(Meta.meta_value.isnot(None),
                                                 Meta.meta_value.notin_(['', 'Null', 'NULL'])).all()
            for result in results:
                species_id = result.species_id
                meta_key = result.meta_key
                meta_value = result.meta_value
                if species_id not in self.meta_dict:
                    self.meta_dict[species_id] = {}
                if meta_key not in self.meta_dict[species_id]:
                    self.meta_dict[species_id][meta_key] = []
                self.meta_dict[species_id][meta_key].append(meta_value)

    def _validate_required_attributes(self):
        """Check if all required attributes are present in the meta_dict for each species."""
        # TODO: Move to datacheck
        with self.metadata_db.session_scope() as session:
            required_attributes = session.query(Attribute.name).filter(Attribute.required == 1).all()
            required_attribute_names = {attr.name for attr in required_attributes}

        missing_attributes = {}
        for species_id, meta in self.meta_dict.items():
            if species_id is None:
                continue
            missing = required_attribute_names - set(meta.keys())
            if missing:
                missing_attributes[species_id] = missing

        if missing_attributes:
            error_msg = "\n".join([
                f"Species ID {species_id} is missing required attributes: {', '.join(sorted(missing))}"
                for species_id, missing in missing_attributes.items()
            ])
            raise exceptions.MissingMetaException(error_msg)

    def get_meta_single_meta_key(self, species_id, parameter):
        """
        Get a single value for a meta_key.
        Raises an exception if multiple values exist for the same key.

        Returns:
            str or None: The meta value, or None if not found

        Raises:
            DuplicateMetaKeyException: If multiple values exist for the key
        """
        species_meta = self.meta_dict.get(species_id)
        if species_meta is None:
            return None

        values = species_meta.get(parameter, [None])

        if len(values) > 1:
            raise exceptions.MetaException(
                f"Species {species_id} has {len(values)} values for meta_key '{parameter}': {values}. "
                f"A single key is currently required to successfully hand over."
            )

        return values[0]

    def get_meta_all_values(self, species_id, parameter):
        """
        Get all values for a meta_key, handling cases with 0, 1, or multiple values.

        Returns:
            list: List of all values for the key (empty list if none exist)
        """
        species_meta = self.meta_dict.get(species_id)
        if species_meta is None:
            return []

        return species_meta.get(parameter, [])

    def get_meta_list_from_prefix_meta_key(self, species_id, prefix):
        """
        Get all meta_keys with a given prefix, including all values.

        Returns:
            dict or None: Dictionary of {key: [values]} where values is always a list,
                         or None if species not found
        """
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
        genebuild_dataset, genebuild_dataset_attributes = self._create_genebuild(
            species_id, meta_session, dataset_source
        )

        # Checking for an existing genome uuid:
        old_genome_uuid = self.get_meta_single_meta_key(species_id, "genome.genome_uuid")
        if old_genome_uuid is not None:
            old_genome = meta_session.query(Genome).filter(
                Genome.genome_uuid == old_genome_uuid).one_or_none()
            if old_genome is not None:
                raise exceptions.MetadataUpdateException(
                    f"Species {species_id}: Core database contains a genome.genome_uuid which matches an entry in the meta table.")
                # TODO: Move to datacheck
            else:
                raise exceptions.MetadataUpdateException(
                    f"Species {species_id}: Database contains a Genome.genome_uuid, but corresponding data is not in meta table.")
                # TODO: Move to datacheck

        if self.is_object_new(organism):
            logger.info(f'Species {species_id}: New organism')
            if not self.is_object_new(assembly):
                raise exceptions.MetadataUpdateException(
                    f"Species {species_id}: New organism, but existing assembly accession")
                # TODO: Move to datacheck , but leave here to be sure
            new_genome, assembly_genome_dataset, genebuild_genome_dataset = self.new_genome(meta_session,
                                                                                            species_id,
                                                                                            organism,
                                                                                            assembly,
                                                                                            assembly_dataset,
                                                                                            genebuild_dataset)
            self.concurrent_commit_genome_uuid(meta_session, species_id, new_genome.genome_uuid)

        elif self.is_object_new(assembly):
            logger.info(f'Species {species_id}: New assembly')
            new_genome, assembly_genome_dataset, genebuild_genome_dataset = self.new_genome(meta_session,
                                                                                            species_id,
                                                                                            organism,
                                                                                            assembly,
                                                                                            assembly_dataset,
                                                                                            genebuild_dataset)
            self.concurrent_commit_genome_uuid(meta_session, species_id, new_genome.genome_uuid)


        # Create genome and populate the database with assembly and dataset
        else:
            provider_name = self.get_meta_single_meta_key(species_id, "genebuild.provider_name")
            geneset_update = self.get_meta_single_meta_key(species_id, "genebuild.last_geneset_update")

            query = meta_session.query(Genome).join(
                Assembly, Genome.assembly
            ).filter(
                Assembly.accession == assembly.accession,
                Genome.provider_name == provider_name,
                Genome.genebuild_date == geneset_update
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
        genebuild_date = self.get_meta_single_meta_key(species_id, "genebuild.last_geneset_update")
        url_name = self.get_meta_single_meta_key(species_id, "assembly.url_name")
        provider_name = self.get_meta_single_meta_key(species_id, "genebuild.provider_name")
        annotation_source = self.get_meta_single_meta_key(species_id, "genebuild.annotation_source")
        if None in (production_name, genebuild_date, annotation_source, provider_name):
            raise exceptions.MetadataUpdateException(f"Unable to find required keys from meta")
        # get next release inline to attach the genome to
        planned_release = get_or_new_release(self.metadata_uri)
        new_genome = Genome(
            genome_uuid=str(uuid.uuid4()),
            assembly=assembly,
            organism=organism,
            genebuild_date=genebuild_date,
            created=func.now(),
            production_name=production_name,
            url_name=url_name,
            annotation_source=annotation_source,
            provider_name=provider_name,
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

        self._create_genome_group_members(meta_session, species_id, new_genome, planned_release)


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

    def _create_genome_group_members(self, meta_session, species_id, new_genome, planned_release):
        """
        Add genome to genome groups specified in meta keys.

        Args:
            meta_session: The metadata database session
            species_id: The species ID from the core database
            new_genome: The newly created Genome object
            planned_release: The EnsemblRelease object

        Raises:
            MetadataUpdateException: If a specified genome group doesn't exist
        """
        genome_group_names = self.get_meta_all_values(species_id, "genome.genome_group")

        if not genome_group_names:
            return None

        for group_name in genome_group_names:
            # Check if the genome group exists
            genome_group = meta_session.query(GenomeGroup).filter(
                GenomeGroup.name == group_name
            ).one_or_none()

            if genome_group is None:
                raise exceptions.MetadataUpdateException(
                    f"Genome group '{group_name}' specified in meta key 'genome.genome_group' does not exist in the database"
                )

            # Create GenomeGroupMember
            genome_group_member = GenomeGroupMember(
                genome=new_genome,
                genome_group=genome_group,
                ensembl_release=planned_release,
                is_current=1,
                is_reference=0,
            )
            meta_session.add(genome_group_member)
            logger.info(f"Added genome {new_genome.genome_uuid} to genome group '{group_name}'")

    def get_or_new_organism(self, species_id, meta_session):
        """
        Get an existing Organism instance or create a new one, depending on the information from the metadata database.
        """
        # Fetch the Ensembl name of the organism from metadata using either 'species.biosample_id'
        # or 'organism.production_name' as the key.
        biosample_id = self.get_meta_single_meta_key(species_id, "organism.biosample_id")
        if biosample_id is None:
            biosample_id = self.get_meta_single_meta_key(species_id, "organism.production_name")
        tol_id = self.get_meta_single_meta_key(species_id, "assembly.tol_id")  # This one should be deleted eventually.
        tol_id = self.get_meta_single_meta_key(species_id, "organism.tol_id")

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
            strain_type=self.get_meta_single_meta_key(species_id, "organism.strain_type"),
            scientific_parlance_name=self.get_meta_single_meta_key(species_id, "organism.scientific_parlance_name"),
            tol_id=tol_id
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
        Get the assembly sequences and aliases from the core DB.
        Returns both AssemblySequence and SequenceAlias objects.
        """
        assembly_sequences = []
        sequence_aliases = []

        with self.db.session_scope() as session:
            circular_seq_attrib = aliased(SeqRegionAttrib)
            results = (session.query(SeqRegion.name, SeqRegion.length, CoordSystem.name.label("coord_system_name"),
                                     SeqRegionSynonym.synonym, circular_seq_attrib.value.label("is_circular"))
                       .outerjoin(SeqRegion.coord_system)
                       .outerjoin(SeqRegionSynonym, SeqRegionSynonym.seq_region_id == SeqRegion.seq_region_id)
                       .join(SeqRegion.seq_region_attrib)
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

            # Single pass: collect synonyms AND process sequence info
            synonym_dict = defaultdict(list)
            accession_info = defaultdict(
                lambda: {
                    "length": None,
                    "location": None,
                    "chromosomal": None,
                    "karyotype_rank": None,
                    "type": None,
                    "is_circular": 0,
                })

            location_mapping = {
                "nuclear_chromosome": "SO:0000738",
                "mitochondrial_chromosome": "SO:0000737",
                "chloroplast_chromosome": "SO:0000745",
                "apicoplast_chromosome": "SO:0001259",
                None: "SO:0000738",
            }

            for seq_region_name, seq_region_length, coord_system_name, synonym, is_circular in results:
                if synonym:
                    synonym_dict[seq_region_name].append(synonym)

                if accession_info[seq_region_name]["length"] is None:
                    location = attribute_dict.get(seq_region_name, {}).get("sequence_location", None)
                    sequence_location = location_mapping[location]
                    karyotype_rank = attribute_dict.get(seq_region_name, {}).get("karyotype_rank", None)

                    chromosomal = 1 if karyotype_rank is not None else (1 if coord_system_name == "chromosome" else 0)

                    accession_info[seq_region_name].update({
                        "length": seq_region_length,
                        "location": sequence_location,
                        "chromosomal": chromosomal,
                        "karyotype_rank": karyotype_rank,
                        "type": coord_system_name,
                        "is_circular": 1 if is_circular == "1" else 0
                    })

            for seq_region_name, info in accession_info.items():
                # Determine the proper accession
                accession = self._get_valid_accession(seq_region_name, synonym_dict.get(seq_region_name, []))

                assembly_sequence = AssemblySequence(
                    name=seq_region_name,
                    assembly=assembly,
                    accession=accession,
                    chromosomal=info["chromosomal"],
                    length=info["length"],
                    sequence_location=info["location"],
                    chromosome_rank=info["karyotype_rank"],
                    type=info["type"],
                    is_circular=info["is_circular"]
                )
                assembly_sequences.append(assembly_sequence)

                # Create SequenceAlias objects for each synonym
                for synonym in synonym_dict.get(seq_region_name, []):
                    sequence_alias = SequenceAlias(
                        assembly_sequence=assembly_sequence,
                        alias=synonym,
                        source="core"
                    )
                    sequence_aliases.append(sequence_alias)

            return assembly_sequences, sequence_aliases


    def _is_valid_ena_accession(self, identifier):
        """
        Check if an identifier matches ENA sequence identifier rules for annotated sequences.

        Valid patterns:
        - [A-Z]{1}[0-9]{5}.[0-9]+
        - [A-Z]{2}[0-9]{6}.[0-9]+
        - [A-Z]{2}[0-9]{8}
        - [A-Z]{4}[0-9]{2}S?[0-9]{6,8}
        - [A-Z]{6}[0-9]{2}S?[0-9]{7,9}

        Returns:
            bool: True if identifier matches any pattern
        """
        ENA_ACCESSION_PATTERNS = [
            re.compile(r'^[A-Z]{1}[0-9]{5}\.[0-9]+$'),
            re.compile(r'^[A-Z]{2}[0-9]{6}\.[0-9]+$'),
            re.compile(r'^[A-Z]{2}[0-9]{8}$'),
            re.compile(r'^[A-Z]{4}[0-9]{2}S?[0-9]{6,8}$'),
            re.compile(r'^[A-Z]{6}[0-9]{2}S?[0-9]{7,9}$'),
        ]
        return any(pattern.match(identifier) for pattern in ENA_ACCESSION_PATTERNS)

    def _get_valid_accession(self, seq_region_name, synonyms):
        """
        Get a valid ENA accession for a sequence region.

        First checks if the seq_region_name matches ENA rules.
        If not, searches through synonyms for the first match.

        Args:
            seq_region_name: The sequence region name from core DB
            synonyms: List of synonyms for this sequence region

        Returns:
            str: Valid ENA accession

        Raises:
            MetadataUpdateException: If no valid accession found
        """
        if self._is_valid_ena_accession(seq_region_name):
            return seq_region_name

        # Search through synonyms for the first valid accession
        # TODO: Make this match the assembly report instead of taking first match
        for synonym in synonyms:
            if self._is_valid_ena_accession(synonym):
                return synonym

        raise exceptions.MetadataUpdateException(
            f"No sequence accession found that matches ENA identifier rules for sequence '{seq_region_name}'. "
            f"Checked name and {len(synonyms)} synonym(s): {synonyms}"
        )

    def get_or_new_assembly(self, species_id, meta_session, source=None):
        """
        Queries the existing metadata to see if the assembly exists and determines
        whether to attach to existing, create new, or return an error.

        Handles multiple assemblies with same accession by comparing sequences.
        Excludes assemblies with FAULTY dataset status.
        """

        assembly_accession = self.get_meta_single_meta_key(species_id, "assembly.accession")
        # Query assemblies but exclude those with faulty assembly datasets
        assemblies = (meta_session.query(Assembly)
                      .outerjoin(Genome, Genome.assembly_id == Assembly.assembly_id)
                      .outerjoin(GenomeDataset, GenomeDataset.genome_id == Genome.genome_id)
                      .outerjoin(Dataset, Dataset.dataset_id == GenomeDataset.dataset_id)
                      .outerjoin(DatasetType, Dataset.dataset_type_id == DatasetType.dataset_type_id)
                      .filter(Assembly.accession == assembly_accession)
                      .filter(or_(
            DatasetType.name != "assembly",
            Dataset.status != DatasetStatus.FAULTY
        )).distinct().all())
        if source is None:
            dataset_source = self.get_or_new_source(meta_session, "core")
        else:
            dataset_source = source

        # Query core DB once upfront - get names and count together
        incoming_seq_names = self._get_incoming_sequence_names(species_id)
        incoming_count = len(incoming_seq_names)

        # Case 1: New assembly accession - Fresh load
        if not assemblies:
            return self._create_new_assembly(species_id, meta_session, dataset_source, assembly_accession)

        # Check for force new UUID flag
        force_new_uuid = self.get_meta_single_meta_key(species_id, "assembly.create_new_uuid")

        # Find assemblies that match on sequence count and names
        matching_assembly = self._find_matching_assembly(assemblies, incoming_seq_names, incoming_count)

        # Case 2: Found exact match - Attach to existing
        if matching_assembly is not None:
            return self._attach_to_existing_assembly(matching_assembly, meta_session, assembly_accession,
                                                     dataset_source)

        # No exact match found - either error or force new
        if int(force_new_uuid) == 1:
            return self._create_new_assembly(species_id, meta_session, dataset_source, assembly_accession)

        # Return error describing discrepancies
        error_details = self._generate_discrepancy_error(assemblies, incoming_seq_names, incoming_count)
        raise exceptions.MetadataUpdateException(f"Assembly mismatch: {error_details}")

    def _find_matching_assembly(self, assemblies, incoming_names, incoming_count):
        """
        Find an assembly that matches both sequence count and names.
        Uses pre-fetched incoming data to avoid redundant queries.

        Returns:
            Assembly or None: The matching assembly if found, None otherwise
        """
        # Filter to assemblies with matching count
        count_matches = [a for a in assemblies if len(a.assembly_sequences) == incoming_count]

        # From those, find one with matching names
        for assembly in count_matches:
            existing_names = {seq.name for seq in assembly.assembly_sequences}
            if existing_names == incoming_names:
                return assembly
        return None

    def _get_incoming_sequence_names(self, species_id):
        """
        Get the names of top-level sequences from the core DB.
        Single query to avoid redundancy.

        Returns:
            set: Set of sequence names
        """
        with self.db.session_scope() as session:
            results = (session.query(SeqRegion.name)
                       .join(SeqRegion.coord_system)
                       .join(SeqRegion.seq_region_attrib)
                       .join(SeqRegionAttrib.attrib_type)
                       .filter(CoordSystem.species_id == species_id)
                       .filter(AttribType.code == "toplevel")
                       .filter(CoordSystem.name != "lrg")
                       .all())
        return {name for (name,) in results}

    def _generate_discrepancy_error(self, assemblies, incoming_names, incoming_count):
        """
        Generate a detailed error message describing why no match was found.
        """
        count_matching_assemblies = [a for a in assemblies if len(a.assembly_sequences) == incoming_count]

        if not count_matching_assemblies:
            # No count matches
            assembly_info = [(a.assembly_uuid, len(a.assembly_sequences)) for a in assemblies]
            counts_str = ", ".join([f"UUID {uuid}: {count} sequences" for uuid, count in assembly_info])
            return (f"Assembly accession found {len(assemblies)} time(s) in database, "
                    f"but none match incoming sequence count of {incoming_count}. "
                    f"Existing counts: {counts_str}")

        # Count matches but names don't
        error_lines = [
            f"Assembly accession found with matching sequence count ({incoming_count}), "
            f"but sequence names do not match.",
            f"Incoming names: {sorted(incoming_names)}"
        ]

        for assembly in count_matching_assemblies:
            existing_names = {seq.name for seq in assembly.assembly_sequences}
            missing = incoming_names - existing_names
            extra = existing_names - incoming_names

            error_lines.append(f"\nUUID {assembly.assembly_uuid}: {sorted(existing_names)}")
            if missing:
                error_lines.append(f"  Missing in existing: {sorted(missing)}")
            if extra:
                error_lines.append(f"  Extra in existing: {sorted(extra)}")

        return "\n".join(error_lines)

    def _attach_to_existing_assembly(self, assembly, meta_session, assembly_accession, dataset_source):
        """Attach to existing assembly when sequences match."""
        # Find the assembly dataset through the relationship path
        # Assembly -> Genome -> GenomeDataset -> Dataset
        assembly_dataset = (meta_session.query(Dataset)
                            .join(GenomeDataset, GenomeDataset.dataset_id == Dataset.dataset_id)
                            .join(Genome, Genome.genome_id == GenomeDataset.genome_id)
                            .join(DatasetType, Dataset.dataset_type_id == DatasetType.dataset_type_id)
                            .filter(Genome.assembly_id == assembly.assembly_id)
                            .filter(DatasetType.name == "assembly")
                            .filter(Dataset.status != DatasetStatus.FAULTY)
                            .first())

        if assembly_dataset is None:
            raise exceptions.MetadataUpdateException(
                f"Assembly {assembly_accession} exists but no valid (non-faulty) assembly dataset found"
            )

        assembly_dataset_attributes = assembly_dataset.dataset_attributes
        assembly_sequences = assembly.assembly_sequences
        return assembly, assembly_dataset, assembly_dataset_attributes, assembly_sequences, dataset_source

    def _create_new_assembly(self, species_id, meta_session, dataset_source, assembly_accession):
        """Create a new assembly with unique UUID."""
        attributes = self.get_meta_list_from_prefix_meta_key(species_id, "assembly")
        is_reference = 1 if self.get_meta_single_meta_key(species_id, "assembly.is_reference") else 0

        with self.db.session_scope() as session:
            level = (session.execute(db.select(CoordSystem.name).filter(
                CoordSystem.species_id == species_id).order_by(CoordSystem.rank)).all())[0][0]
            accession_body = self.get_meta_single_meta_key(species_id, "assembly.accession_body")
            if not accession_body:
                accession_body = "INSDC"

        assembly = Assembly(
            ucsc_name=self.get_meta_single_meta_key(species_id, "assembly.ucsc_alias"),
            accession=assembly_accession,
            level=level,
            name=self.get_meta_single_meta_key(species_id, "assembly.name"),
            accession_body=accession_body,
            assembly_default=self.get_meta_single_meta_key(species_id, "assembly.default"),
            created=func.now(),
            assembly_uuid=str(uuid.uuid4()),
            is_reference=is_reference,
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

        # Get assembly sequences AND aliases
        assembly_sequences, sequence_aliases = self.get_assembly_sequences(species_id, assembly)

        meta_session.add_all(assembly_sequences)
        meta_session.add_all(sequence_aliases)
        meta_session.add_all(assembly_dataset_attributes)

        return assembly, assembly_dataset, assembly_dataset_attributes, assembly_sequences, dataset_source

    def _create_genebuild(self, species_id, meta_session, source=None):
        """
        Create a new genebuild dataset for a species from a core database.
        This method always creates a new dataset - if a matching genome already exists, it throws an exception.
        The uniqueness is enforced at the Genome level (assembly + provider + genebuild_date).
        """
        assembly_accession = self.get_meta_single_meta_key(species_id, "assembly.accession")
        provider_name = self.get_meta_single_meta_key(species_id, "genebuild.provider_name")
        last_geneset_update = self.get_meta_single_meta_key(species_id, "genebuild.last_geneset_update")
        annotation_source = self.get_meta_single_meta_key(species_id, "genebuild.annotation_source")
        # Query for an existing combination - this is our uniqueness check
        # If this exists, we should NOT create a new one
        existing_combination = (
            meta_session.query(Genome.genome_id)
            .join(Assembly, Genome.assembly_id == Assembly.assembly_id)
            .filter(
                Assembly.accession == assembly_accession,
                Genome.provider_name == provider_name,
                Genome.genebuild_date == last_geneset_update,
            )
        )

        test_for_existing = meta_session.query(existing_combination.exists()).scalar()
        if test_for_existing:
            raise exceptions.MetaException(
                f"Genebuild already exists for assembly {assembly_accession} "
                f"with provider '{provider_name}' and date '{last_geneset_update}'. "
                "Cannot create duplicate genebuild."
            )

        # Check for conflicting annotation source
        # This isn't persay a strict requirment but it will make the FTP confusing as hell if we allow it.
        conflicting_combination = (
            meta_session.query(Genome.genome_id)
            .join(Assembly, Genome.assembly_id == Assembly.assembly_id)
            .filter(
                Assembly.accession == assembly_accession,
                Genome.provider_name != provider_name,
                Genome.annotation_source == annotation_source,
            )
        )

        test_for_conflicting = meta_session.query(conflicting_combination.exists()).scalar()
        if test_for_conflicting:
            raise exceptions.MetaException(
                f"Genebuild already exists for assembly {assembly_accession} "
                f"existing genebuild with different provider uses an annotation source of '{annotation_source}'. "
                "Please use a different one."
            )
        genebuild_label = f"{assembly_accession}_{provider_name}_{last_geneset_update}"

        if source is None:
            dataset_source = self.get_or_new_source(meta_session, "core")
        else:
            dataset_source = source

        dataset_type = meta_session.query(DatasetType).filter(DatasetType.name == "genebuild").first()
        attributes = self.get_meta_list_from_prefix_meta_key(species_id, "genebuild.")
        dataset_version = last_geneset_update
        dataset_factory = DatasetFactory(self.metadata_uri)
        (dataset_uuid, genebuild_dataset, genebuild_dataset_attributes,
         new_genome_dataset) = dataset_factory.create_dataset(
            meta_session, None, dataset_source,
            dataset_type, attributes, "genebuild",
            genebuild_label, dataset_version
        )

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
