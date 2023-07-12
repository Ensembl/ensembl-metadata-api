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
#   limitations under the License.
import logging
import re
import uuid
from collections import defaultdict

import sqlalchemy as db
from ensembl.core.models import Meta, Assembly as AssemblyCore, CoordSystem, SeqRegionAttrib, SeqRegion, \
    SeqRegionSynonym, AttribType, ExternalDb
from sqlalchemy import select, update, func, and_
from sqlalchemy.engine import make_url
from sqlalchemy.orm import aliased
from ensembl.database import DBConnection
from sqlalchemy.exc import NoResultFound
import sys
from ensembl.production.metadata.api.genome import GenomeAdaptor
from ensembl.production.metadata.api.models import *
from ensembl.production.metadata.updater.base import BaseMetaUpdater


##TODO:
# Prevent deletion of release data.
# Logic:
##Create new organism on new production name if no ensembl name. If ensembl name is given create new if none, if already exists create new genome based on production name.
##Check that taxid is present in db.


class CoreMetaUpdater(BaseMetaUpdater):
    def __init__(self, db_uri, metadata_uri, taxonomy_uri, release=None):
        # Each of these objects represents a table in the database to store data in as eith0er an array or a single object.
        self.organism = None
        self.metadata_uri = metadata_uri
        self.taxonomy_uri = taxonomy_uri
        self.assembly = None
        self.assembly_sequences = None  # array
        self.assembly_dataset = None
        self.assembly_dataset_attributes = None  # array
        self.genome = None
        self.genome_release = None

        self.genebuild_dataset_attributes = None  # array
        self.genebuild_dataset = None
        self.dataset_type = None
        self.dataset_source = None
        self.attribute = None

        super().__init__(db_uri, metadata_uri=self.metadata_uri, taxonomy_uri=self.taxonomy_uri, release=None)
        self.db_type = 'core'

    def process_core(self, **kwargs):
        # Special case for loading a single species from a collection database. Can be removed in a future release
        sel_species = kwargs.get('species', None)
        metadata_uri = kwargs.get('metadata_uri', self.metadata_uri)
        taxonomy_uri = kwargs.get('metadata_uri', self.taxonomy_uri)

        if sel_species:
            with self.db.session_scope() as session:
                multi_species = session.execute(
                    select(Meta.species_id).filter(Meta.meta_key == "species.production_name").filter(
                        Meta.meta_value == sel_species).distinct()
                )
        else:
            # Normal handling of collections from here
            # Handle multispecies databases and run an update for each species
            with self.db.session_scope() as session:
                multi_species = session.execute(
                    select(Meta.species_id).filter(Meta.meta_key == "species.production_name").distinct()
                )
        multi_species = [multi_species for multi_species, in multi_species]

        for species in multi_species:
            self.process_species(species, metadata_uri, taxonomy_uri)

    def process_species(self, species, metadata_uri, taxonomy_uri, db_uri):
        """
        Process an individual species from a core database to update the metadata db.
        This method contains the logic for updating the metadata
        """
        meta_conn = DBConnection(metadata_uri)
        with meta_conn.session_scope() as meta_session:
            self.organism, organism_status = self.get_or_new_organism(species, meta_session, metadata_uri, taxonomy_uri)
            self.assembly, self.assembly_dataset, self.assembly_dataset_attributes, self.assembly_sequences, \
                self.dataset_source, assembly_status = self.get_or_new_assembly(species, meta_session, metadata_uri,
                                                                                db_uri)
            #Add release check here!
            self.genebuild_dataset, self.genebuild_dataset_attributes = self.new_genebuild(species,
                                                                                           meta_session, db_uri)

        ###############Check if all sources are the same and it has been released ################

        # If all the sources are the same and it hasn't been released: Delete corresponding genome and create new


        # if new assembly, create new genome.


            print(self.organism, organism_status)

    def get_or_new_organism(self, species, meta_session, metadata_uri, taxonomy_uri):
        """
        Get an existing Organism instance or create a new one, depending on the information from the metadata database.
        """

        # Fetch the Ensembl name of the organism from metadata using either 'species.ensembl_name'
        # or 'species.production_name' as the key.
        ensembl_name = self.get_meta_single_meta_key(species, "species.ensembl_name")
        if ensembl_name is None:
            ensembl_name = self.get_meta_single_meta_key(species, "species.production_name")

        # Instantiate a new Organism object using data fetched from metadata.
        new_organism = Organism(
            species_taxonomy_id=self.get_meta_single_meta_key(species, "species.species_taxonomy_id"),  # REQURIED
            taxonomy_id=self.get_meta_single_meta_key(species, "species.taxonomy_id"),  # REQURIED
            display_name=self.get_meta_single_meta_key(species, "species.display_name"),  # REQURIED , MAY BE DELETED
            scientific_name=self.get_meta_single_meta_key(species, "species.scientific_name"),  # REQURIED
            url_name=self.get_meta_single_meta_key(species, "species.url"),
            ensembl_name=ensembl_name,
            strain=self.get_meta_single_meta_key(species, "species.strain"),
            #
        )

        # Query the metadata database to find if an Organism with the same Ensembl name already exists.
        old_organism = meta_session.query(Organism).filter(
            Organism.ensembl_name == new_organism.ensembl_name).one_or_none()

        # If an existing Organism is found, return it and indicate that it already existed.
        if old_organism:
            return old_organism, "Existing"
        else:
            # If no existing Organism is found, conduct additional checks before creating a new one.

            # Check if the new organism's taxonomy ID exists in the taxonomy database.
            conn = GenomeAdaptor(metadata_uri=metadata_uri, taxonomy_uri=taxonomy_uri)
            try:
                conn.fetch_taxonomy_names(taxonomy_ids=new_organism.taxonomy_id)
            except NoResultFound:
                raise Exception("taxid not found in taxonomy database for scientific name")

            # Check if an Assembly with the same accession already exists in the metadata database.
            accession = self.get_meta_single_meta_key(species, "assembly.accession")
            assembly_test = meta_session.query(Assembly).filter(Assembly.accession == accession).one_or_none()
            if assembly_test is not None:
                raise Exception(
                    "Assembly Accession already exists for a different organism. Please do a manual update.")

            # Fetch the division name of the new organism from metadata.
            division_name = self.get_meta_single_meta_key(species, "species.division")
            if division_name is None:
                Exception("No species.division found in meta table")

            # Query the metadata database to find if an OrganismGroup with the same division name already exists.
            division = meta_session.query(OrganismGroup).filter(OrganismGroup.name == division_name).one_or_none()
            if division is None:
                # If no such OrganismGroup exists, create a new one.
                division = OrganismGroup(
                    type="Division",
                    name=division_name,
                )

            # Create a new OrganismGroupMember linking the new Organism to the division group.
            organism_group_member = OrganismGroupMember(
                is_reference=0,
                organism_id=new_organism,
                organism_group_id=division,
            )

            # Return the newly created Organism and indicate that it is new.
            return new_organism, "New"

    def get_assembly_sequences(self, species, assembly):
        """
        Get the assembly sequences and the values that correspond to the metadata table
        """
        assembly_sequences = []
        with self.db.session_scope() as session:
            # Create an alias for SeqRegionAttrib and AttribType to be used for sequence_location
            SeqRegionAttribAlias = aliased(SeqRegionAttrib)
            AttribTypeAlias = aliased(AttribType)

            # One complicated query to get all the data. Otherwise this takes far too long to do.
            results = (session.query(SeqRegion.name, SeqRegion.length, CoordSystem.name,
                                     SeqRegionAttribAlias.value, SeqRegionSynonym.synonym, ExternalDb.db_name)
                       .join(SeqRegion.coord_system)
                       .join(SeqRegion.seq_region_attrib)
                       .join(SeqRegionAttrib.attrib_type)
                       .join(CoordSystem.meta)
                       .outerjoin(SeqRegion.seq_region_synonym)
                       .outerjoin(SeqRegionSynonym.external_db)
                       .join(SeqRegionAttribAlias, SeqRegion.seq_region_attrib)  # join with SeqRegionAttribAlias
                       .outerjoin(AttribTypeAlias, SeqRegionAttribAlias.attrib_type)  # join with AttribTypeAlias
                       .filter(Meta.species_id == species)
                       .filter(AttribType.code == "toplevel")  # ensure toplevel
                       .filter(AttribTypeAlias.code == "sequence_location").all())  # ensure sequence_location

            # Create a dictionary so that the results can have multiple synonyms per line and only one SeqRegion
            results_dict = defaultdict(dict)
            for seq_region_name, seq_region_length, coord_system_name, sequence_location, synonym, db_name in results:
                key = (seq_region_name, seq_region_length, coord_system_name, sequence_location)
                results_dict[key][synonym] = db_name

            for (
                    seq_region_name, seq_region_length, coord_system_name,
                    sequence_location), synonyms in results_dict.items():
                # Test if chromosomal:
                if coord_system_name == "chromosome":
                    chromosomal = 1
                else:
                    chromosomal = 0
                # Test to see if the seq_name follows accession standards (99% of sequences)
                if re.match(r'^[a-zA-Z]+\d+\.\d+', seq_region_name):
                    # If so assign it to accession
                    accession = seq_region_name
                    if not synonyms:
                        # If it doesn't have any synonyms the accession is the name.
                        name = accession
                    # otherwise join all the accessions and store them in name
                    ################Likely problematic in the future######################
                    name = ";".join(synonyms.keys())
                else:
                    # For named sequences like chr1
                    name = seq_region_name
                    for synonym, db in synonyms:
                        # We used to match to KnownXref, however that should not be necessary. Testing this way for now.
                        if re.match(r'^[a-zA-Z]+\d+\.\d+', synonym):
                            accession = synonym
                        else:
                            name = name + ";" + synonym
                    if accession is none:
                        raise Exception(f"seq_region_name {seq_region_name} accession could not be found. Please check")
                assembly_sequence = AssemblySequence(
                    name=name,
                    assembly_id=assembly,
                    accession=accession,
                    chromosomal=chromosomal,
                    length=seq_region_length,
                    sequence_location=sequence_location,
                    # sequence_checksum="", Not implemented
                    # ga4gh_identifier="", Not implemented
                )
                assembly_sequences.append(assembly_sequence)
        return assembly_sequences

    #TODO: add in assembly override for unreleased. Call this method agiain during logic after removing old assembly.
    def get_or_new_assembly(self, species, meta_session, metadata_uri, db_uri):
        # Get the new assembly assession from the core handed over
        assembly_accession = self.get_meta_single_meta_key(species, "assembly.accession")
        assembly = meta_session.query(Assembly).filter(Assembly.accession == assembly_accession).one_or_none()

        if assembly is not None:
            # Get the existing assembly dataset
            assembly_dataset = meta_session.query(Dataset).filter(Dataset.label == assembly_accession).one_or_none()
            # I should not need this, but double check on database updating.
            assembly_dataset_attributes = assembly_dataset.dataset_attributes

            ################################ Tests #################################
            new_assembly_sequences = self.get_assembly_sequences(species, assembly)
            assembly_sequences = assembly.assembly_sequences
            # assembly sequences. Count and compare to make sure that they match.
            if len(assembly_sequences) != len(new_assembly_sequences):
                raise Exception("Number of sequences does not match number in database. "
                                "A new assembly requires a new accession.")
            ##########################################################################
            dataset_source, source_status = self.get_or_new_source(meta_session, db_uri, "core")

            return assembly, assembly_dataset, assembly_dataset_attributes, assembly_sequences, dataset_source, "Existing"


        else:
            with self.db.session_scope() as session:
                # May be problematic. Might be provided by genebuild.
                level = (session.execute(db.select(CoordSystem.name).filter(
                    CoordSystem.species_id == species).order_by(CoordSystem.rank)).all())[0][0]
            assembly = Assembly(
                ucsc_name=self.get_meta_single_meta_key(species, "assembly.ucsc_alias"),
                accession=self.get_meta_single_meta_key(self.species, "assembly.accession"),
                level=level,
#                level=self.get_meta_single_meta_key(self.species, "assembly.level"),   #Not yet implemented.
                name=self.get_meta_single_meta_key(self.species, "assembly.name"),
                accession_body=self.get_meta_single_meta_key(self.species, "assembly.provider"),
                assembly_default=self.get_meta_single_meta_key(self.species, "assembly.default"),
                tol_id=self.get_meta_single_meta_key(self.species, "assembly.tol_id"),  # Not implemented yet
                created=func.now(),
                ensembl_name=self.get_meta_single_meta_key(self.species, "assembly.name"),
                assembly_uuid=str(uuid.uuid4()),
            )
            dataset_type = meta_session.query(DatasetType).filter(DatasetType.name == "assembly").first()
            dataset_source, source_status = self.get_or_new_source(meta_session, db_uri, "core")

            assembly_dataset = Dataset(
                dataset_uuid=str(uuid.uuid4()),
                dataset_type=dataset_type,  # extract from dataset_type
                name="assembly",
                ###version=None, Could be changed.
                label=assembly.accession,  # Required. Makes for a quick lookup
                created=func.now(),
                dataset_source=dataset_source,  # extract from dataset_source
                status='Submitted',
            )

            attributes = self.get_meta_list_from_prefix_meta_key(species, "assembly")
            assembly_dataset_attributes = []
            for attribute, value in attributes:
                attribute.replace("assembly.", "", 1)
                meta_attribute = meta_session.query(Attribute).filter(Attribute.name == attribute).one_or_none()
                if meta_attribute is None:
                    raise Exception(f"Atribute {attribute} not found. Please enter it into the db manually")
                dataset_attribute = DatasetAttribute(
                    value=value,
                    dataset=assembly_dataset,
                    attribute=meta_attribute,
                )
                assembly_dataset_attributes.append(dataset_attribute)

            assembly_sequences = self.get_assembly_sequences(species, assembly)

            return assembly, assembly_dataset, assembly_dataset_attributes, assembly_sequences, dataset_source, "New"

    def new_genebuild(self, species, meta_session, db_uri):
        """
        Process an individual species from a core database to update the metadata db.
        This method contains the logic for updating the metadata
        This is not a get, as we don't update the metadata for genebuild, only replace it if it is not released.
        """
        assembly_accession = self.get_meta_single_meta_key(species, "assembly.accession")
        genebuild_version = self.get_meta_single_meta_key(species, "genebuild.version")
        genebuild_accesion = assembly_accession + "_" + genebuild_version
        # genebuild_dataset = meta_session.query(Dataset).filter(
        #         Dataset.label == genebuild_accesion).one_or_none()
        dataset_source, source_status = self.get_or_new_source(meta_session, db_uri, "core")
        dataset_type = meta_session.query(DatasetType).filter(DatasetType.name == "genebuild").first()
        genebuild_dataset = Dataset(
            dataset_uuid=str(uuid.uuid4()),
            dataset_type=dataset_type,  # extract from dataset_type
            name="assembly",
            version=genebuild_version,
            label=genebuild_accesion,  # Required. Used for lookup in this script
            created=func.now(),
            dataset_source=dataset_source,  # extract from dataset_source
            status='Submitted',
        )
        attributes = self.get_meta_list_from_prefix_meta_key(species, "genebuild.")
        genebuild_dataset_attributes = []
        for attribute, value in attributes:
            attribute.replace("genebuild.", "", 1)
            meta_attribute = meta_session.query(Attribute).filter(Attribute.name == attribute).one_or_none()
            if meta_attribute is None:
                raise Exception(f"Atribute {attribute} not found. Please enter it into the db manually")
            dataset_attribute = DatasetAttribute(
                value=value,
                dataset=genebuild_dataset,
                attribute=meta_attribute,
            )
            genebuild_dataset_attributes.append(dataset_attribute)


        return genebuild_dataset, genebuild_dataset_attributes





            attributes = self.get_meta_list_from_prefix_meta_key(self, species, "assembly")
            assembly_dataset_attributes = []
            for attribute, value in attributes:
                meta_attribute = meta_session.query(Attribute).filter(Attribute.name == attribute).one_or_none()
                if meta_attribute is None:
                    raise Exception(f"Atribute {attribute} not found. Please enter it into the db manually")
                dataset_attribute = DatasetAttribute(
                    value=value,
                    dataset=assembly_dataset,
                    attribute=meta_attribute,
                )
                assembly_dataset_attributes.append(dataset_attribute)

            assembly_sequences = self.get_assembly_sequences(species, assembly)

            return assembly, assembly_dataset, assembly_dataset_attributes, assembly_sequences, dataset_source, "New"

            # Old functions
        # self.new_genome()
        # self.new_genome_release()
        # self.new_assembly()
        # self.new_assembly_sequence()
        # self.new_assembly_dataset()
        # self.new_dataset_source()
        # self.new_genome_dataset()
        # self.new_datasets()

        ########################################################################
        #####                    Logic Section          ########################
        ########################################################################

    #     # Species Check
    #     # Check for new species by checking if ensembl name is already present in the database
    #     if not GenomeAdaptor(metadata_uri=self.metadata_db.url,
    #                          taxonomy_uri=self.taxonomy_uri).fetch_genomes_by_ensembl_name(
    #             self.organism.ensembl_name):
    #         # Check if the assembly accesion is already present in the database
    #         new_assembly_acc = self.get_meta_single_meta_key(self.species, "assembly.accession")
    #         with self.metadata_db.session_scope() as session:
    #             if session.query(session.query(Assembly).filter_by(accession=new_assembly_acc).exists()).scalar():
    #                 Exception("Assembly Accession already exists for a different organism. Please do a manual update.")
    #         self.create_organism()
    #         logging.info("Fresh Organism. Adding data to organism, genome, genome_release,"
    #                      " assembly, assembly_sequence, dataset, dataset source, and genome_dataset tables.")
    #
    #         # Check to see if it is an updated organism.
    #     else:
    #         with self.metadata_db.session_scope() as session:
    #             session.expire_on_commit = False
    #             test_organism = session.execute(db.select(Organism).filter(
    #                 Organism.ensembl_name == self.organism.ensembl_name)).one_or_none()
    #         self.organism.organism_id = Organism.organism_id
    #         self.organism.scientific_parlance_name = Organism.scientific_parlance_name
    #
    #         if int(test_organism.Organism.species_taxonomy_id) == int(
    #                 self.organism.species_taxonomy_id) and \
    #                 int(test_organism.Organism.taxonomy_id) == int(
    #             self.organism.taxonomy_id) and \
    #                 str(test_organism.Organism.display_name) == str(
    #             self.organism.display_name) and \
    #                 str(test_organism.Organism.scientific_name) == str(
    #             self.organism.scientific_name) and \
    #                 str(test_organism.Organism.url_name) == str(
    #             self.organism.url_name) and \
    #                 str(test_organism.Organism.strain) == str(self.organism.strain):
    #             logging.info("Old Organism with no change. No update to organism table")
    #             ################################################################
    #             ##### Assembly Check and Update
    #             ################################################################
    #             with self.metadata_db.session_scope() as session:
    #                 assembly_acc = session.execute(db.select(Assembly
    #                                                          ).join(Genome.assembly).join(Genome.organism).filter(
    #                     Organism.ensembl_name == self.organism.ensembl_name)).all()
    #                 new_assembly_acc = self.get_meta_single_meta_key(self.species, "assembly.accession")
    #                 assembly_test = False
    #                 for assembly_obj in assembly_acc:
    #                     if assembly_obj[0].accession == new_assembly_acc:
    #                         assembly_test = True
    #             if assembly_test:
    #                 logging.info(
    #                     "Old Assembly with no change. No update to Genome, genome_release, assembly, and assembly_sequence tables.")
    #                 for dataset in self.datasets:
    #                     with self.metadata_db.session_scope() as session:
    #                         # Check to see if any already exist:
    #                         # for all of genebuild in dataset, see if any have the same label (genebuild.id) and version. If so, don't update and error out here!
    #                         if dataset.name == "genebuild":
    #                             dataset_test = session.query(Dataset).filter(Dataset.name == "genebuild",
    #                                                                          Dataset.version == dataset.version,
    #                                                                          Dataset.label == dataset.label).first()
    #                             if dataset_test is None:
    #                                 gb_dataset_type = session.query(DatasetType).filter(
    #                                     DatasetType.name == "genebuild").first()
    #                                 dataset.dataset_type = gb_dataset_type
    #                                 dataset.dataset_source = self.dataset_source
    #                                 session.add(dataset)
    #
    #             else:
    #                 logging.info("New Assembly. Updating  genome, genome_release,"
    #                              " assembly, assembly_sequence, dataset, dataset source, and genome_dataset tables.")
    #                 self.update_assembly()
    #             ################################################################
    #             ##### dataset Check and Update
    #             ################################################################
    #             # Dataset section. More logic will be necessary for additional datasets. Currently only the genebuild is listed here.
    #
    #
    #
    #
    #         else:
    #             self.update_organism()
    #             logging.info("Old Organism with changes. Updating organism table")
    #
    # def create_organism(self):
    #     # In this, we are assuming that with a new genome, there will be a new assemblbly.
    #
    #     with self.metadata_db.session_scope() as session:
    #         # Organism section
    #         # Updating Organism, organism_group_member, and organism_group
    #         self.new_organism_group_and_members(session)
    #         # Add in the new assembly here
    #         # assembly sequence, assembly, genome, genome release.
    #         assembly_test = session.execute(db.select(Assembly).filter(
    #             Assembly.accession == self.assembly.accession)).one_or_none()
    #         if assembly_test is not None:
    #             Exception(
    #                 "Error, existing name but, assembly accession already found. Please update the Ensembl Name in the Meta field manually")
    #         if self.listed_release is not None:
    #             release = session.query(EnsemblRelease).filter(EnsemblRelease.release_id == self.listed_release).first()
    #             self.genome_release.ensembl_release = release
    #             self.genome_release.genome = self.genome
    #
    #         for assembly_seq in self.assembly_sequences:
    #             assembly_seq.assembly = self.assembly
    #         self.assembly.genomes.append(self.genome)
    #
    #         self.genome.organism = self.organism
    #
    #         # Update assembly dataset
    #         # Updates genome_dataset,dataset,dataset_source
    #         dataset_source_test = session.execute(
    #             db.select(DatasetSource).filter(DatasetSource.name == self.dataset_source.name)).one_or_none()
    #         if dataset_source_test is not None:
    #             Exception("Error, data already present in source")
    #
    #         dataset_type = session.query(DatasetType).filter(DatasetType.name == "assembly").first()
    #         if self.listed_release is not None:
    #             self.genome_dataset.ensembl_release = release
    #             self.genome_dataset.genome = self.genome
    #             self.genome_dataset.dataset = self.assembly_dataset
    #
    #         self.assembly_dataset.dataset_type = dataset_type
    #         self.assembly_dataset.dataset_source = self.dataset_source
    #
    #         assembly_genome_dataset = GenomeDataset(
    #             genome_dataset_id=None,  # Should be autogenerated upon insertion
    #             dataset_id=None,  # extract from dataset once genertated
    #             genome_id=None,  # extract from genome once genertated
    #             release_id=None,  # extract from release once genertated
    #             is_current=0,
    #         )
    #         assembly_genome_dataset.dataset = self.assembly_dataset
    #         self.genome.genome_datasets.append(assembly_genome_dataset)
    #
    #         #        session.add(assembly_genome_dataset)
    #
    #         # Dataset section. More logic will be necessary for additional datasets. Currently only the genebuild is listed here.
    #         for dataset in self.datasets:
    #             # Check to see if any already exist:
    #             # for all of genebuild in dataset, see if any have the same label (genebuild.id) and version. If so, don't update and error out here!
    #             if dataset.name == "genebuild":
    #                 dataset_test = session.query(Dataset).filter(Dataset.name == "genebuild",
    #                                                              Dataset.version == dataset.version,
    #                                                              Dataset.label == dataset.label).first()
    #                 if dataset_test is None:
    #                     dataset.dataset_type = session.query(DatasetType).filter(
    #                         DatasetType.name == "genebuild").first()
    #                     dataset.dataset_source = self.dataset_source
    #             temp_genome_dataset = GenomeDataset(
    #                 genome_dataset_id=None,  # Should be autogenerated upon insertion
    #                 dataset_id=None,  # extract from dataset once genertated
    #                 genome_id=None,  # extract from genome once genertated
    #                 release_id=None,  # extract from release once genertated
    #                 is_current=0,
    #             )
    #             temp_genome_dataset.dataset = dataset
    #             self.genome.genome_datasets.append(temp_genome_dataset)
    #         # Add everything to the database. Closing the session commits it.
    #         session.add(self.organism)
    #
    # def update_organism(self):
    #     with self.metadata_db.session_scope() as session:
    #         session.execute(
    #             update(Organism).where(Organism.ensembl_name == self.organism.ensembl_name).values(
    #                 species_taxonomy_id=self.organism.species_taxonomy_id,
    #                 taxonomy_id=self.organism.taxonomy_id,
    #                 display_name=self.organism.display_name,
    #                 scientific_name=self.organism.scientific_name,
    #                 url_name=self.organism.url_name,
    #                 ensembl_name=self.organism.ensembl_name,
    #                 strain=self.organism.strain,
    #             ))
    #
    #         # TODO: Add an update to the groups here.
    #
    # def update_assembly(self):
    #     # Change to new assembly/fresh
    #     with self.metadata_db.session_scope() as session:
    #         # Get the genome
    #         self.organism = session.query(Organism).filter(
    #             Organism.ensembl_name == self.organism.ensembl_name).first()
    #         self.genome.organism = self.organism
    #
    #         if self.listed_release is not None:
    #             release = session.query(EnsemblRelease).filter(EnsemblRelease.release_id == self.listed_release).first()
    #             self.genome_release.ensembl_release = release
    #             self.genome_release.genome = self.genome
    #
    #         self.assembly.genomes.append(self.genome)
    #
    #         # Update assembly dataset
    #         # Updates genome_dataset,dataset,dataset_source
    #         dataset_source_test = session.execute(
    #             db.select(DatasetSource).filter(DatasetSource.name == self.dataset_source.name)).one_or_none()
    #         if dataset_source_test is not None:
    #             self.dataset_source = session.query(DatasetSource).filter(
    #                 DatasetSource.name == self.dataset_source.name).first()
    #
    #         dataset_type = session.query(DatasetType).filter(DatasetType.name == "assembly").first()
    #         if self.listed_release is not None:
    #             self.genome_dataset.ensembl_release = release
    #             self.genome_dataset.genome = self.genome
    #             self.genome_dataset.dataset = self.assembly_dataset
    #
    #         self.assembly_dataset.dataset_type = dataset_type
    #         self.assembly_dataset.dataset_source = self.dataset_source
    #
    #         assembly_genome_dataset = GenomeDataset(
    #             genome_dataset_id=None,  # Should be autogenerated upon insertion
    #             dataset_id=None,  # extract from dataset once genertated
    #             genome_id=None,  # extract from genome once genertated
    #             release_id=None,  # extract from release once genertated
    #             is_current=0,
    #         )
    #         assembly_genome_dataset.dataset = self.assembly_dataset
    #         self.genome.genome_datasets.append(assembly_genome_dataset)
    #
    #         for dataset in self.datasets:
    #             # Check to see if any already exist:
    #             # for all of genebuild in dataset, see if any have the same label (genebuild.id) and version. If so, don't update and error out here!
    #             if dataset.name == "genebuild":
    #                 dataset_test = session.query(Dataset).filter(Dataset.name == "genebuild",
    #                                                              Dataset.version == dataset.version,
    #                                                              Dataset.label == dataset.label).first()
    #                 dataset.dataset_type = session.query(DatasetType).filter(
    #                     DatasetType.name == "genebuild").first()
    #                 dataset.dataset_source = self.dataset_source
    #             temp_genome_dataset = GenomeDataset(
    #                 genome_dataset_id=None,  # Should be autogenerated upon insertion
    #                 dataset_id=None,  # extract from dataset once genertated
    #                 genome_id=None,  # extract from genome once genertated
    #                 release_id=None,  # extract from release once genertated
    #                 is_current=0,
    #             )
    #             temp_genome_dataset.dataset = dataset
    #             self.genome.genome_datasets.append(temp_genome_dataset)
    #         # Add everything to the database. Closing the session commits it.
    #         session.add(self.genome)
    #
    # # The following methods populate the data from the core into the objects. K
    # # It may be beneficial to move them to the base class with later implementations
    # def new_organism(self):
    #     # All taken from the meta table except parlance name.
    #     self.organism = Organism(
    #         organism_id=None,  # Should be autogenerated upon insertion
    #         species_taxonomy_id=self.get_meta_single_meta_key(self.species, "species.species_taxonomy_id"),
    #         taxonomy_id=self.get_meta_single_meta_key(self.species, "species.taxonomy_id"),
    #         display_name=self.get_meta_single_meta_key(self.species, "species.display_name"),
    #         scientific_name=self.get_meta_single_meta_key(self.species, "species.scientific_name"),
    #         url_name=self.get_meta_single_meta_key(self.species, "species.url"),
    #         ensembl_name=self.get_meta_single_meta_key(self.species, "species.production_name"),
    #         strain=self.get_meta_single_meta_key(self.species, "species.strain"),
    #         scientific_parlance_name=None,
    #     )
    #     if self.organism.species_taxonomy_id is None:
    #         self.organism.species_taxonomy_id = self.organism.taxonomy_id
    #
    # def new_organism_group_and_members(self, session):
    #     # This method auto grabs the division name and checks for the strain groups
    #     division_name = self.get_meta_single_meta_key(self.species, "species.division")
    #     if division_name is None:
    #         Exception("No species.dvision found in meta table")
    #     division = session.execute(db.select(OrganismGroup).filter(OrganismGroup.name == division_name)).one_or_none()
    #     if division is None:
    #         group = OrganismGroup(
    #             organism_group_id=None,
    #             type="Division",
    #             name=division_name,
    #             code=None,
    #         )
    #     else:
    #         group = session.query(OrganismGroup).filter(OrganismGroup.name == division_name).first()
    #     self.organism_group_member = OrganismGroupMember(
    #         organism_group_member_id=None,
    #         is_reference=0,
    #         organism_id=None,
    #         organism_group_id=None,
    #     )
    #     self.organism_group_member.organism_group = group
    #     self.organism_group_member.organism = self.organism
    #
    #     # Work on the strain level group members
    #     strain = self.get_meta_single_meta_key(self.species, "species.strain")
    #     strain_group = self.get_meta_single_meta_key(self.species, "species.strain_group")
    #     strain_type = self.get_meta_single_meta_key(self.species, "species.type")
    #
    #     if strain is not None:
    #         if strain == 'reference':
    #             reference = 1
    #         else:
    #             reference = 0
    #         group_member = OrganismGroupMember(
    #             organism_group_member_id=None,
    #             is_reference=reference,
    #             organism_id=None,
    #             organism_group_id=None,
    #         )
    #         # Check for group, if not present make it
    #         division = session.execute(
    #             db.select(OrganismGroup).filter(OrganismGroup.name == strain_group)).one_or_none()
    #         if division is None:
    #             group = OrganismGroup(
    #                 organism_group_id=None,
    #                 type=strain_type,
    #                 name=strain_group,
    #                 code=None,
    #             )
    #
    #         else:
    #             group = session.query(OrganismGroup).filter(OrganismGroup.name == strain_group).first()
    #             group_member.organism_group = group
    #             group_member.organism = self.organism
    #
    # def new_genome(self):
    #     # Data for the update function.
    #     self.genome = Genome(
    #         genome_id=None,  # Should be autogenerated upon insertion
    #         genome_uuid=str(uuid.uuid4()),
    #         assembly_id=None,  # Update the assembly before inserting and grab the assembly key
    #         organism_id=None,  # Update the organism before inserting and grab the organism_id
    #         created=func.now(),  # Replace all of them with sqlalchemy func.now()
    #     )
    #
    # def new_genome_release(self):
    #     # Genome Release
    #     self.genome_release = GenomeRelease(
    #         genome_release_id=None,  # Should be autogenerated upon insertion
    #         genome_id=None,  # Update the genome before inserting and grab the genome_id
    #         release_id=None,
    #         is_current=self.listed_release_is_current,
    #     )
    #
    # def new_assembly(self):
    #     level = None
    #     with self.db.session_scope() as session:
    #         level = (session.execute(db.select(CoordSystem.name).filter(
    #             CoordSystem.species_id == self.species).order_by(CoordSystem.rank)).all())[0][0]
    #
    #     self.assembly = Assembly(
    #         assembly_id=None,  # Should be autogenerated upon insertion
    #         ucsc_name=self.get_meta_single_meta_key(self.species, "assembly.ucsc_alias"),
    #         accession=self.get_meta_single_meta_key(self.species, "assembly.accession"),
    #         level=level,
    #         name=self.get_meta_single_meta_key(self.species, "assembly.name"),
    #         accession_body=None,  # Not implemented yet
    #         assembly_default=self.get_meta_single_meta_key(self.species, "assembly.default"),
    #         created=func.now(),
    #         ensembl_name=self.get_meta_single_meta_key(self.species, "assembly.name"),
    #     )

    # def new_assembly_dataset(self):
    #     self.assembly_dataset = Dataset(
    #         dataset_id=None,  # Should be autogenerated upon insertion
    #         dataset_uuid=str(uuid.uuid4()),
    #         dataset_type_id=None,  # extract from dataset_type
    #         name="assembly",
    #         version=None,
    #         created=func.now(),
    #         dataset_source_id=None,  # extract from dataset_source
    #         label=self.assembly.accession,
    #         status='Submitted',
    #     )
    #
    # def new_assembly_sequence(self):
    #     self.assembly_sequences = []
    #     with self.db.session_scope() as session:
    #         # Alias the seq_region_attrib and seq_region_synonym tables
    #         sra1 = aliased(SeqRegionAttrib)
    #         sra3 = aliased(SeqRegionAttrib)
    #
    #         results = (
    #             session.query(SeqRegion.name, SeqRegionSynonym.synonym, SeqRegion.length,
    #                           CoordSystem.name,
    #                           sra3.value,
    #                           )
    #             .join(CoordSystem, SeqRegion.coord_system_id == CoordSystem.coord_system_id)
    #             .join(Meta, CoordSystem.species_id == Meta.species_id)
    #             .join(sra1, SeqRegion.seq_region_id == sra1.seq_region_id)
    #             .outerjoin(SeqRegionSynonym, and_(SeqRegion.seq_region_id == SeqRegionSynonym.seq_region_id,
    #                                               SeqRegionSynonym.external_db_id == 50710))
    #             .outerjoin(sra3, and_(SeqRegion.seq_region_id == sra3.seq_region_id,
    #                                   sra3.attrib_type_id == 547))
    #             .filter(Meta.meta_key == 'assembly.accession', sra1.attrib_type_id == 6,
    #                     Meta.species_id == self.species)
    #         ).all()
    #     for data in results:
    #         # If the name does not match normal accession formating, then use that name.
    #         name = None
    #         if re.match(r'^[a-zA-Z]+\d+\.\d+', data[0]):
    #             name = None
    #         else:
    #             name = data[0]
    #         # Nab accession from the seq region synonym or else the name.
    #         accession = None
    #         if data[1] is not None and re.match(r'^[a-zA-Z]+\d+\.\d+', data[1]):
    #             accession = data[1]
    #         elif name is not None:
    #             accession = name
    #         else:
    #             accession = data[0]
    #
    #         chromosomal = 0
    #         if data[3] == 'chromosome':
    #             chromosomal = 1
    #
    #         sequence_location = None
    #         if data[4] == 'nuclear_chromosome':
    #             sequence_location = 'SO:0000738'
    #         elif data[4] == 'mitochondrial_chromosome':
    #             sequence_location = 'SO:0000737'
    #         elif data[4] == 'chloroplast_chromosome':
    #             sequence_location = 'SO:0000745'
    #         elif data[4] is None:
    #             sequence_location = 'SO:0000738'
    #         else:
    #             raise Exception('Error with sequence location: ' + data[4] + ' is not a valid type')
    #
    #         self.assembly_sequences.append(AssemblySequence(
    #             assembly_sequence_id=None,  # Should be autogenerated upon insertion
    #             name=name,
    #             assembly_id=None,  # Update the assembly before inserting and grab the assembly_id
    #             accession=accession,
    #             chromosomal=chromosomal,
    #             length=data[2],
    #             sequence_location=sequence_location,
    #             # These two get populated in the core stats pipeline.
    #             sequence_checksum=None,
    #             ga4gh_identifier=None,
    #         ))
    #
    # def new_genome_dataset(self):
    #     self.genome_dataset = GenomeDataset(
    #         genome_dataset_id=None,  # Should be autogenerated upon insertion
    #         dataset_id=None,  # extract from dataset once genertated
    #         genome_id=None,  # extract from genome once genertated
    #         release_id=None,  # extract from release once genertated
    #         is_current=self.listed_release_is_current,
    #     )
    #
    # def new_dataset_source(self):
    #     self.dataset_source = DatasetSource(
    #         dataset_source_id=None,  # Should be autogenerated upon insertion
    #         type=self.db_type,  # core/fungen etc
    #         name=make_url(self.db_uri).database  # dbname
    #     )
    #
    # def new_datasets(self):
    #     self.datasets = []
    #     # Genebuild.
    #     label = self.get_meta_single_meta_key(self.species, "genebuild.last_geneset_update")
    #     if label is None:
    #         label = self.get_meta_single_meta_key(self.species, "genebuild.start_date")
    #     self.datasets.append(Dataset(
    #         dataset_id=None,  # Should be autogenerated upon insertion
    #         dataset_uuid=str(uuid.uuid4()),
    #         dataset_type_id=None,  # extract from dataset_type
    #         name="genebuild",
    #         version=self.get_meta_single_meta_key(self.species, "gencode.version"),
    #         created=func.now(),
    #         dataset_source_id=None,  # extract from dataset_source
    #         label=label,
    #         status='Submitted',
    #     ))
    # Protein Features
