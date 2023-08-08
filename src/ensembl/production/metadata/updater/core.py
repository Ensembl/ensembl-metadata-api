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
import re
from collections import defaultdict
import sqlalchemy as db
from ensembl.core.models import Meta, CoordSystem, SeqRegionAttrib, SeqRegion, \
    SeqRegionSynonym, AttribType, ExternalDb
from sqlalchemy import select, func
from sqlalchemy.orm import aliased
from ensembl.database import DBConnection
from sqlalchemy.exc import NoResultFound
from ensembl.production.metadata.api.genome import GenomeAdaptor
from ensembl.production.metadata.api.dataset import DatasetAdaptor
from ensembl.production.metadata.api.models import *
from ensembl.production.metadata.updater.base import BaseMetaUpdater


class CoreMetaUpdater(BaseMetaUpdater):
    def __init__(self, db_uri, metadata_uri, taxonomy_uri):
        # Each of these objects represents a table in the database to store data in as an array or a single object.
        self.organism = None
        self.division = None
        self.organism_group_member = None
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
        db_uri = kwargs.get('db_uri', self.db_uri)
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
            self.process_species(species, metadata_uri, taxonomy_uri, db_uri)

    def process_species(self, species, metadata_uri, taxonomy_uri, db_uri):
        """
        Process an individual species from a core database to update the metadata db.
        This method contains the logic for updating the metadata
        """
        meta_conn = DBConnection(metadata_uri)
        with meta_conn.session_scope() as meta_session:
            self.organism, self.division, self.organism_group_member, organism_status = \
                self.get_or_new_organism(species, meta_session, metadata_uri, taxonomy_uri)
            self.assembly, self.assembly_dataset, self.assembly_dataset_attributes, self.assembly_sequences, \
                self.dataset_source, assembly_status = self.get_or_new_assembly(species, meta_session, db_uri)
            self.genebuild_dataset, self.genebuild_dataset_attributes, \
                genebuild_status = self.new_genebuild(species, meta_session, db_uri, self.dataset_source)

            conn = DatasetAdaptor(metadata_uri=metadata_uri)
            genebuild_release_status = conn.check_release_status(self.genebuild_dataset.dataset_uuid)

            if organism_status == "New":
                print ("New organism")
                # ###############################Checks that dataset and assembly are new ##################
                if assembly_status != "New" or genebuild_status != "New":
                    raise Exception("New organism, but existing assembly accession and/or genebuild version")
                ###############################################
                # Create genome and populate the database with organism, assembly and dataset
                new_genome, assembly_genome_dataset, genebuild_genome_dataset = self.new_genome(meta_session,
                                    self.organism, self.assembly, self.assembly_dataset, self.genebuild_dataset)

            elif assembly_status == "New":
                print ("New assembly")

                # ###############################Checks that dataset and update are new ##################
                if genebuild_status != "New":
                    raise Exception("New assembly, but existing genebuild version")
                ###############################################

                new_genome, assembly_genome_dataset, genebuild_genome_dataset = self.new_genome(meta_session,
                                    self.organism, self.assembly, self.assembly_dataset, self.genebuild_dataset)

                # Create genome and populate the database with assembly and dataset
            elif genebuild_status == "New":
                print ("New genebuild")

                # Create genome and populate the database with genebuild dataset
                new_genome, assembly_genome_dataset, genebuild_genome_dataset = self.new_genome(meta_session,
                                    self.organism, self.assembly, self.assembly_dataset, self.genebuild_dataset)
            else:
                # Check if the data has been released:
                if genebuild_release_status is True:
                    raise Exception("Existing Organism, Assembly, and Datasets within a release")
                else:
                    print("Rewrite")
                    #Need to do a rewrite, so that it only redoes the geneset data.

                    # Delete the data from the database and repopulate assembly and genebuild.
                    genome_dataset = meta_session.query(GenomeDataset).join(Dataset).filter(
                        Dataset.dataset_uuid == self.assembly_dataset.dataset_uuid).first()
                    current_genome = meta_session.query(Genome).get(genome_dataset.genome_id)
                    for d in meta_session.query(Dataset).join(GenomeDataset).filter(
                            GenomeDataset.genome_id == current_genome.genome_id).filter(Dataset.name == "genebuild"):
                        meta_session.delete(d)
                    meta_session.commit()
                    meta_session.flush()
                    genebuild_genome_dataset = GenomeDataset(
                        genome=current_genome,
                        dataset=self.genebuild_dataset,
                        is_current=True,
                    )
                    meta_session.add(genebuild_genome_dataset)


    def new_genome(self, meta_session, organism, assembly, assembly_dataset, genebuild_dataset):
        new_genome = Genome(
            genome_uuid=str(uuid.uuid4()),
            assembly=assembly,
            organism=organism,
            created=func.now(),
        )
        meta_session.add(new_genome)
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
        return new_genome, assembly_genome_dataset, genebuild_genome_dataset

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
            species_taxonomy_id=self.get_meta_single_meta_key(species, "species.species_taxonomy_id"),
            taxonomy_id=self.get_meta_single_meta_key(species, "species.taxonomy_id"),
            display_name=self.get_meta_single_meta_key(species, "species.display_name"),
            scientific_name=self.get_meta_single_meta_key(species, "species.scientific_name"),
            url_name=self.get_meta_single_meta_key(species, "species.url"),
            ensembl_name=ensembl_name,
            strain=self.get_meta_single_meta_key(species, "species.strain"),
            #
        )

        # Query the metadata database to find if an Organism with the same Ensembl name already exists.
        old_organism = meta_session.query(Organism).filter(
            Organism.ensembl_name == new_organism.ensembl_name).one_or_none()
        division_name = self.get_meta_single_meta_key(species, "species.division")
        division = meta_session.query(OrganismGroup).filter(OrganismGroup.name == division_name).one_or_none()

        # If an existing Organism is found, return it and indicate that it already existed.
        if old_organism:
            organism_group_member = meta_session.query(OrganismGroupMember).filter(
                OrganismGroupMember.organism_id == old_organism.organism_id,
                OrganismGroupMember.organism_group_id == division.organism_group_id).one_or_none()
            return old_organism, division, organism_group_member, "Existing"
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
            if division_name is None:
                Exception("No species.division found in meta table")

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
            return new_organism, division, organism_group_member, "New"

    def get_assembly_sequences(self, species, assembly):
        """
        Get the assembly sequences and the values that correspond to the metadata table
        """
        assembly_sequences = []
        with self.db.session_scope() as session:
            # Create an alias for SeqRegionAttrib and AttribType to be used for sequence_location
            SeqRegionAttribAlias = aliased(SeqRegionAttrib)
            AttribTypeAlias = aliased(AttribType)

            # One complicated query to get all the data. Otherwise, this takes far too long to do.
            results = (session.query(SeqRegion.name, SeqRegion.length, CoordSystem.name,
                                     SeqRegionAttribAlias.value, SeqRegionSynonym.synonym, ExternalDb.db_name)
                       .join(SeqRegion.coord_system)
                       .join(SeqRegion.seq_region_attrib)
                       .join(SeqRegionAttrib.attrib_type)
                       .outerjoin(SeqRegion.seq_region_synonym)
                       .outerjoin(SeqRegionSynonym.external_db)
                       .join(SeqRegionAttribAlias, SeqRegion.seq_region_attrib)  # join with SeqRegionAttribAlias
                       .outerjoin(AttribTypeAlias, SeqRegionAttribAlias.attrib_type)  # join with AttribTypeAlias
                       .filter(CoordSystem.species_id == species)
                       .filter(AttribType.code == "toplevel")  # ensure toplevel
                       .filter(AttribTypeAlias.code == "sequence_location").all())  # ensure sequence_location

            # Create a dictionary so that the results can have multiple synonyms per line and only one SeqRegion
            accession_info = defaultdict(
                lambda: {"names": set(), "length": None, "location": None, "chromosomal": None})

            for seq_region_name, seq_region_length, coord_system_name, location, synonym, db_name in results:

                # Test to see if the seq_name follows accession standards (99% of sequences)
                if re.match(r'^[a-zA-Z]+\d+\.\d+', seq_region_name):
                    # If so assign it to accession
                    accession = seq_region_name
                    if not synonym:
                        # If it doesn't have any synonyms the accession is the name.
                        accession_info[accession]["names"].add(accession)
                    else:
                        accession_info[accession]["names"].add(synonym)
                else:
                    # For named sequences like chr1
                    name = seq_region_name
                    if re.match(r'^[a-zA-Z]+\d+\.\d+', synonym):
                        accession = synonym
                        accession_info[accession]["names"].add(name)
                    else:
                        accession = name  # In case synonym doesn't match the pattern, use the name as the accession
                        accession_info[accession]["names"].add(synonym if synonym else name)

                # Save the sequence location, length, and chromosomal flag.
                location_mapping = {
                    'nuclear_chromosome': 'SO:0000738',
                    'mitochondrial_chromosome': 'SO:0000737',
                    'chloroplast_chromosome': 'SO:0000745',
                    None: 'SO:0000738',
                }

                try:
                    sequence_location = location_mapping[location]
                except KeyError:
                    raise Exception('Error with sequence location: {} is not a valid type'.format(location))

                # Test if chromosomal:
                if coord_system_name == "chromosome":
                    chromosomal = 1
                else:
                    chromosomal = 0

                # Assign the values to the dictionary
                accession_info[accession]["location"] = sequence_location
                accession_info[accession]["chromosomal"] = chromosomal
                accession_info[accession]["length"] = seq_region_length

            # Now, create AssemblySequence objects for each unique accession.
            for accession, info in accession_info.items():
                # Combine all unique names with ";". If a name appears in multiple sequences with the same accession,
                name = ";".join(info["names"])

                # Create an AssemblySequence object.
                assembly_sequence = AssemblySequence(
                    name=name,
                    assembly=assembly,
                    accession=accession,
                    chromosomal=info["chromosomal"],
                    length=info["length"],
                    sequence_location=info["location"],
                    # sequence_checksum="", Not implemented
                    # ga4gh_identifier="", Not implemented
                )

                assembly_sequences.append(assembly_sequence)
        return assembly_sequences

    def get_or_new_assembly(self, species, meta_session, db_uri, source=None):
        # Get the new assembly accession  from the core handed over
        assembly_accession = self.get_meta_single_meta_key(species, "assembly.accession")
        assembly = meta_session.query(Assembly).filter(Assembly.accession == assembly_accession).one_or_none()

        if assembly is not None:
            # Get the existing assembly dataset
            assembly_dataset = meta_session.query(Dataset).filter(Dataset.label == assembly_accession).one_or_none()
            # I should not need this, but double check on database updating.
            assembly_dataset_attributes = assembly_dataset.dataset_attributes
            assembly_sequences = assembly.assembly_sequences
            if source is not None:
                dataset_source, source_status = self.get_or_new_source(meta_session, db_uri, "core")
            else:
                dataset_source = source

            return assembly, assembly_dataset, assembly_dataset_attributes, \
                assembly_sequences, dataset_source, "Existing"

        else:
            with self.db.session_scope() as session:
                # May be problematic. Might be provided by genebuild.
                level = (session.execute(db.select(CoordSystem.name).filter(
                    CoordSystem.species_id == species).order_by(CoordSystem.rank)).all())[0][0]
            assembly = Assembly(
                ucsc_name=self.get_meta_single_meta_key(species, "assembly.ucsc_alias"),
                accession=self.get_meta_single_meta_key(species, "assembly.accession"),
                level=level,
                # level=self.get_meta_single_meta_key(self.species, "assembly.level"),   #Not yet implemented.
                name=self.get_meta_single_meta_key(species, "assembly.name"),
                accession_body=self.get_meta_single_meta_key(species, "assembly.provider"),
                assembly_default=self.get_meta_single_meta_key(species, "assembly.default"),
                tol_id=self.get_meta_single_meta_key(species, "assembly.tol_id"),  # Not implemented yet
                created=func.now(),
                ensembl_name=self.get_meta_single_meta_key(species, "assembly.name"),
                assembly_uuid=str(uuid.uuid4()),
            )
            dataset_type = meta_session.query(DatasetType).filter(DatasetType.name == "assembly").first()
            if source is None:
                dataset_source, source_status = self.get_or_new_source(meta_session, db_uri, "core")
            else:
                dataset_source = source

            assembly_dataset = Dataset(
                dataset_uuid=str(uuid.uuid4()),
                dataset_type=dataset_type,  # extract from dataset_type
                name="assembly",
                # version=None, Could be changed.
                label=assembly.accession,  # Required. Makes for a quick lookup
                created=func.now(),
                dataset_source=dataset_source,  # extract from dataset_source
                status='Submitted',
            )
            attributes = self.get_meta_list_from_prefix_meta_key(species, "assembly")
            assembly_dataset_attributes = []
            for attribute, value in attributes.items():
                meta_attribute = meta_session.query(Attribute).filter(Attribute.name == attribute).one_or_none()
                if meta_attribute is None:
                    raise Exception(f"Attribute {attribute} not found. Please enter it into the db manually")
                dataset_attribute = DatasetAttribute(
                    value=value,
                    dataset=assembly_dataset,
                    attribute=meta_attribute,
                )
                assembly_dataset_attributes.append(dataset_attribute)
            assembly_sequences = self.get_assembly_sequences(species, assembly)
            meta_session.add(assembly)
            meta_session.add_all(assembly_sequences)
            meta_session.add(assembly_dataset)
            meta_session.add_all(assembly_dataset_attributes)
            return assembly, assembly_dataset, assembly_dataset_attributes, assembly_sequences, dataset_source, "New"

    def new_genebuild(self, species, meta_session, db_uri, source=None):
        """
        Process an individual species from a core database to update the metadata db.
        This method contains the logic for updating the metadata
        This is not a get, as we don't update the metadata for genebuild, only replace it if it is not released.
        """
        # The assembly accession and genebuild version are extracted from the metadata of the species
        assembly_accession = self.get_meta_single_meta_key(species, "assembly.accession")
        genebuild_version = self.get_meta_single_meta_key(species, "genebuild.version")

        # The genebuild accession is formed by combining the assembly accession and the genebuild version
        genebuild_accession = assembly_accession + "_" + genebuild_version

        # Depending on whether a source is provided, it uses the provided source or creates a new source
        if source is None:
            dataset_source, source_status = self.get_or_new_source(meta_session, db_uri, "core")
        else:
            dataset_source = source

        # The type of the dataset is set to be "genebuild"
        dataset_type = meta_session.query(DatasetType).filter(DatasetType.name == "genebuild").first()

        # A new Dataset instance is created with all necessary properties
        genebuild_dataset = Dataset(
            dataset_uuid=str(uuid.uuid4()),
            dataset_type=dataset_type,
            name="genebuild",
            version=genebuild_version,
            label=genebuild_accession,
            created=func.now(),
            dataset_source=dataset_source,
            status='Submitted',
        )

        # Fetching all attributes associated with "genebuild" from the metadata of the species
        attributes = self.get_meta_list_from_prefix_meta_key(species, "genebuild.")

        # An empty list to hold DatasetAttribute instances
        genebuild_dataset_attributes = []
        # For each attribute-value pair, a new DatasetAttribute instance is created and added to the list
        for attribute, value in attributes.items():
            meta_attribute = meta_session.query(Attribute).filter(Attribute.name == attribute).one_or_none()
            if meta_attribute is None:
                raise Exception(f"Attribute {attribute} not found. Please enter it into the db manually")
            dataset_attribute = DatasetAttribute(
                value=value,
                dataset=genebuild_dataset,
                attribute=meta_attribute,
            )
            genebuild_dataset_attributes.append(dataset_attribute)

        # Check if the genebuild dataset with the given label already exists
        test_status = meta_session.query(Dataset).filter(Dataset.label == genebuild_accession).one_or_none()

        # If it does not exist, it is added to the session, otherwise the status is set to "Existing"
        if test_status is None:
            status = "New"
            meta_session.add(genebuild_dataset)
            meta_session.add_all(genebuild_dataset_attributes)
        else:
            status = "Existing"

        # The method returns the Dataset instance, the list of DatasetAttribute instances, and the status
        return genebuild_dataset, genebuild_dataset_attributes, status
