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
    SeqRegionSynonym, AttribType
from sqlalchemy import select
from sqlalchemy import or_
from ensembl.database import DBConnection
from sqlalchemy.exc import NoResultFound
from ensembl.production.metadata.api.models import *
from ensembl.production.metadata.updater.base import BaseMetaUpdater
from ensembl.ncbi_taxonomy.api.utils import Taxonomy
from ensembl.ncbi_taxonomy.models import NCBITaxaName
import logging

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

    def process_species(self, species_id, metadata_uri, taxonomy_uri, db_uri):
        """
        Process an individual species from a core database to update the metadata db.
        This method contains the logic for updating the metadata
        """
        meta_conn = DBConnection(metadata_uri)
        with meta_conn.session_scope() as meta_session:
            self.organism, self.division, self.organism_group_member, organism_status = \
                self.get_or_new_organism(species_id, meta_session, metadata_uri, taxonomy_uri)
            self.assembly, self.assembly_dataset, self.assembly_dataset_attributes, self.assembly_sequences, \
                self.dataset_source, assembly_status = self.get_or_new_assembly(species_id, meta_session, db_uri)
            self.genebuild_dataset, self.genebuild_dataset_attributes, \
                genebuild_status = self.new_genebuild(species_id, meta_session, db_uri, self.dataset_source)

            genebuild_release_status = check_release_status(DBConnection(metadata_uri), self.genebuild_dataset.dataset_uuid)

            if organism_status == "New":
                logging.info('New organism')
                # ###############################Checks that dataset and assembly are new ##################
                if assembly_status != "New" or genebuild_status != "New":
                    raise Exception("New organism, but existing assembly accession and/or genebuild version")
                ###############################################
                # Create genome and populate the database with organism, assembly and dataset
                new_genome, assembly_genome_dataset, genebuild_genome_dataset = self.new_genome(meta_session,
                                                                                                self.organism,
                                                                                                self.assembly,
                                                                                                self.assembly_dataset,
                                                                                                self.genebuild_dataset)

            elif assembly_status == "New":
                logging.info('New assembly')

                # ###############################Checks that dataset and update are new ##################
                if genebuild_status != "New":
                    raise Exception("New assembly, but existing genebuild version")
                ###############################################

                new_genome, assembly_genome_dataset, genebuild_genome_dataset = self.new_genome(meta_session,
                                                                                                self.organism,
                                                                                                self.assembly,
                                                                                                self.assembly_dataset,
                                                                                                self.genebuild_dataset)

                # Create genome and populate the database with assembly and dataset
            elif genebuild_status == "New":
                logging.info('New genebuild')

                # Create genome and populate the database with genebuild dataset
                new_genome, assembly_genome_dataset, genebuild_genome_dataset = self.new_genome(meta_session,
                                                                                                self.organism,
                                                                                                self.assembly,
                                                                                                self.assembly_dataset,
                                                                                                self.genebuild_dataset)
            else:
                # Check if the data has been released:
                if genebuild_release_status is True:
                    raise Exception("Existing Organism, Assembly, and Datasets within a release")
                else:
                    logging.info('Rewrite of existing data')
                    # Need to do a rewrite, so that it only redoes the geneset data.

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
            is_best=0,
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

    def get_or_new_organism(self, species_id, meta_session, metadata_uri, taxonomy_uri):
        """
        Get an existing Organism instance or create a new one, depending on the information from the metadata database.
        """
        tdbc = DBConnection(taxonomy_uri)
        # Fetch the Ensembl name of the organism from metadata using either 'species.ensembl_name'
        # or 'species.production_name' as the key.
        ensembl_name = self.get_meta_single_meta_key(species_id, "organism.ensembl_name")
        if ensembl_name is None:
            ensembl_name = self.get_meta_single_meta_key(species_id, "species.production_name")

        # Getting the common name from the meta table, otherwise we grab it from ncbi.
        common_name = self.get_meta_single_meta_key(species_id, "species.common_name")
        if common_name is None:
            taxid = self.get_meta_single_meta_key(species_id, "species.taxonomy_id")

            with tdbc.session_scope() as session:
                common_name = session.query(NCBITaxaName).filter(
                    NCBITaxaName.taxon_id == taxid,
                    NCBITaxaName.name_class == "genbank common name"
                ).one_or_none().name
            common_name = common_name if common_name is not None else '-'
        # Instantiate a new Organism object using data fetched from metadata.
        new_organism = Organism(
            species_taxonomy_id=self.get_meta_single_meta_key(species_id, "species.species_taxonomy_id"),
            taxonomy_id=self.get_meta_single_meta_key(species_id, "species.taxonomy_id"),
            common_name=common_name,
            scientific_name=self.get_meta_single_meta_key(species_id, "species.scientific_name"),
            ensembl_name=ensembl_name,
            strain=self.get_meta_single_meta_key(species_id, "species.strain"),
            strain_type=self.get_meta_single_meta_key(species_id, "strain.type"),
            scientific_parlance_name=self.get_meta_single_meta_key(species_id, "species.parlance_name")
        )

        # Query the metadata database to find if an Organism with the same Ensembl name already exists.
        old_organism = meta_session.query(Organism).filter(
            Organism.ensembl_name == new_organism.ensembl_name).one_or_none()
        division_name = self.get_meta_single_meta_key(species_id, "species.division")
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
            with tdbc.session_scope() as session:
                try:
                    Taxonomy.fetch_node_by_id(session, new_organism.taxonomy_id)
                except NoResultFound:
                    raise RuntimeError(f"taxon id {new_organism.taxonomy_id} not found in taxonomy database for scientific name")

            # Check if an Assembly with the same accession already exists in the metadata database.
            accession = self.get_meta_single_meta_key(species_id, "assembly.accession")
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

    def get_assembly_sequences(self, species_id, assembly):
        """
        Get the assembly sequences and the values that correspond to the metadata table
        """
        assembly_sequences = []
        with self.db.session_scope() as session:

            results = (session.query(SeqRegion.name, SeqRegion.length, CoordSystem.name, SeqRegionSynonym.synonym)
                       .join(SeqRegion.coord_system)
                       .outerjoin(SeqRegionSynonym, SeqRegionSynonym.seq_region_id == SeqRegion.seq_region_id)
                       .join(SeqRegion.seq_region_attrib)
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

            # Create a dictionary so that the results can have multiple synonyms per line and only one SeqRegion
            accession_info = defaultdict(
                lambda: {"names": set(), "accession": None, "length": None, "location": None, "chromosomal": None,
                         "karyotype_rank": None})

            for seq_region_name, seq_region_length, coord_system_name, synonym in results:
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
            # Now, create AssemblySequence objects for each unique accession.
            for accession, info in accession_info.items():
                seq_region_name = accession
                accession_pattern = r'^[a-zA-Z]{2}\d+\.\d+'
                names = info["names"]
                if not names:
                    name = accession
                else:
                    # Sort names based on whether they contain a period.
                    # Names without a period will come first.
                    sorted_names = sorted(names, key=lambda x: ('.' in x, x))
                    preferred_name = sorted_names[0]

                    if re.match(accession_pattern, accession):
                        name = preferred_name
                    else:
                        names.add(accession)
                        matching_accessions = [temp for temp in names if re.match(accession_pattern, temp)]

                        accession = matching_accessions[0] if matching_accessions else accession
                        name = preferred_name

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
                )

                assembly_sequences.append(assembly_sequence)
        return assembly_sequences

    def get_or_new_assembly(self, species_id, meta_session, db_uri, source=None):
        # Get the new assembly accession  from the core handed over
        assembly_accession = self.get_meta_single_meta_key(species_id, "assembly.accession")
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
            is_reference = 1 if self.get_meta_single_meta_key(species_id, "assembly.is_reference") else 0
            with self.db.session_scope() as session:
                # May be problematic.
                # Ideally this should be done through karyotype rank, however using coord system was more efficent
                # and upon testing 100 databases, it was found to be accurate in every case.
                # Leaving it until told otherwise.
                level = (session.execute(db.select(CoordSystem.name).filter(
                    CoordSystem.species_id == species_id).order_by(CoordSystem.rank)).all())[0][0]
                tol_id = self.get_meta_single_meta_key(species_id, "assembly.tol_id")
                if tol_id is None:
                    tol_id = self.get_meta_single_meta_key(species_id, "assembly.tolid")

            assembly = Assembly(
                ucsc_name=self.get_meta_single_meta_key(species_id, "assembly.ucsc_alias"),
                accession=self.get_meta_single_meta_key(species_id, "assembly.accession"),
                level=level,
                name=self.get_meta_single_meta_key(species_id, "assembly.name"),
                accession_body=self.get_meta_single_meta_key(species_id, "assembly.provider"),
                assembly_default=self.get_meta_single_meta_key(species_id, "assembly.default"),
                tol_id=tol_id,
                created=func.now(),
                ensembl_name=self.get_meta_single_meta_key(species_id, "assembly.name"),
                assembly_uuid=str(uuid.uuid4()),
                url_name=self.get_meta_single_meta_key(species_id, "assembly.url_name"),
                is_reference=is_reference

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
            attributes = self.get_meta_list_from_prefix_meta_key(species_id, "assembly")
            assembly_dataset_attributes = []
            for attribute, value in attributes.items():
                meta_attribute = meta_session.query(Attribute).filter(Attribute.name == attribute).one_or_none()
                if meta_attribute is None:
                    meta_attribute = Attribute(
                        name=attribute,
                        label=attribute,
                        description=attribute,
                        type="string",
                    )
                dataset_attribute = DatasetAttribute(
                    value=value,
                    dataset=assembly_dataset,
                    attribute=meta_attribute,
                )
                assembly_dataset_attributes.append(dataset_attribute)
            assembly_sequences = self.get_assembly_sequences(species_id, assembly)
            meta_session.add(assembly)
            meta_session.add_all(assembly_sequences)
            meta_session.add(assembly_dataset)
            meta_session.add_all(assembly_dataset_attributes)
            return assembly, assembly_dataset, assembly_dataset_attributes, assembly_sequences, dataset_source, "New"

    def new_genebuild(self, species_id, meta_session, db_uri, source=None):
        """
        Process an individual species from a core database to update the metadata db.
        This method contains the logic for updating the metadata
        This is not a get, as we don't update the metadata for genebuild, only replace it if it is not released.
        """
        # The assembly accession and genebuild version are extracted from the metadata of the species
        assembly_accession = self.get_meta_single_meta_key(species_id, "assembly.accession")
        genebuild_version = self.get_meta_single_meta_key(species_id, "genebuild.version")
        if genebuild_version is None:
            raise Exception(f"genebuild.version is required in the core database")
        # Test if sample_gene is present.

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
        attributes = self.get_meta_list_from_prefix_meta_key(species_id, "genebuild.")

        # An empty list to hold DatasetAttribute instances
        genebuild_dataset_attributes = []
        # For each attribute-value pair, a new DatasetAttribute instance is created and added to the list
        for attribute, value in attributes.items():
            meta_attribute = meta_session.query(Attribute).filter(Attribute.name == attribute).one_or_none()
            if meta_attribute is None:
                meta_attribute = Attribute(
                    name=attribute,
                    label=attribute,
                    description=attribute,
                    type="string",
                )
            dataset_attribute = DatasetAttribute(
                value=value,
                dataset=genebuild_dataset,
                attribute=meta_attribute,
            )
            genebuild_dataset_attributes.append(dataset_attribute)

        # Grab the necessary sample data and add it as an datasetattribute
        gene_param_attribute=meta_session.query(Attribute).filter(Attribute.name == "sample.gene_param").one_or_none()
        if gene_param_attribute is None:
            gene_param_attribute = Attribute(
                name="sample.gene_param",
                label="sample.gene_param",
                description="Sample Gene Data",
                type="string",
            )
        sample_gene_param = DatasetAttribute(
            value=self.get_meta_single_meta_key(species_id, "sample.gene_param"),
            dataset=genebuild_dataset,
            attribute=gene_param_attribute,
        )
        genebuild_dataset_attributes.append(sample_gene_param)
        sample_location_attribute=meta_session.query(Attribute).filter(Attribute.name == "sample.location_param").one_or_none()
        if sample_location_attribute is None:
            sample_location_attribute = Attribute(
                name="sample.location_param",
                label="sample.location_param",
                description="Sample Location Data",
                type="string",
            )
        sample_location_param = DatasetAttribute(
            value=self.get_meta_single_meta_key(species_id, "sample.location_param"),
            dataset=genebuild_dataset,
            attribute=sample_location_attribute,
        )
        genebuild_dataset_attributes.append(sample_location_param)
        # Add the production name:
        production_name_attribute = meta_session.query(Attribute).filter(
            Attribute.name == "production.production_name").one_or_none()
        if production_name_attribute is None:
            production_name_attribute = Attribute(
                name="production.production_name",
                label="Internal Production Name",
                description="Backward compatibility for registry production Name",
                type="string",
            )
        production_name = DatasetAttribute(
            value=self.get_meta_single_meta_key(species_id, "species.production_name"),
            dataset=genebuild_dataset,
            attribute=production_name_attribute,
        )
        genebuild_dataset_attributes.append(production_name)
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
