import logging
import re
import uuid

from ensembl.core.models import Meta, Assembly, CoordSystem, SeqRegionAttrib, SeqRegion, SeqRegionSynonym
from sqlalchemy import select, update, func, and_
from sqlalchemy.engine import make_url
from sqlalchemy.orm import aliased

import ensembl.production
from ensembl.production.metadata.updater.base import BaseMetaUpdater


class CoreMetaUpdater(BaseMetaUpdater):
    def __init__(self, db_uri, metadata_uri, release=None):
        # Each of these objects represents a table in the database to store data in as either an array or a single object.
        self.organism = None
        self.organism_group_member = None
        self.organism_group = None

        self.assembly = None
        self.assembly_sequences = None  # array
        self.assembly_dataset = None
        self.genome = None
        self.genome_release = None

        self.genome_dataset = None
        self.datasets = None  # array
        self.dataset_type = None
        self.dataset_source = None
        self.dataset_attribute = None
        self.attribute = None

        super().__init__(db_uri, metadata_uri, release)
        self.db_type = 'core'

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
            # Handle multispecies databases and run an update for each species
            with self.db.session_scope() as session:
                multi_species = session.execute(
                    select(Meta.species_id).filter(Meta.meta_key == "species.production_name").distinct()
                )
        multi_species = [multi_species for multi_species, in multi_species]

        for species in multi_species:
            self.species = species
            self.process_species()

    def process_species(self):

        # Each class that is called here extracts unlinked data from the submission database to use in comparisons and to
        # populate the new values if possible.
        self.new_organism()
        self.new_genome()
        self.new_genome_release()
        self.new_assembly()
        self.new_assembly_sequence()
        self.new_assembly_dataset()
        self.new_dataset_source()
        self.new_genome_dataset()
        self.new_datasets()

        #################
        # Transactions are committed once per program run.
        # Failures prevent any commit
        #################

        # Species Check
        # Check for new species by checking if ensembl name is already present in the database
        if GenomeAdaptor(metadata_uri=self.metadata_db.url).fetch_genomes_by_ensembl_name(
                self.organism.ensembl_name) == []:
            # Check if the assembly accesion is already present in the database
            new_assembly_acc = self.get_meta_single_meta_key(self.species, "assembly.accession")
            with self.metadata_db.session_scope() as session:
                if session.query(session.query(Assembly).filter_by(accession=new_assembly_acc).exists()).scalar():
                    Exception("Assembly Accession already exists for a different organism. Please do a manual update.")
            self.create_organism()
            logging.info("Fresh Organism. Adding data to organism, genome, genome_release,"
                         " assembly, assembly_sequence, dataset, dataset source, and genome_dataset tables.")

            # Check to see if it is an updated organism.
        else:
            with self.metadata_db.session_scope() as session:
                session.expire_on_commit = False
                test_organism = session.execute(db.select(Organism).filter(
                    Organism.ensembl_name == self.organism.ensembl_name)).one_or_none()
            self.organism.organism_id = ensembl.production.metadata.models.organism.Organism.organism_id
            self.organism.scientific_parlance_name = ensembl.production.metadata.models.organism.Organism.scientific_parlance_name

            if int(ensembl.production.metadata.models.organism.Organism.species_taxonomy_id) == int(self.organism.species_taxonomy_id) and \
                    int(ensembl.production.metadata.models.organism.Organism.taxonomy_id) == int(self.organism.taxonomy_id) and \
                    str(ensembl.production.metadata.models.organism.Organism.display_name) == str(self.organism.display_name) and \
                    str(ensembl.production.metadata.models.organism.Organism.scientific_name) == str(self.organism.scientific_name) and \
                    str(ensembl.production.metadata.models.organism.Organism.url_name) == str(self.organism.url_name) and \
                    str(ensembl.production.metadata.models.organism.Organism.strain) == str(self.organism.strain):
                logging.info("Old Organism with no change. No update to organism table")
                ################################################################
                ##### Assembly Check and Update
                ################################################################
                with self.metadata_db.session_scope() as session:
                    assembly_acc = session.execute(db.select(ensembl.production.metadata.models.assembly.Assembly
                                                             ).join(Genome.assembly).join(Genome.organism).filter(
                        Organism.ensembl_name == self.organism.ensembl_name)).all()
                    new_assembly_acc = self.get_meta_single_meta_key(self.species, "assembly.accession")
                    assembly_test = False
                    for assembly_obj in assembly_acc:
                        if assembly_obj[0].accession == new_assembly_acc:
                            assembly_test = True
                if assembly_test:
                    logging.info(
                        "Old Assembly with no change. No update to Genome, genome_release, assembly, and assembly_sequence tables.")
                    for dataset in self.datasets:
                        with self.metadata_db.session_scope() as session:
                            # Check to see if any already exist:
                            # for all of genebuild in dataset, see if any have the same label (genebuild.id) and version. If so, don't update and error out here!
                            if dataset.name == "genebuild":
                                dataset_test = session.query(Dataset).filter(Dataset.name == "genebuild",
                                                                             Dataset.version == dataset.version,
                                                                             Dataset.label == dataset.label).first()
                                if dataset_test is None:
                                    gb_dataset_type = session.query(DatasetType).filter(
                                        DatasetType.name == "genebuild").first()
                                    dataset.dataset_type = gb_dataset_type
                                    dataset.dataset_source = self.dataset_source
                                    session.add(dataset)

                else:
                    logging.info("New Assembly. Updating  genome, genome_release,"
                                 " assembly, assembly_sequence, dataset, dataset source, and genome_dataset tables.")
                    self.update_assembly()
                ################################################################
                ##### dataset Check and Update
                ################################################################
                # Dataset section. More logic will be necessary for additional datasets. Currently only the genebuild is listed here.




            else:
                self.update_organism()
                logging.info("Old Organism with changes. Updating organism table")

    def create_organism(self):
        # In this, we are assuming that with a new genome, there will be a new assemblbly.

        with self.metadata_db.session_scope() as session:
            # Organism section
            # Updating Organism, organism_group_member, and organism_group
            self.new_organism_group_and_members(session)
            # Add in the new assembly here
            # assembly sequence, assembly, genome, genome release.
            assembly_test = session.execute(db.select(ensembl.production.metadata.models.assembly.Assembly).filter(
                ensembl.production.metadata.models.assembly.Assembly.accession == self.assembly.accession)).one_or_none()
            if assembly_test is not None:
                Exception(
                    "Error, existing name but, assembly accession already found. Please update the Ensembl Name in the Meta field manually")
            if self.listed_release is not None:
                release = session.query(EnsemblRelease).filter(EnsemblRelease.release_id == self.listed_release).first()
                self.genome_release.ensembl_release = release
                self.genome_release.genome = self.genome

            for assembly_seq in self.assembly_sequences:
                assembly_seq.assembly = self.assembly
            self.assembly.genomes.append(self.genome)

            self.genome.organism = self.organism

            # Update assembly dataset
            # Updates genome_dataset,dataset,dataset_source
            dataset_source_test = session.execute(
                db.select(DatasetSource).filter(DatasetSource.name == self.dataset_source.name)).one_or_none()
            if dataset_source_test is not None:
                Exception("Error, data already present in source")

            dataset_type = session.query(DatasetType).filter(DatasetType.name == "assembly").first()
            if self.listed_release is not None:
                self.genome_dataset.ensembl_release = release
                self.genome_dataset.genome = self.genome
                self.genome_dataset.dataset = self.assembly_dataset

            self.assembly_dataset.dataset_type = dataset_type
            self.assembly_dataset.dataset_source = self.dataset_source

            assembly_genome_dataset = GenomeDataset(
                genome_dataset_id=None,  # Should be autogenerated upon insertion
                dataset_id=None,  # extract from dataset once genertated
                genome_id=None,  # extract from genome once genertated
                release_id=None,  # extract from release once genertated
                is_current=0,
            )
            assembly_genome_dataset.dataset = self.assembly_dataset
            self.genome.genome_datasets.append(assembly_genome_dataset)

            #        session.add(assembly_genome_dataset)

            # Dataset section. More logic will be necessary for additional datasets. Currently only the genebuild is listed here.
            for dataset in self.datasets:
                # Check to see if any already exist:
                # for all of genebuild in dataset, see if any have the same label (genebuild.id) and version. If so, don't update and error out here!
                if dataset.name == "genebuild":
                    dataset_test = session.query(Dataset).filter(Dataset.name == "genebuild",
                                                                 Dataset.version == dataset.version,
                                                                 Dataset.label == dataset.label).first()
                    if dataset_test is None:
                        dataset.dataset_type = session.query(DatasetType).filter(
                            DatasetType.name == "genebuild").first()
                        dataset.dataset_source = self.dataset_source
                temp_genome_dataset = GenomeDataset(
                    genome_dataset_id=None,  # Should be autogenerated upon insertion
                    dataset_id=None,  # extract from dataset once genertated
                    genome_id=None,  # extract from genome once genertated
                    release_id=None,  # extract from release once genertated
                    is_current=0,
                )
                temp_genome_dataset.dataset = dataset
                self.genome.genome_datasets.append(temp_genome_dataset)
            # Add everything to the database. Closing the session commits it.
            session.add(self.organism)

    def update_organism(self):
        with self.metadata_db.session_scope() as session:
            session.execute(
                update(Organism).where(Organism.ensembl_name == self.organism.ensembl_name).values(
                    species_taxonomy_id=self.organism.species_taxonomy_id,
                    taxonomy_id=self.organism.taxonomy_id,
                    display_name=self.organism.display_name,
                    scientific_name=self.organism.scientific_name,
                    url_name=self.organism.url_name,
                    ensembl_name=self.organism.ensembl_name,
                    strain=self.organism.strain,
                ))

            # TODO: Add an update to the groups here.

    def update_assembly(self):
        # Change to new assembly/fresh
        with self.metadata_db.session_scope() as session:
            # Get the genome
            self.organism = session.query(Organism).filter(
                Organism.ensembl_name == self.organism.ensembl_name).first()
            self.genome.organism = self.organism

            if self.listed_release is not None:
                release = session.query(EnsemblRelease).filter(EnsemblRelease.release_id == self.listed_release).first()
                self.genome_release.ensembl_release = release
                self.genome_release.genome = self.genome

            self.assembly.genomes.append(self.genome)

            # Update assembly dataset
            # Updates genome_dataset,dataset,dataset_source
            dataset_source_test = session.execute(
                db.select(DatasetSource).filter(DatasetSource.name == self.dataset_source.name)).one_or_none()
            if dataset_source_test is not None:
                self.dataset_source = session.query(DatasetSource).filter(
                    DatasetSource.name == self.dataset_source.name).first()

            dataset_type = session.query(DatasetType).filter(DatasetType.name == "assembly").first()
            if self.listed_release is not None:
                self.genome_dataset.ensembl_release = release
                self.genome_dataset.genome = self.genome
                self.genome_dataset.dataset = self.assembly_dataset

            self.assembly_dataset.dataset_type = dataset_type
            self.assembly_dataset.dataset_source = self.dataset_source

            assembly_genome_dataset = GenomeDataset(
                genome_dataset_id=None,  # Should be autogenerated upon insertion
                dataset_id=None,  # extract from dataset once genertated
                genome_id=None,  # extract from genome once genertated
                release_id=None,  # extract from release once genertated
                is_current=0,
            )
            assembly_genome_dataset.dataset = self.assembly_dataset
            self.genome.genome_datasets.append(assembly_genome_dataset)

            for dataset in self.datasets:
                # Check to see if any already exist:
                # for all of genebuild in dataset, see if any have the same label (genebuild.id) and version. If so, don't update and error out here!
                if dataset.name == "genebuild":
                    dataset_test = session.query(Dataset).filter(Dataset.name == "genebuild",
                                                                 Dataset.version == dataset.version,
                                                                 Dataset.label == dataset.label).first()
                    dataset.dataset_type = session.query(DatasetType).filter(
                        DatasetType.name == "genebuild").first()
                    dataset.dataset_source = self.dataset_source
                temp_genome_dataset = GenomeDataset(
                    genome_dataset_id=None,  # Should be autogenerated upon insertion
                    dataset_id=None,  # extract from dataset once genertated
                    genome_id=None,  # extract from genome once genertated
                    release_id=None,  # extract from release once genertated
                    is_current=0,
                )
                temp_genome_dataset.dataset = dataset
                self.genome.genome_datasets.append(temp_genome_dataset)
            # Add everything to the database. Closing the session commits it.
            session.add(self.genome)

    # The following methods populate the data from the core into the objects. K
    # It may be beneficial to move them to the base class with later implementations
    def new_organism(self):
        # All taken from the meta table except parlance name.
        self.organism = Organism(
            organism_id=None,  # Should be autogenerated upon insertion
            species_taxonomy_id=self.get_meta_single_meta_key(self.species, "species.species_taxonomy_id"),
            taxonomy_id=self.get_meta_single_meta_key(self.species, "species.taxonomy_id"),
            display_name=self.get_meta_single_meta_key(self.species, "species.display_name"),
            scientific_name=self.get_meta_single_meta_key(self.species, "species.scientific_name"),
            url_name=self.get_meta_single_meta_key(self.species, "species.url"),
            ensembl_name=self.get_meta_single_meta_key(self.species, "species.production_name"),
            strain=self.get_meta_single_meta_key(self.species, "species.strain"),
            scientific_parlance_name=None,
        )
        if self.organism.species_taxonomy_id is None:
            self.organism.species_taxonomy_id = self.organism.taxonomy_id

    def new_organism_group_and_members(self, session):
        # This method auto grabs the division name and checks for the strain groups
        division_name = self.get_meta_single_meta_key(self.species, "species.division")
        if division_name is None:
            Exception("No species.dvision found in meta table")
        division = session.execute(db.select(OrganismGroup).filter(OrganismGroup.name == division_name)).one_or_none()
        if division is None:
            group = OrganismGroup(
                organism_group_id=None,
                type="Division",
                name=division_name,
                code=None,
            )
        else:
            group = session.query(OrganismGroup).filter(OrganismGroup.name == division_name).first()
        self.organism_group_member = OrganismGroupMember(
            organism_group_member_id=None,
            is_reference=0,
            organism_id=None,
            organism_group_id=None,
        )
        self.organism_group_member.organism_group = group
        self.organism_group_member.organism = self.organism

        # Work on the strain level group members
        strain = self.get_meta_single_meta_key(self.species, "species.strain")
        strain_group = self.get_meta_single_meta_key(self.species, "species.strain_group")
        strain_type = self.get_meta_single_meta_key(self.species, "species.type")

        if strain is not None:
            if strain == 'reference':
                reference = 1
            else:
                reference = 0
            group_member = OrganismGroupMember(
                organism_group_member_id=None,
                is_reference=reference,
                organism_id=None,
                organism_group_id=None,
            )
            # Check for group, if not present make it
            division = session.execute(
                db.select(OrganismGroup).filter(OrganismGroup.name == strain_group)).one_or_none()
            if division is None:
                group = OrganismGroup(
                    organism_group_id=None,
                    type=strain_type,
                    name=strain_group,
                    code=None,
                )

            else:
                group = session.query(OrganismGroup).filter(OrganismGroup.name == strain_group).first()
                group_member.organism_group = group
                group_member.organism = self.organism

    def new_genome(self):
        # Data for the update function.
        self.genome = Genome(
            genome_id=None,  # Should be autogenerated upon insertion
            genome_uuid=str(uuid.uuid4()),
            assembly_id=None,  # Update the assembly before inserting and grab the assembly key
            organism_id=None,  # Update the organism before inserting and grab the organism_id
            created=func.now(),  # Replace all of them with sqlalchemy func.now()
        )

    def new_genome_release(self):
        # Genome Release
        self.genome_release = GenomeRelease(
            genome_release_id=None,  # Should be autogenerated upon insertion
            genome_id=None,  # Update the genome before inserting and grab the genome_id
            release_id=None,
            is_current=self.listed_release_is_current,
        )

    def new_assembly(self):
        level = None
        with self.db.session_scope() as session:
            level = (session.execute(db.select(CoordSystem.name).filter(
                CoordSystem.species_id == self.species).order_by(CoordSystem.rank)).all())[0][0]

        self.assembly = ensembl.production.metadata.models.assembly.Assembly(
            assembly_id=None,  # Should be autogenerated upon insertion
            ucsc_name=self.get_meta_single_meta_key(self.species, "assembly.ucsc_alias"),
            accession=self.get_meta_single_meta_key(self.species, "assembly.accession"),
            level=level,
            name=self.get_meta_single_meta_key(self.species, "assembly.name"),
            accession_body=None,  # Not implemented yet
            assembly_default=self.get_meta_single_meta_key(self.species, "assembly.default"),
            created=func.now(),
            ensembl_name=self.get_meta_single_meta_key(self.species, "assembly.name"),
        )

    def new_assembly_dataset(self):
        self.assembly_dataset = Dataset(
            dataset_id=None,  # Should be autogenerated upon insertion
            dataset_uuid=str(uuid.uuid4()),
            dataset_type_id=None,  # extract from dataset_type
            name="assembly",
            version=None,
            created=func.now(),
            dataset_source_id=None,  # extract from dataset_source
            label=self.assembly.accession,
            status='Submitted',
        )

    def new_assembly_sequence(self):
        self.assembly_sequences = []
        with self.db.session_scope() as session:
            # Alias the seq_region_attrib and seq_region_synonym tables
            sra1 = aliased(SeqRegionAttrib)
            sra3 = aliased(SeqRegionAttrib)

            results = (
                session.query(SeqRegion.name, SeqRegionSynonym.synonym, SeqRegion.length,
                              CoordSystem.name,
                              sra3.value,
                              )
                .join(CoordSystem, SeqRegion.coord_system_id == CoordSystem.coord_system_id)
                .join(Meta, CoordSystem.species_id == Meta.species_id)
                .join(sra1, SeqRegion.seq_region_id == sra1.seq_region_id)
                .outerjoin(SeqRegionSynonym, and_(SeqRegion.seq_region_id == SeqRegionSynonym.seq_region_id,
                                                  SeqRegionSynonym.external_db_id == 50710))
                .outerjoin(sra3, and_(SeqRegion.seq_region_id == sra3.seq_region_id,
                                      sra3.attrib_type_id == 547))
                .filter(Meta.meta_key == 'assembly.accession', sra1.attrib_type_id == 6,
                        Meta.species_id == self.species)
            ).all()
        for data in results:

            # If the name does not match normal accession formating, then use that name.
            name = None
            if re.match(r'^[a-zA-Z]+\d+\.\d+', data[0]):
                name = None
            else:
                name = data[0]
            # Nab accession from the seq region synonym or else the name.
            accession = None
            if data[1] is not None and re.match(r'^[a-zA-Z]+\d+\.\d+', data[1]):
                accession = data[1]
            elif name is not None:
                accession = name
            else:
                accession = data[0]

            chromosomal = 0
            if data[3] == 'chromosome':
                chromosomal = 1

            sequence_location = None
            if data[4] == 'nuclear_chromosome':
                sequence_location = 'SO:0000738'
            elif data[4] == 'mitochondrial_chromosome':
                sequence_location = 'SO:0000737'
            elif data[4] == 'chloroplast_chromosome':
                sequence_location = 'SO:0000745'
            elif data[4] is None:
                sequence_location = 'SO:0000738'
            else:
                raise Exception('Error with sequence location: ' + data[4] + ' is not a valid type')

            self.assembly_sequences.append(AssemblySequence(
                assembly_sequence_id=None,  # Should be autogenerated upon insertion
                name=name,
                assembly_id=None,  # Update the assembly before inserting and grab the assembly_id
                accession=accession,
                chromosomal=chromosomal,
                length=data[2],
                sequence_location=sequence_location,
                # These two get populated in the core stats pipeline.
                sequence_checksum=None,
                ga4gh_identifier=None,
            ))

    def new_genome_dataset(self):
        self.genome_dataset = GenomeDataset(
            genome_dataset_id=None,  # Should be autogenerated upon insertion
            dataset_id=None,  # extract from dataset once genertated
            genome_id=None,  # extract from genome once genertated
            release_id=None,  # extract from release once genertated
            is_current=self.listed_release_is_current,
        )

    def new_dataset_source(self):
        self.dataset_source = DatasetSource(
            dataset_source_id=None,  # Should be autogenerated upon insertion
            type=self.db_type,  # core/fungen etc
            name=make_url(self.db_uri).database  # dbname
        )

    def new_datasets(self):
        self.datasets = []
        # Genebuild.
        label = self.get_meta_single_meta_key(self.species, "genebuild.last_geneset_update")
        if label is None:
            label = self.get_meta_single_meta_key(self.species, "genebuild.start_date")
        self.datasets.append(Dataset(
            dataset_id=None,  # Should be autogenerated upon insertion
            dataset_uuid=str(uuid.uuid4()),
            dataset_type_id=None,  # extract from dataset_type
            name="genebuild",
            version=self.get_meta_single_meta_key(self.species, "gencode.version"),
            created=func.now(),
            dataset_source_id=None,  # extract from dataset_source
            label=label,
            status='Submitted',
        ))
        # Protein Features
