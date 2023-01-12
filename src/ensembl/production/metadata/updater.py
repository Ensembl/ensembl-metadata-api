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


# TODO: add in cleanup_removed_genomes_collection, clean up inports, solve collections situation

import re

import ensembl.production.metadata.models
from ensembl.production.metadata.api import *

from ensembl.database.dbconnection import DBConnection
from sqlalchemy.engine.url import make_url
from sqlalchemy import select, update, func
from ensembl.core.models import *
import uuid
import datetime


class BaseMetaUpdater:
    def __init__(self, db_uri, metadata_uri=None, release=None):
        self.db_uri = db_uri
        self.db = DBConnection(self.db_uri)
        self.species = None
        self.db_type = None
        if metadata_uri is None:
            metadata_uri = get_metadata_uri()
        #Use the current release if none is specified.
        if release is None:
            self.release = NewReleaseAdaptor(metadata_uri).current_release_id
        else:
            self.release = release
        self.release_is_current = ReleaseAdaptor(metadata_uri).fetch_releases(release_id=self.release)[
            0].EnsemblRelease.is_current
        self.metadata_db = DBConnection(metadata_uri)

    # Basic API for the meta table in the submission database.
    def get_meta_single_meta_key(self, species_id, parameter):
        with self.db.session_scope() as session:
            result = (session.execute(db.select(Meta.meta_value).filter(
                Meta.meta_key == parameter and Meta.species_id == species_id)).one_or_none())
            if result is None:
                return None
            else:
                return result[0]


class CoreMetaUpdater(BaseMetaUpdater):
    def __init__(self, db_uri, metadata_uri):
        # Each of these objects represents a table in the database to store data in as either an array or a single object.
        self.organism = None
        self.organism_group_member = None   #Implement
        self.organism_group = None          #Implement

        self.assembly = None
        self.assembly_sequence = None

        self.genome = None
        self.genome_release = None

        self.genome_dataset = None
        self.dataset = None
        self.dataset_type = None
        self.dataset_source = None
        self.dataset_attribute = None
        self.attribute = None

        super().__init__(db_uri, metadata_uri)
        self.db_type = 'core'


    def process_core(self):
        # Handle multispecies databases and run an update for each species
        with self.db.session_scope() as session:
            multi_species = session.execute(
                select(Meta.species_id).filter(Meta.meta_key == "species.production_name").distinct()
            )
        multi_species = [multi_species for multi_species, in multi_species]

        if len(multi_species) == 1:
            self.species = multi_species[0]
            self.process_species()
        else:
            # Add in the cleanup_removed genomes here
            # Further discussion is needed to see how this will be implemented.
            for species in multi_species:
                self.species = species
                self.process_species()

    def process_species(self):

        #Get species data from the core db and populate the temporary meta table objects
        self.new_organism()
        self.new_genome()
        self.new_genome_release()
        self.new_assembly()
        self.new_assembly_sequence()
        self.new_dataset_source()
        self.new_genome_dataset()
        self.new_dataset()

        #################
        # Transactions are committed once per pipeline.
        # Failures prevent any commit
        #################

        #Species Check and Update
        # Check for new genome by checking if ensembl name is already present in the database
        if GenomeAdaptor().fetch_genomes_by_ensembl_name(self.organism.ensembl_name) == []:
            self.fresh_genome()
            print("Fresh Organism. Adding data to organism, genome, genome_release,"
                  " assembly, assembly_sequence, dataset, dataset source, and genome_dataset tables.")
        else:
            with self.metadata_db.session_scope() as session:
                session.expire_on_commit = False
                test_organism = session.execute(db.select(Organism).filter(
                    Organism.ensembl_name == self.organism.ensembl_name)).one()
            self.organism.organism_id = test_organism.Organism.organism_id
            self.organism.scientific_parlance_name = test_organism.Organism.scientific_parlance_name
#           The following should work, but doesn't due to some type switching.
            #!DP!# Change this if you do for the other checks.
#            if test_organism.Organism == self.organism:
            if int(test_organism.Organism.species_taxonomy_id) == int(self.organism.species_taxonomy_id) and \
                    int(test_organism.Organism.taxonomy_id) == int(self.organism.taxonomy_id) and \
                    str(test_organism.Organism.display_name) == str(self.organism.display_name) and \
                    str(test_organism.Organism.scientific_name) == str(self.organism.scientific_name) and \
                    str(test_organism.Organism.url_name) == str(self.organism.url_name) and \
                    str(test_organism.Organism.ensembl_name) == str(self.organism.ensembl_name) and \
                    str(test_organism.Organism.strain) == str(self.organism.strain):
                print("Old Organism with no change. No update to organism table")
            else:
                self.update_organism()
                print("Old Organism with changes. Updating organism table")

        # Assembly Check and Update
        with self.metadata_db.session_scope() as session:
            assembly_acc = session.execute(db.select(ensembl.production.metadata.models.Assembly
                        ).join(Genome.assembly).join(Genome.organism).filter(
                Organism.ensembl_name == self.organism.ensembl_name)).one().Assembly.accession
        new_assembly_acc = self.get_meta_single_meta_key(self.species, "assembly.accession")
        if new_assembly_acc == assembly_acc:
             print("Old Assembly with no change. No update to Genome, genome_release, assembly, and assembly_sequence tables.")
        else:
            print("New Assembly. Updating  genome, genome_release,"
                  " assembly, assembly_sequence, dataset, dataset source, and genome_dataset tables.")
            self.update_assembly()

    def fresh_genome(self):
        #In this, we are assuming that with a new genome, there will be a new assemblbly. I am also assuming that there
        #won't be an old assembly that needs to be deleted.

        with self.metadata_db.session_scope() as session:
            #Organism section
            #Updating Organism, organism_group_member, and organism_group
            self.new_organism_group_and_members(session)
            #Add in the new assembly here
            #assembly sequence, assembly, genome, genome release.
            assembly_test = session.execute(db.select(ensembl.production.metadata.models.Assembly).filter(
                ensembl.production.metadata.models.Assembly.accession == self.assembly.accession)).one_or_none()
            if assembly_test is not None:
                Exception("Error, existing name but, assembly accession already found. Please update the Ensembl Name in the Meta field manually")

            release = session.query(EnsemblRelease).filter(EnsemblRelease.release_id == self.release).first()

            for assembly_seq in self.assembly_sequence:
                assembly_seq.assembly = self.assembly
            self.assembly.genomes.append(self.genome)

            self.genome_release.ensembl_release = release
            self.genome_release.genome = self.genome

            self.genome.organism = self.organism

            # Update assembly dataset
            #Updates genome_dataset,dataset,dataset_source
            dataset_source_test = session.execute(db.select(DatasetSource).filter(DatasetSource.name == self.dataset_source.name)).one_or_none()
            if dataset_source_test is not None:
                Exception("Error, data already present in source")

            dataset_type = session.query(DatasetType).filter(DatasetType.name == "assembly").first()

            self.genome_dataset.ensembl_release = release
            self.genome_dataset.genome = self.genome
            self.genome_dataset.dataset = self.dataset

            self.dataset.dataset_type = dataset_type
            self.dataset.dataset_source = self.dataset_source

            #Add everything to the database. Closing the session commits it.
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

            #TODO: Add an update to the groups here.

    def update_assembly(self):
        with self.metadata_db.session_scope() as session:
            #Get the genome
            self.genome = session.query(Genome,Organism).filter(Genome.organism_id==Organism.organism_id).filter(
                Organism.ensembl_name == self.organism.ensembl_name).first().Genome

            release = session.query(EnsemblRelease).filter(EnsemblRelease.release_id == self.release).first()

            for assembly_seq in self.assembly_sequence:
                assembly_seq.assembly = self.assembly
            self.assembly.genomes.append(self.genome)

            self.genome_release.ensembl_release = release
            self.genome_release.genome = self.genome

            # Update assembly dataset
            #Updates genome_dataset,dataset,dataset_source
            dataset_source_test = session.execute(db.select(DatasetSource).filter(DatasetSource.name == self.dataset_source.name)).one_or_none()
            if dataset_source_test is not None:
                self.dataset_source = session.query(DatasetSource).filter(DatasetSource.name == self.dataset_source.name).first()

            dataset_type = session.query(DatasetType).filter(DatasetType.name == "assembly").first()

            self.genome_dataset.ensembl_release = release
            self.genome_dataset.genome = self.genome
            self.genome_dataset.dataset = self.dataset

            self.dataset.dataset_type = dataset_type
            self.dataset.dataset_source = self.dataset_source




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


    def new_organism_group_and_members(self,session):
        #This method auto grabs the division name and checks for the strain groups
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
                print(division)
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
            assembly_id=None,# Update the assembly before inserting and grab the assembly key
            organism_id=None,  # Update the organism before inserting and grab the organism_id
            created=func.now(), # Replace all of them with sqlalchemy func.now()
        )

    def new_genome_release(self):
        # Genome Release
        self.genome_release = GenomeRelease(
            genome_release_id=None,  # Should be autogenerated upon insertion
            genome_id=None,  # Update the genome before inserting and grab the genome_id
            release_id=None,
            is_current=self.release_is_current,
        )
    def new_assembly(self):
        level = None
        print (self.species)
        with self.db.session_scope() as session:
            level = (session.execute(db.select(CoordSystem.name).filter(
                CoordSystem.species_id == self.species).order_by(CoordSystem.rank)).all())[0][0]
            print (level)
        self.assembly = ensembl.production.metadata.models.Assembly(
            assembly_id=None,  # Should be autogenerated upon insertion
            ucsc_name=self.get_meta_single_meta_key(self.species, "assembly.ucsc_alias"),
            accession=self.get_meta_single_meta_key(self.species, "assembly.accession"),
            level=level,
            name=self.get_meta_single_meta_key(self.species, "assembly.name"),
            accession_body=None,  # Not implemented yet
            assembly_default=self.get_meta_single_meta_key(self.species, "assembly.default"),
            created=func.now(),
            ensembl_name=self.get_meta_single_meta_key(self.species, "species.production_name"),
            # !DP!# Why are we duplicating this? It is found in the genome. If it is something assembly related, what is it?
        )


    def new_assembly_sequence(self):
        self.assembly_sequence = []
            #TODO:Make this ORM, get the sequence location
        with self.db.session_scope() as session:
            accessions = session.execute("select distinct s.name, ss.synonym, c.name, s.length  from coord_system c "
                "join seq_region s using (coord_system_id)left join seq_region_synonym ss "
                "on(ss.seq_region_id=s.seq_region_id and ss.external_db_id in "
                f"(select external_db_id from external_db where db_name='INSDC')) where c.species_id={self.species}"
                " and attrib like '%default_version%'")
            accession_dict = {}
            length_dict = {}
            chromosome_dict = {}

            for acc in accessions:
                accession_dict[acc[0]] = acc[1]
                length_dict[acc[0]] = acc[3]
                if acc[2] == 'chromosome':
                    chromosome_dict[acc[0]] = 1
                else:
                    chromosome_dict[acc[0]] = 0

        #Populate the accessions based on those that have ENA.
            accessions = []
            accessions = session.execute("select s.name "
                                         "from coord_system c "
                                         "join seq_region s using (coord_system_id) "
                                         "join seq_region_attrib sa using (seq_region_id) "
                                         f"where sa.value='ENA' and c.species_id={self.species} "
                                         "and attrib like '%default_version%'")
            for acc in accessions:
                accession_dict[acc[0]] = acc[0]

            #Look up the sequence location
            #!D!# Not sure exactly what I should put here. Looks easy enough, but which joins to do should be disscussed.

            #Nor am I sure where to get the sequence checksum or ga4gh identifiers
        for name in accession_dict:
            self.assembly_sequence.append(AssemblySequence(
                assembly_sequence_id = None,  # Should be autogenerated upon insertion
                name = name,
                assembly_id = None,  # Update the assembly before inserting and grab the assembly_id
                accession = accession_dict[name],
                chromosomal = chromosome_dict[name],
                length = length_dict[name],
                sequence_location = None,
                sequence_checksum = None,
                ga4gh_identifier = None,
            ))

    def new_genome_dataset(self):
        self.genome_dataset = GenomeDataset(
            genome_dataset_id = None,  # Should be autogenerated upon insertion
            dataset_id = None, #extract from dataset once genertated
            genome_id = None, #extract from genome once genertated
            release_id = None,#extract from release once genertated
            is_current = self.release_is_current,
        )
    def new_dataset_source(self):
        self.dataset_source = DatasetSource(
            dataset_source_id=None,  # Should be autogenerated upon insertion
            type = self.db_type,        # core/fungen etc
            name = make_url(self.db_uri).database   #dbname
        )
        print (self.dataset_source.type)
        print (self.db_type)
    def new_dataset(self):
        #Currently only functional for assembly.
        self.dataset = Dataset(
            dataset_id = None,  # Should be autogenerated upon insertion
            dataset_uuid = str(uuid.uuid4()),
            dataset_type_id = None,#extract from dataset_type
            name = "assembly",
            version = None,
            created = func.now(),
            dataset_source_id = None, #extract from dataset_source
            label = self.assembly.accession,
        )





def meta_factory(db_uri, metadata_uri=None):
    db_url = make_url(db_uri)
    if '_compara_' in db_url.database:
        raise Exception("compara not implemented yet")
    # !DP!#
    # NOT DONE#######################Worry about this after the core et al are done. Don't delete it yet.
    #     elif '_collection_' in db_url.database:
    #        self.db_type = "collection"
    ################################################################
    elif '_variation_' in db_url.database:
        raise Exception("variation not implemented yet")
    elif '_funcgen_' in db_url.database:
        raise Exception("funcgen not implemented yet")
    elif '_core_' in db_url.database:
        return CoreMetaUpdater(db_uri, metadata_uri)
    elif '_otherfeatures_' in db_url.database:
        raise Exception("otherfeatures not implemented yet")
    elif '_rnaseq_' in db_url.database:
        raise Exception("rnaseq not implemented yet")
    elif '_cdna_' in db_url.database:
        raise Exception("cdna not implemented yet")
    # Dealing with other versionned databases like mart, ontology,...
    elif re.match('^\w+_?\d*_\d+$', db_url.database):
        raise Exception("other not implemented yet")
    elif re.match(
            '^ensembl_accounts|ensembl_archive|ensembl_autocomplete|ensembl_metadata|ensembl_production|ensembl_stable_ids|ncbi_taxonomy|ontology|website',
            db_url.database):
        raise Exception("other not implemented yet")
    else:
        raise "Can't find data_type for database " + db_url.database


# Simple Needs drastic simplification for tests.
TEST = meta_factory('mysql://danielp:Killadam69!@localhost:3306/acanthochromis_polyacanthus_core_109_1','mysql://danielp:Killadam69!@localhost:3306/ensembl_metadata_2020')
# multi_db Needs humoungous simplification for tests
# TEST = MetaUpdater('mysql://danielp:Killadam69!@localhost:3306/fungi_ascomycota1_collection_core_56_109_1','mysql://danielp:Killadam69!@localhost:3306/ensembl_metadata_2020')

# Data Present
#TEST = meta_factory('mysql://danielp:Killadam69!@localhost:3306/caenorhabditis_elegans_core_56_109_282', 'mysql://danielp:Killadam69!@localhost:3306/ensembl_metadata_2020')

TEST.process_core()
