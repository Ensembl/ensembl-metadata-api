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
from sqlalchemy import select, func, desc, update, delete
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
        self.db_type = 'core'
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
        self.new_genome()  # needs organism_id and assembly_id
        self.new_genome_release()  # needs genome_id
        self.new_assembly()
        self.new_assembly_sequence()  # needs assembly_id
        self.new_dataset_source()
        self.new_genome_dataset()

        #################
        # Transactions are committed once per logic step (Species,Assembly,Dataset).
        # Failures are reverted mid transacation
        # If a failure happens it will only revert that section.
        # This should not cause issues, as the program will continue to update the following steps on restart.
        #################

        #Species Check and Update
        # Check for new genome by checking if ensembl name is already present in the database
        if GenomeAdaptor().fetch_genomes_by_ensembl_name(self.organism.ensembl_name) == []:
            self.fresh_genome()
            print("Fresh Organism. Adding data to organism, genome, genome_release, assembly, and assembly_sequence tables.")
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
                print("Old Organism with chages. Updating organism table")

        #Assembly Check and Update
        # A huge assumption here is that there is a single assembly for each organism. Otherwise several things will need
        # to be changed. !DP!
        with self.metadata_db.session_scope() as session:
            assembly_acc = session.execute(db.select(ensembl.production.metadata.models.Assembly
                        ).join(Genome.assembly).join(Genome.organism).filter(
                Organism.ensembl_name == self.organism.ensembl_name)).one().Assembly.accession
        new_assembly_acc = self.get_meta_single_meta_key(self.species, "assembly.accession")
        if new_assembly_acc == assembly_acc:
             print("Old Assembly with no change. No update to enome, genome_release, assembly, and assembly_sequence tables.")
        else:
            print("Old Assembly with changes. Updating Genome, genome_release, assembly, and assembly_sequence tables.")
            self.update_assembly()

    def fresh_genome(self):
        #In this, we are assuming that with a new genome, there will be a new assemblbly. I am also assuming that there
        #won't be an old assembly that needs to be deleted.
        #!DP!# Verify with Marc

        with self.metadata_db.session_scope() as session:
            session.add(self.organism)
            session.flush()

            #            scientific_parlance_name=None,


            organism_id = session.execute(db.select(Organism).filter(
                Organism.ensembl_name == self.organism.ensembl_name)).one().Organism.organism_id
            session.add(self.assembly)
            session.flush()
            assembly_id = session.execute(db.select(ensembl.production.metadata.models.Assembly).filter(
                ensembl.production.metadata.models.Assembly.accession == self.assembly.accession)).one().Assembly.assembly_id
            for assembly_seq in self.assembly_sequence:
                assembly_seq.assembly_id = assembly_id
            session.add_all(self.assembly_sequence)
            self.genome.organism_id = organism_id
            self.genome.assembly_id = assembly_id
            session.add(self.genome)
            session.flush()
            genome_id = session.execute(db.select(Genome).filter(
                Genome.organism_id == self.genome.organism_id)).one().Genome.genome_id
            self.genome_release.genome_id = genome_id
            session.add(self.genome_release)

            # Section for the total genome length #!DP!. Do I leave this out, or do I calculate it.
            #self.new_assembly_dataset()
            #add  self.dataset_source
            # Tables to update:
            #dataset #needs update_source_id
            # genome_dataset #needs dataset_id and genome_id
            # dataset attribute_ #needs dataset_id

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

    def update_assembly(self):
        with self.metadata_db.session_scope() as session:
            organism_id = session.execute(db.select(Organism).filter(
                Organism.ensembl_name == self.organism.ensembl_name)).one().Organism.organism_id
            genome_id = session.execute(db.select(Genome).filter(
                Genome.organism_id == organism_id)).one().Genome.genome_id
            assembly_id = session.execute(db.select(ensembl.production.metadata.models.Assembly
                        ).join(Genome.assembly).join(Genome.organism).filter(
                Organism.ensembl_name == self.organism.ensembl_name)).one().Assembly.assembly_id
            # Update date in Genome
            session.execute(update(Genome).where(Genome.genome_id == genome_id).values(
                 created=self.genome.created))
            # Update release_id and is_current in genome_release
            session.execute(update(GenomeRelease).where(GenomeRelease.genome_id == genome_id).values(
                 release_id=self.genome_release.release_id, is_current=self.genome_release.is_current))
            # Wipe all associated rows in assembly sequences and repopulate.
            session.execute(delete(AssemblySequence).where(AssemblySequence.assembly_id == assembly_id))
            session.flush()
            for assembly_seq in self.assembly_sequence:
                assembly_seq.assembly_id = assembly_id
            session.add_all(self.assembly_sequence)
            # full update of assembly.
            session.execute(
                update(ensembl.production.metadata.models.Assembly).where(ensembl.production.metadata.models.Assembly.assembly_id == assembly_id).values(
                    assembly_id=assembly_id,
                    ucsc_name=self.assembly.ucsc_name,
                    accession=self.assembly.accession,
                    level=self.assembly.level,
                    name=self.assembly.name,
                    accession_body=self.assembly.accession_body,
                    assembly_default=self.assembly.assembly_default,
                    created=self.assembly.created,
                    ensembl_name=self.assembly.ensembl_name,
                ))

            # Add self.dataset_source if it does not already exist





    # The following functions and classes are each related to a single table. It may be benificial to move them to the base class with later implementations
    def new_organism(self):
        self.organism = Organism(
            organism_id=None,  # Should be autogenerated upon insertion
            species_taxonomy_id=self.get_meta_single_meta_key(self.species, "species.species_taxonomy_id"),
            taxonomy_id=self.get_meta_single_meta_key(self.species, "species.taxonomy_id"),
            display_name=self.get_meta_single_meta_key(self.species, "species.display_name"),
            scientific_name=self.get_meta_single_meta_key(self.species, "species.scientific_name"),
            url_name=self.get_meta_single_meta_key(self.species, "species.url"),
            ensembl_name=self.get_meta_single_meta_key(self.species, "species.production_name"),
            strain=self.get_meta_single_meta_key(self.species, "species.strain"),

            # Implement this on its own.
            scientific_parlance_name=None,


        )
        if self.organism.species_taxonomy_id is None:
            self.organism.species_taxonomy_id = self.organism.taxonomy_id
        # print(self.organism.organism_id, self.organism.species_taxonomy_id, self.organism.taxonomy_id, self.organism.display_name,
        #      self.organism.scientific_name, self.organism.url_name, self.organism.ensembl_name)

        # !DP!#
        # We currently don't touch the tables organism_group or organism_group_member

    def new_organism(self):
        self.organism = Organism(
            organism_id=None,  # Should be autogenerated upon insertion
            species_taxonomy_id=self.get_meta_single_meta_key(self.species, "species.species_taxonomy_id"),
            taxonomy_id=self.get_meta_single_meta_key(self.species, "species.taxonomy_id"),
            display_name=self.get_meta_single_meta_key(self.species, "species.display_name"),
            scientific_name=self.get_meta_single_meta_key(self.species, "species.scientific_name"),
            url_name=self.get_meta_single_meta_key(self.species, "species.url"),
            ensembl_name=self.get_meta_single_meta_key(self.species, "species.production_name"),
            strain=self.get_meta_single_meta_key(self.species, "species.strain"),
            # !DP!#
            # What is the purpose to the strain? Not quite relevent to the project but we use this very oddly.
            # scientific_parlance_name=self.get_meta_single_meta_key(self.species, "species.scientific_parlance_name"),
        )

    def new_organism(self):
        self.organism = Organism(
            organism_id=None,  # Should be autogenerated upon insertion
            species_taxonomy_id=self.get_meta_single_meta_key(self.species, "species.species_taxonomy_id"),
            taxonomy_id=self.get_meta_single_meta_key(self.species, "species.taxonomy_id"),
            display_name=self.get_meta_single_meta_key(self.species, "species.display_name"),
            scientific_name=self.get_meta_single_meta_key(self.species, "species.scientific_name"),
            url_name=self.get_meta_single_meta_key(self.species, "species.url"),
            ensembl_name=self.get_meta_single_meta_key(self.species, "species.production_name"),
            strain=self.get_meta_single_meta_key(self.species, "species.strain"),
            # !DP!#
            # What is the purpose to the strain? Not quite relevent to the project but we use this very oddly.
            # scientific_parlance_name=self.get_meta_single_meta_key(self.species, "species.scientific_parlance_name"),
        )



    def new_genome(self):
        # Data for the update function.
        self.genome = Genome(
            genome_id=None,  # Should be autogenerated upon insertion
            genome_uuid=str(uuid.uuid4()),
            assembly_id=None,# Update the assembly before inserting and grab the assembly key
            organism_id=None,  # Update the organism before inserting and grab the organism_id
            created=datetime.datetime.utcnow(),
            # !DP!#
            # I feel that this date should be when the assembly is created. Easy enough to add, but our core db is not the best with the date.
            # Alternatively, we can grab the assembly date from the metadata. *This will not be in the proper format! but does that matter?
            # created = self.get_meta_single_meta_key(self.species, "assembly.date")
        )
        # print(self.genome.created, self.genome.genome_uuid)

    def new_genome_release(self):
        # Genome Release
        self.genome_release = GenomeRelease(
            genome_release_id=None,  # Should be autogenerated upon insertion
            genome_id=None,  # Update the genome before inserting and grab the genome_id
            release_id=self.release,
            is_current=self.release_is_current,
        )
    def new_assembly(self):
        # !DP!# We are loading models from two different files. I should switch to direct calls for one of them.
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
            accession_body=None,  # !DP!# I have no idea what this is supposed to be.
            assembly_default=self.get_meta_single_meta_key(self.species, "assembly.default"),
            created=datetime.datetime.utcnow(),
            # !DP!#
            # I feel that this date should be when the assembly is created. Easy enough to add, but our core db is not the best with the date.
            # Alternatively, we can grab the assembly date from the metadata. *This will not be in the proper format! but does that matter?
            # created = self.get_meta_single_meta_key(self.species, "assembly.date")
            ensembl_name=self.get_meta_single_meta_key(self.species, "species.production_name"),
            # !DP!# Why are we duplicating this? It is found in the genome. If it is something assembly related, what is it?
        )


    def new_assembly_sequence(self):
        self.assembly_sequence = []
        # !DP!# Come back to this when you have time. So none of these models in core/models.py are backrefed. Do it and then update to ORM.....
        # Will be ugly as an ORM. Comment heavily

        with self.db.session_scope() as session:
            accessions = session.execute("select distinct s.name, ss.synonym, c.name, s.length  from coord_system c "
                "join seq_region s using (coord_system_id)left join seq_region_synonym ss "
                "on(ss.seq_region_id=s.seq_region_id and ss.external_db_id in "
                f"(select external_db_id from external_db where db_name='INSDC')) where c.species_id={self.species}"
                " and attrib like '%default_version%'")
            #After this is done, turn it all into objects if that makes it cleaner and more readible.
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
        # for i in self.assembly_sequence:
        #     print (i.name, i.accession, i.chromosomal, i.length)

    # def new_assembly_dataset(self):
    #     self.new_dataset = Dataset(
    #         dataset_id=None,  # Should be autogenerated upon insertion
    #         dataset_uuid = str(uuid.uuid4()),
    #         dataset_type_id = 1,
    #         name = 'assembly',
    #         version = None,
    #         created = datetime.datetime.utcnow(),
    #         dataset_source_id = None, #Update after
    #         label = self.assembly.accession
    #     )
    #     self.dataset_attribute =  DatasetAttribute(
    #         dataset_attribute_id=None,  # Should be autogenerated upon insertion
    #         type = 'lenght_bp',
    #         value = #I don't think I am supposed to calculate this here, but can do if need be !DP!
    #         attribute_id = #Not sure how to get this properly either
    #         dataset_id = None,

    def new_genome_dataset(self):
        GenomeDataset(
            genome_dataset_id = None,  # Should be autogenerated upon insertion
            dataset_id = None, #extract from dataset once genertated
            genome_id = None, #extract from genome once genertated
            release_id = self.release,
            is_current = self.release_is_current,
        )
    def new_dataset_source(self):
        self.dataset_source = DatasetSource(
            dataset_source_id=None,  # Should be autogenerated upon insertion
            type = self.db_type,        # core/fungen etc
            name = make_url(self.db_uri).database   #dbname
        )
    # def new_dataset(self):
    #     Dataset(
    #         dataset_id = None,  # Should be autogenerated upon insertion
    #         dataset_uuid = str(uuid.uuid4()),
    #         dataset_type_id = "DO THIS NOW"#Lookup from dataset. Do now!
    #         name = Column(String(128), nullable=False)
    #         version = Column(String(128))
    #         created = Column(DATETIME(fsp=6), nullable=False)
    #         dataset_source_id = Column(ForeignKey('dataset_source.dataset_source_id'), nullable=False, index=True)
    #         label = Column(String(128), nullable=False)
    #     )



    def new_dataset(self):
        with self.db.session_scope() as session:
            print(session.execute(select(Meta.meta_value)).filter(
                Meta.meta_key == "species.species_taxonomy_id" and Meta.species_id == self.species).scalar().one())

    def update_dataset(self):
        with self.db.session_scope() as session:
            print(session.execute(select(Meta.meta_value)).filter(
                Meta.meta_key == "species.species_taxonomy_id" and Meta.species_id == self.species).scalar().one())


#                   a = Address(email='foo@bar.com')
#                   p = Person(name='foo')
#                    p.addresses.append(a)


# Check to see if metadata.organism.ensembl_name contains meta.species.production_name

# Update the following metadata:
# organism:taxonomy_id, specie_taxonomy_id, display_name, strain, scientific_name, url_name, ensembl_name,
# organism_group_member:is_reference,organism_id,organism_group_id,organism_member_id

# Do not touch scientific parlence name!

# Org name = ensembl.name

# Check to see if the genome exists in the database.
# If it does, has it changed?

# Check to see if the assembly exists in the db.
# If it does, has it changed?

# Check to see if the datasets exist in the db.
# If so, have they changed?

# DO NOT CALCULATE STATISTICS

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
