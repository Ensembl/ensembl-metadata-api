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
from pathlib import Path

import pytest
from ensembl.core.models import Meta
from ensembl.utils.database import UnitTestDB, DBConnection

from ensembl.production.metadata.api.exceptions import MetadataUpdateException
from ensembl.production.metadata.api.factory import meta_factory
from ensembl.production.metadata.api.models import *

db_directory = Path(__file__).parent / 'databases'
db_directory = db_directory.resolve()


@pytest.mark.parametrize("test_dbs", [[{'src': Path(__file__).parent / "databases/ensembl_genome_metadata"},
                                       {'src': Path(__file__).parent / "databases/ncbi_taxonomy"},
                                       {'src': Path(__file__).parent / "databases/core_1"},
                                       {'src': Path(__file__).parent / "databases/core_2"},
                                       {'src': Path(__file__).parent / "databases/core_3"},
                                       {'src': Path(__file__).parent / "databases/core_4"},
                                       {'src': Path(__file__).parent / "databases/core_5"},
                                       {'src': Path(__file__).parent / "databases/core_6"},
                                       {'src': Path(__file__).parent / "databases/core_7"},
                                       {'src': Path(__file__).parent / "databases/core_8"}
                                       ]],
                         indirect=True)
class TestUpdater:
    dbc = None  # type: UnitTestDB

    def test_new_organism(self, test_dbs):
        test = meta_factory(test_dbs['core_1'].dbc.url,
                            test_dbs['ensembl_genome_metadata'].dbc.url,
                            test_dbs['ncbi_taxonomy'].dbc.url)
        test.process_core()

        # Check for insertion of genome_uuid
        core_1_db = DBConnection(test_dbs['core_1'].dbc.url)
        inserted_genome_uuid = None
        with core_1_db.session_scope() as session:
            species_id = "1"
            inserted_meta = session.query(Meta).filter(
                Meta.species_id == species_id,
                Meta.meta_key == 'genome.genome_uuid'
            ).first()
            inserted_genome_uuid = inserted_meta.meta_value
        assert inserted_genome_uuid is not None

        # Look for organism, assembly and geneset
        metadata_db = DBConnection(test_dbs['ensembl_genome_metadata'].dbc.url)
        # Test the species
        with metadata_db.session_scope() as session:
            organism = session.query(Organism).where(Organism.biosample_id == 'Jabberwocky').first()
            assembly = session.query(Assembly).where(Assembly.name == 'jaber01').first()
            assert organism.scientific_name == 'carol_jabberwocky'
            assert organism.genomes[0].genebuild_date == '2023-01'
            # Test the Assembly
            assert assembly.accession == 'GCF_1111111123.3'
            # select * from genebuild where version = 999 and name = 'genebuild and label =01
            dataset = session.query(Dataset).where(
                (Dataset.version == 'ENS01') & (Dataset.name == 'genebuild')
            ).first()
            assert dataset is not None
            assert re.match(".*_core_1", dataset.dataset_source.name)
            assert dataset.dataset_source.type == "core"
            assert dataset.dataset_type.name == "genebuild"
            # Testing assembly sequence is circular
            sequence = session.query(AssemblySequence).where(
                (AssemblySequence.is_circular == 1) & (AssemblySequence.name == 'AA123456.1')
            ).first()
            assert sequence is not None
            assert sequence.type == "primary_assembly"  # Testing assembly_sequence.type
            sequence2 = session.query(AssemblySequence).where(
                (AssemblySequence.is_circular == 0) & (AssemblySequence.name == 'AA123456.2')
            ).first()
            assert sequence2 is not None
            assert sequence.type == "primary_assembly"
            sequence3 = session.query(AssemblySequence).where(
                (AssemblySequence.is_circular == 0) & (AssemblySequence.name == 'AA123456.3')
            ).first()
            assert sequence3 is not None
            count = session.query(Dataset).join(DatasetSource).join(DatasetType) \
                .join(GenomeDataset).join(Genome).filter(
                DatasetSource.name.like('%compara%'),
                DatasetType.name == 'homology_compute',
                Genome.genome_uuid == inserted_genome_uuid
            ).count()
            assert count == 1

    def test_fail_existing_genome_uuid_no_data(self, test_dbs):
        test = meta_factory(test_dbs['core_2'].dbc.url,
                            test_dbs['ensembl_genome_metadata'].dbc.url,
                            test_dbs['ncbi_taxonomy'].dbc.url)
        with pytest.raises(MetadataUpdateException) as exif:
            test.process_core()
            assert ("Database contains a Genome.genome_uuid, "
                    "but the corresponding data is not in the meta table. "
                    "Please remove it from the meta key and resubmit" in str(exif.value))

    def test_update_assembly(self, test_dbs):
        test = meta_factory(test_dbs['core_3'].dbc.url,
                            test_dbs['ensembl_genome_metadata'].dbc.url,
                            test_dbs['ncbi_taxonomy'].dbc.url)
        test.process_core()

        # Get the genome_uuid that was just inserted
        core_3_db = DBConnection(test_dbs['core_3'].dbc.url)
        with core_3_db.session_scope() as core_session:
            inserted_meta = core_session.query(Meta).filter(
                Meta.species_id == "1",
                Meta.meta_key == 'genome.genome_uuid'
            ).first()
            inserted_genome_uuid = inserted_meta.meta_value

        metadata_db = DBConnection(test_dbs['ensembl_genome_metadata'].dbc.url)
        with metadata_db.session_scope() as session:
            genome = session.query(Genome).filter(
                Genome.genome_uuid == inserted_genome_uuid
            ).one()

            organism = genome.organism
            assert organism.scientific_name == 'carol_jabberwocky'
            assert genome.assembly.accession == 'weird02'
            assert genome.genebuild_date == '2024-02'
    #
    def test_update_geneset(self, test_dbs):
        # Run the update process
        test = meta_factory(test_dbs['core_4'].dbc.url,
                            test_dbs['ensembl_genome_metadata'].dbc.url,
                            test_dbs['ncbi_taxonomy'].dbc.url)
        test.process_core()

        # Get the genome_uuid that was just inserted into core_4 by the process
        core_4_db = DBConnection(test_dbs['core_4'].dbc.url)
        with core_4_db.session_scope() as core_session:
            inserted_meta = core_session.query(Meta).filter(
                Meta.species_id == "1",
                Meta.meta_key == 'genome.genome_uuid'
            ).first()
            inserted_genome_uuid = inserted_meta.meta_value

        # Now query the metadata database for THIS SPECIFIC genome
        metadata_db = DBConnection(test_dbs['ensembl_genome_metadata'].dbc.url)
        with metadata_db.session_scope() as session:
            # Get the genome that was just created
            genome = session.query(Genome).filter(
                Genome.genome_uuid == inserted_genome_uuid
            ).one()

            # Get the genebuild dataset for THIS genome
            genebuild_dataset = session.query(Dataset).join(GenomeDataset).join(Genome).filter(
                Genome.genome_uuid == inserted_genome_uuid,
                Dataset.name == "genebuild"
            ).one()  # ← ADD THIS!

            assert genebuild_dataset is not None

            assert re.match(".*_core_4", genebuild_dataset.dataset_source.name)
            assert genebuild_dataset.dataset_source.type == "core"
            assert genebuild_dataset.dataset_type.name == "genebuild"
            assert genome.genebuild_date == '2023-01'  # From core_4 meta table
            assert len(genome.genome_releases) > 0

    def test_taxonomy_common_name(self, test_dbs):
        test = meta_factory(test_dbs['core_5'].dbc.url,
                            test_dbs['ensembl_genome_metadata'].dbc.url,
                            test_dbs['ncbi_taxonomy'].dbc.url)
        test.process_core()
        metadata_db = DBConnection(test_dbs['ensembl_genome_metadata'].dbc.url)
        with metadata_db.session_scope() as session:
            organism = session.query(Organism).where(Organism.biosample_id == 'test_case_5').first()
            assert organism.common_name == 'Sheep'

    def test_fail_existing_genome_uuid_data_not_match(self, test_dbs):
        test = meta_factory(test_dbs['core_6'].dbc.url,
                            test_dbs['ensembl_genome_metadata'].dbc.url,
                            test_dbs['ncbi_taxonomy'].dbc.url)
        with pytest.raises(MetadataUpdateException) as exif:
            test.process_core()
            assert ("Core database contains a genome.genome_uuid which matches an entry in the meta table. "
                    "The force flag was not specified so the core was not updated." in str(exif.value))

    def test_update_released(self, test_dbs):
        test = meta_factory(test_dbs['core_8'].dbc.url,
                            test_dbs['ensembl_genome_metadata'].dbc.url,
                            test_dbs['ncbi_taxonomy'].dbc.url)
        with pytest.raises(Exception) as exif:
            test.process_core()
            assert ("Existing Organism, Assembly, and Datasets within a release. ")
