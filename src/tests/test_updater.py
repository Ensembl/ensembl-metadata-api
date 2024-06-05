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
from ensembl.database import UnitTestDB, DBConnection

from ensembl.production.metadata.api.exceptions import MetadataUpdateException
from ensembl.production.metadata.api.factory import meta_factory
from ensembl.production.metadata.api.models import *

db_directory = Path(__file__).parent / 'databases'
db_directory = db_directory.resolve()


@pytest.mark.parametrize("multi_dbs", [[{'src': Path(__file__).parent / "databases/ensembl_genome_metadata"},
                                        {'src': Path(__file__).parent / "databases/ncbi_taxonomy"},
                                        {'src': Path(__file__).parent / "databases/core_1"},
                                        {'src': Path(__file__).parent / "databases/core_2"},
                                        {'src': Path(__file__).parent / "databases/core_3"},
                                        {'src': Path(__file__).parent / "databases/core_4"},
                                        {'src': Path(__file__).parent / "databases/core_5"},
                                        {'src': Path(__file__).parent / "databases/core_6"},
                                        {'src': Path(__file__).parent / "databases/core_7"},
                                        {'src': Path(__file__).parent / "databases/core_8"},
                                        {'src': Path(__file__).parent / "databases/core_9"}
                                        ]],
                         indirect=True)
class TestUpdater:
    dbc = None  # type: UnitTestDB

    def test_new_organism(self, multi_dbs):
        test = meta_factory(multi_dbs['core_1'].dbc.url,
                            multi_dbs['ensembl_genome_metadata'].dbc.url,
                            multi_dbs['ncbi_taxonomy'].dbc.url)
        test.process_core()

        # Check for insertion of genome_uuid
        core_1_db = DBConnection(multi_dbs['core_1'].dbc.url)
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
        metadata_db = DBConnection(multi_dbs['ensembl_genome_metadata'].dbc.url)
        # Test the species
        with metadata_db.session_scope() as session:
            organism = session.query(Organism).where(Organism.biosample_id == 'Jabberwocky').first()
            assembly = session.query(Assembly).where(Assembly.name == 'jaber01').first()
            assert organism.scientific_name == 'carol_jabberwocky'
            assert organism.genomes[0].genebuild_version == 'ENS01'
            assert organism.genomes[0].genebuild_date == '2023-07'
            # Test the Assembly
            assert assembly.accession == 'GCF_1111111123.3'
            assert assembly.alt_accession == 'GCA_0000012345.3'
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
                (AssemblySequence.is_circular == 1) & (AssemblySequence.name == 'TEST1_seqA')
            ).first()
            assert sequence is not None
            assert sequence.type == "primary_assembly"  # Testing assembly_sequence.type
            sequence2 = session.query(AssemblySequence).where(
                (AssemblySequence.is_circular == 0) & (AssemblySequence.name == 'TEST2_seqB')
            ).first()
            assert sequence2 is not None
            assert sequence.type == "primary_assembly"
            sequence3 = session.query(AssemblySequence).where(
                (AssemblySequence.is_circular == 0) & (AssemblySequence.name == 'TEST3_seqC')
            ).first()
            assert sequence3 is not None
            count = session.query(Dataset).join(DatasetSource).join(DatasetType) \
                .join(GenomeDataset).join(Genome).filter(
                DatasetSource.name.like('%compara%'),
                DatasetType.name == 'homology_compute',
                Genome.genome_uuid == inserted_genome_uuid
            ).count()
            assert count == 1

    def test_fail_existing_genome_uuid_no_data(self, multi_dbs):
        test = meta_factory(multi_dbs['core_2'].dbc.url, multi_dbs['ensembl_genome_metadata'].dbc.url,
                            multi_dbs['ncbi_taxonomy'].dbc.url)
        with pytest.raises(MetadataUpdateException) as exif:
            test.process_core()
            assert ("Database contains a Genome.genome_uuid, "
                    "but the corresponding data is not in the meta table. "
                    "Please remove it from the meta key and resubmit" in str(exif.value))

    def test_update_assembly(self, multi_dbs):
        test = meta_factory(multi_dbs['core_3'].dbc.url, multi_dbs['ensembl_genome_metadata'].dbc.url,
                            multi_dbs['ncbi_taxonomy'].dbc.url)
        test.process_core()
        metadata_db = DBConnection(multi_dbs['ensembl_genome_metadata'].dbc.url)
        with metadata_db.session_scope() as session:
            organism = session.query(Organism).where(Organism.biosample_id == 'Jabberwocky').first()
            assert organism.scientific_name == 'carol_jabberwocky'
            assert organism.genomes[1].assembly.accession == 'weird02'
            assert organism.genomes[1].genebuild_version == 'ENS01'
            assert organism.genomes[1].genebuild_date == '2024-02'

    #
    def test_update_geneset(self, multi_dbs):
        test = meta_factory(multi_dbs['core_4'].dbc.url, multi_dbs['ensembl_genome_metadata'].dbc.url,
                            multi_dbs['ncbi_taxonomy'].dbc.url)
        test.process_core()
        metadata_db = DBConnection(multi_dbs['ensembl_genome_metadata'].dbc.url)
        with metadata_db.session_scope() as session:
            dataset = session.query(Dataset).where(
                (Dataset.version == "ENS02") & (Dataset.name == 'genebuild')
            ).first()
            assert dataset is not None
            assert re.match(".*_core_4", dataset.dataset_source.name)
            assert dataset.dataset_source.type == "core"
            assert dataset.dataset_type.name == "genebuild"
            assert dataset.genome_datasets[0].genome.genebuild_version == 'ENS02'
            assert dataset.genome_datasets[0].genome.genebuild_date == '2023-07'
            assert dataset.genome_datasets[0].genome.genome_releases is not None

    def test_taxonomy_common_name(self, multi_dbs):
        test = meta_factory(multi_dbs['core_5'].dbc.url, multi_dbs['ensembl_genome_metadata'].dbc.url,
                            multi_dbs['ncbi_taxonomy'].dbc.url)
        test.process_core()
        metadata_db = DBConnection(multi_dbs['ensembl_genome_metadata'].dbc.url)
        with metadata_db.session_scope() as session:
            organism = session.query(Organism).where(Organism.biosample_id == 'test_case_5').first()
            assert organism.common_name == 'Sheep'

    def test_fail_existing_genome_uuid_data_not_match(self, multi_dbs):
        test = meta_factory(multi_dbs['core_6'].dbc.url, multi_dbs['ensembl_genome_metadata'].dbc.url,
                            multi_dbs['ncbi_taxonomy'].dbc.url)
        with pytest.raises(MetadataUpdateException) as exif:
            test.process_core()
            assert ("Core database contains a genome.genome_uuid which matches an entry in the meta table. "
                    "The force flag was not specified so the core was not updated." in str(exif.value))

    def test_update_unreleased_no_force(self, multi_dbs):
        test = meta_factory(multi_dbs['core_7'].dbc.url, multi_dbs['ensembl_genome_metadata'].dbc.url,
                            multi_dbs['ncbi_taxonomy'].dbc.url)
        test.process_core()
        metadata_db = DBConnection(multi_dbs['ensembl_genome_metadata'].dbc.url)
        with metadata_db.session_scope() as session:
            # Test that assembly seqs have been updated
            new_seq = session.query(AssemblySequence).filter(
                AssemblySequence.name == 'TEST1_seq_update').one_or_none()
            assert new_seq is None
            old_seq = session.query(AssemblySequence).where(
                (AssemblySequence.name == 'TEST1_seqA')).first()
            assert old_seq is not None
            # Check that the old datasets have been removed
            genebuild_test = session.query(Dataset).join(DatasetSource).join(DatasetType).filter(
                DatasetSource.name.like('%core_5'),
            ).filter(DatasetType.name == "genebuild").one_or_none()
            assert genebuild_test is None

            # Check that the old attributes are gone
            count = session.query(DatasetAttribute).join(Attribute).filter(
                Attribute.name == 'assembly.default',
                DatasetAttribute.value == 'NewTest'
            ).count()
            assert count == 1
            count = session.query(DatasetAttribute).join(Attribute).filter(
                Attribute.name == 'genebuild.provider_name',
                DatasetAttribute.value == 'removed_for_test'
            ).count()
            assert count == 0

            # Check that the new dataset are present and not duplicated
            count = session.query(Dataset).join(DatasetSource).join(DatasetType).filter(
                DatasetSource.name.like('%core_7'),
                DatasetType.name == 'assembly'
            ).count()
            assert count == 1
            count = session.query(Dataset).join(DatasetSource).join(DatasetType).filter(
                DatasetSource.name.like('%core_7'),
                DatasetType.name == 'genebuild'
            ).count()
            assert count == 1
            # Check that the new attribute values are present
            count = session.query(DatasetAttribute).join(Attribute).filter(
                Attribute.name == 'assembly.ucsc_alias',
                DatasetAttribute.value == 'test_alias'
            ).count()
            assert count > 0

            count = session.query(DatasetAttribute).join(Attribute).filter(
                Attribute.name == 'genebuild.havana_datafreeze_date',
                DatasetAttribute.value == 'test2'
            ).count()
            assert count > 0

    def test_update_released_no_force(self, multi_dbs):
        test = meta_factory(multi_dbs['core_8'].dbc.url, multi_dbs['ensembl_genome_metadata'].dbc.url,
                            multi_dbs['ncbi_taxonomy'].dbc.url)
        with pytest.raises(Exception) as exif:
            test.process_core()
            assert ("Existing Organism, Assembly, and Datasets within a release. "
                    "To update released data set force=True. "
                    "This will force assembly and genebuilddataset updates and assembly sequences." in str(exif.value))

    def test_update_released_force(self, multi_dbs):
        test = meta_factory(multi_dbs['core_9'].dbc.url, multi_dbs['ensembl_genome_metadata'].dbc.url,
                            multi_dbs['ncbi_taxonomy'].dbc.url, force=True)
        test.process_core()
        metadata_db = DBConnection(multi_dbs['ensembl_genome_metadata'].dbc.url)
        with metadata_db.session_scope() as session:
            # Test that assembly seqs have not been updated
            new_seq = session.query(AssemblySequence).where(
                (AssemblySequence.name == 'TEST1_seq_BAD')).first()
            assert new_seq is None
            old_seq = session.query(AssemblySequence).where(
                (AssemblySequence.accession == 'MtDNA')).first()
            assert old_seq is not None
            # Check that the old datasets have been removed

            count = session.query(Dataset).join(DatasetSource).join(DatasetType).filter(
                DatasetSource.name.like('%core_7'),
                DatasetType.name == 'assembly'
            ).count()
            assert count == 0
            # Check that the new datasets exist
            count = session.query(Dataset).join(DatasetSource).join(DatasetType).filter(
                DatasetSource.name.like('%core_9'),
                DatasetType.name == 'assembly'
            ).count()
            assert count == 1
            # Check that the old attributes are gone
            count = session.query(DatasetAttribute).join(Attribute).filter(
                Attribute.name == 'assembly.total_coding_sequence_length',
                DatasetAttribute.value == '8989'
            ).count()
            assert count == 0
            count = session.query(DatasetAttribute).join(Attribute).filter(
                Attribute.name == 'genebuild.havana_datafreeze_date',
                DatasetAttribute.value == 'test2'
            ).count()
            assert count == 1
