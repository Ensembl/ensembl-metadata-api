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

from typing import Optional, List

from pydantic import BaseModel
from sqlalchemy.orm import Session, joinedload

from ensembl.production.metadata.api.models import (
    Genome, Dataset, DatasetAttribute,
    EnsemblRelease, GenomeRelease, GenomeDataset, Attribute,
    ReleaseStatus
)


# ============================================================================
# PYDANTIC SCHEMAS
# ============================================================================

class GenomeSearchDocument(BaseModel):
    """Schema for genome search indexing"""

    # Direct fields from Genome/Organism/Assembly
    genome_uuid: str
    common_name: Optional[str] = None
    scientific_name: str
    strain_type: Optional[str] = None
    strain: Optional[str] = None
    assembly_name: str
    accession: str
    url_name: Optional[str] = None
    tol_id: Optional[str] = None
    is_reference: bool
    species_taxonomy_id: int
    taxonomy_id: int
    scientific_parlance_name: Optional[str] = None
    organism_id: int
    rank: int = 0

    # Complex derived fields from datasets
    contig_n50: int
    coding_genes: int
    has_variation: bool = False
    has_regulation: bool = False
    genebuild_provider: str
    genebuild_method_display: str

    # Release information
    release_type: str
    release_label: str
    release_id: int

    class Config:
        from_attributes = True


# ============================================================================
# QUERY HELPER CLASS
# ============================================================================

class GenomeSearchQueryHelper:
    """Handles complex queries for extracting genome search data"""

    def __init__(self, session: Session):
        self.session = session

    def _get_dataset_attribute_value(
            self,
            genome_id: int,
            release_id: int,
            release_type: str,
            dataset_type_name: str,
            attribute_name: str
    ) -> Optional[str]:
        """
        Get dataset attribute value with complex release logic

        If release is integrated: use dataset with matching release_id
        If release is partial: use is_current dataset
        """
        # Build the query
        query = (
            self.session.query(DatasetAttribute.value)
            .join(Attribute, DatasetAttribute.attribute_id == Attribute.attribute_id)
            .join(Dataset, DatasetAttribute.dataset_id == Dataset.dataset_id)
            .join(Dataset.dataset_type)
            .join(GenomeDataset, Dataset.dataset_id == GenomeDataset.dataset_id)
            .filter(
                GenomeDataset.genome_id == genome_id,
                Dataset.dataset_type.has(name=dataset_type_name),
                Attribute.name == attribute_name
            )
        )

        # Apply release-specific filtering
        if release_type == 'integrated':
            query = query.filter(GenomeDataset.release_id == release_id)
        else:  # partial
            query = query.filter(GenomeDataset.is_current == 1)

        result = query.first()
        return result[0] if result else None

    def _has_dataset_type(
            self,
            genome_id: int,
            release_id: int,
            release_type: str,
            dataset_type_name: str
    ) -> bool:
        """Check if genome has a dataset of specific type"""
        query = (
            self.session.query(GenomeDataset)
            .join(Dataset, GenomeDataset.dataset_id == Dataset.dataset_id)
            .join(Dataset.dataset_type)
            .filter(
                GenomeDataset.genome_id == genome_id,
                Dataset.dataset_type.has(name=dataset_type_name)
            )
        )

        if release_type == 'integrated':
            query = query.filter(GenomeDataset.release_id == release_id)
        else:  # partial
            query = query.filter(GenomeDataset.is_current == 1)

        return self.session.query(query.exists()).scalar()

    def _get_genebuild_provider(
            self,
            genome: Genome,
            release_id: int,
            release_type: str
    ) -> Optional[str]:
        """Get genebuild provider with fallback logic"""
        # Try to get from dataset attribute first
        provider = self._get_dataset_attribute_value(
            genome.genome_id,
            release_id,
            release_type,
            'genebuild',
            'genebuild.provider_name_display'
        )

        # Fallback to genome.provider_name
        return provider if provider else genome.provider_name

    def extract_genome_data(
            self,
            genome: Genome,
            release: EnsemblRelease
    ) -> dict:
        """Extract all required data from genome for given release"""

        return {
            # Direct fields
            'genome_uuid': genome.genome_uuid,
            'common_name': genome.organism.common_name,
            'scientific_name': genome.organism.scientific_name,
            'strain_type': genome.organism.strain_type,
            'strain': genome.organism.strain,
            'assembly_name': genome.assembly.name,
            'accession': genome.assembly.accession,
            'url_name': genome.url_name,
            'tol_id': genome.organism.tol_id,
            'is_reference': bool(genome.assembly.is_reference),
            'species_taxonomy_id': genome.organism.species_taxonomy_id,
            'scientific_parlance_name': genome.organism.scientific_parlance_name,
            'organism_id': genome.organism_id,
            'rank': genome.organism.rank or 0,

            # Complex dataset fields
            'contig_n50': self._get_dataset_attribute_value(
                genome.genome_id, release.release_id, release.release_type,
                'assembly', 'assembly.stats.contig_n50'
            ),
            'coding_genes': self._get_dataset_attribute_value(
                genome.genome_id, release.release_id, release.release_type,
                'genebuild', 'genebuild.stats.coding_genes'
            ),
            'has_variation': self._has_dataset_type(
                genome.genome_id, release.release_id, release.release_type,
                'variation'
            ),
            'has_regulation': self._has_dataset_type(
                genome.genome_id, release.release_id, release.release_type,
                'regulatory_features'
            ),
            'genebuild_provider': self._get_genebuild_provider(
                genome, release.release_id, release.release_type
            ),
            'genebuild_method_display': self._get_dataset_attribute_value(
                genome.genome_id, release.release_id, release.release_type,
                'genebuild', 'genebuild.method_display'
            ),

            # Release fields
            'release_type': release.release_type,
            'release_label': release.label,
            'release_id': release.release_id,
        }


# ============================================================================
# MAIN SERVICE CLASS
# ============================================================================

class GenomeSearchIndexer:
    """Service for generating genome search documents"""

    def __init__(self, session: Session):
        self.session = session
        self.query_helper = GenomeSearchQueryHelper(session)

    def _get_relevant_release(self, genome: Genome) -> Optional[EnsemblRelease]:
        """
        Determine which release to use for a genome.
        Prefer partial if exists, otherwise use integrated.
        """
        releases = (
            self.session.query(EnsemblRelease)
            .join(GenomeRelease)
            .filter(
                GenomeRelease.genome_id == genome.genome_id,
                EnsemblRelease.status == ReleaseStatus.RELEASED
            )
            .all()
        )

        # Check for partial release
        partial_releases = [r for r in releases if r.release_type == 'partial']
        if partial_releases:
            return partial_releases  # Should only be one, but return list for consistency

        # Return all integrated releases
        integrated_releases = [r for r in releases if r.release_type == 'integrated']
        return integrated_releases if integrated_releases else None

    def get_released_genomes(self) -> List[Genome]:
        """Get all genomes that are released"""
        return (
            self.session.query(Genome)
            .join(GenomeRelease)
            .join(EnsemblRelease)
            .filter(
                EnsemblRelease.status == ReleaseStatus.RELEASED,
                Genome.suppressed == 0
            )
            .options(
                joinedload(Genome.organism),
                joinedload(Genome.assembly),
                joinedload(Genome.genome_releases).joinedload(GenomeRelease.ensembl_release)
            )
            .distinct()
            .all()
        )

    def create_search_documents(
            self,
            genome: Genome
    ) -> List[GenomeSearchDocument]:
        """
        Create search documents for a genome.
        Returns list because a genome can be in multiple integrated releases.
        """
        releases = self._get_relevant_release(genome)

        if not releases:
            return []

        # Ensure releases is a list
        if not isinstance(releases, list):
            releases = [releases]

        documents = []
        for release in releases:
            genome_data = self.query_helper.extract_genome_data(genome, release)
            documents.append(GenomeSearchDocument(**genome_data))

        return documents

    def generate_all_search_documents(self) -> List[GenomeSearchDocument]:
        """Generate search documents for all released genomes"""
        genomes = self.get_released_genomes()
        all_documents = []

        for genome in genomes:
            documents = self.create_search_documents(genome)
            all_documents.extend(documents)

        return all_documents

    def generate_search_documents_as_dicts(self) -> List[dict]:
        """Generate search documents as dictionaries for indexing"""
        documents = self.generate_all_search_documents()
        return [doc.model_dump() for doc in documents]


# ============================================================================
# USAGE EXAMPLES
# ============================================================================

def index_genomes_for_search(session: Session):
    """Main entry point for generating search index data"""
    indexer = GenomeSearchIndexer(session)

    # Get all documents as dicts ready for search indexing
    search_documents = indexer.generate_search_documents_as_dicts()

    # Send to your search service (Elasticsearch, Solr, etc.)
    # send_to_search_index(search_documents)

    return search_documents


def index_single_genome(session: Session, genome_uuid: str):
    """Index a specific genome"""
    indexer = GenomeSearchIndexer(session)

    genome = (
        session.query(Genome)
        .filter(Genome.genome_uuid == genome_uuid)
        .options(
            joinedload(Genome.organism),
            joinedload(Genome.assembly)
        )
        .first()
    )

    if not genome:
        raise ValueError(f"Genome {genome_uuid} not found")

    documents = indexer.create_search_documents(genome)
    return [doc.model_dump() for doc in documents]
