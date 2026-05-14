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
from sqlalchemy import or_, func, distinct

from ensembl.production.metadata.api.models import Genome, GenomeRelease, EnsemblRelease, ReleaseStatus


def _live_genome_base_query(session):
    """
    Shared base query for live genomes:
        ensembl_release.status = 'Released'
        AND (genome_release.is_current = 1 OR ensembl_release.release_type = 'integrated')
    """
    return (
        session.query(Genome)
        .join(GenomeRelease, GenomeRelease.genome_id == Genome.genome_id)
        .join(EnsemblRelease, EnsemblRelease.release_id == GenomeRelease.release_id)
        .filter(
            EnsemblRelease.status == ReleaseStatus.RELEASED,
            or_(
                GenomeRelease.is_current == 1,
                EnsemblRelease.release_type == "integrated",
            ),
        )
        .distinct()
    )


def get_all_live_genomes_count(session):
    """
    Returns the number of genomes that should be live.

    :param session: SQLAlchemy session object
    :return: int: number of live genomes
    """
    return (
        session.query(func.count(distinct(Genome.genome_uuid)))
        .join(GenomeRelease, GenomeRelease.genome_id == Genome.genome_id)
        .join(EnsemblRelease, EnsemblRelease.release_id == GenomeRelease.release_id)
        .filter(
            EnsemblRelease.status == ReleaseStatus.RELEASED,
            or_(
                GenomeRelease.is_current == 1,
                EnsemblRelease.release_type == "integrated",
            ),
        )
        .scalar()
    )


def get_all_live_genomes(session):
    """
    Returns all Genome objects that should be live.

    :param session: SQLAlchemy session object
    :return: list[Genome]: list of live Genome ORM objects
    """
    return _live_genome_base_query(session).all()
