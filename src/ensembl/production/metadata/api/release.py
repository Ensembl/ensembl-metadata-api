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
import sqlalchemy as db

from ensembl.production.metadata.api.base import check_parameter, BaseAdaptor
from ensembl.production.metadata.models import EnsemblRelease, EnsemblSite, GenomeRelease, Genome, GenomeDataset, \
    Dataset


class ReleaseAdaptor(BaseAdaptor):

    def fetch_releases(
            self,
            release_id=None,
            release_version=None,
            current_only=True,
            release_type=None,
            site_name=None,
    ):
        release_id = check_parameter(release_id)
        release_version = check_parameter(release_version)
        release_type = check_parameter(release_type)
        site_name = check_parameter(site_name)

        release_select = db.select(
            EnsemblRelease, EnsemblSite
        ).join(EnsemblRelease.ensembl_site)

        # WHERE ensembl_release.release_id = :release_id_1
        if release_id is not None:
            release_select = release_select.filter(
                EnsemblRelease.release_id.in_(release_id)
            )
        # WHERE ensembl_release.version = :version_1
        elif release_version is not None:
            release_select = release_select.filter(
                EnsemblRelease.version.in_(release_version)
            )
        # WHERE ensembl_release.is_current =:is_current_1
        elif current_only:
            release_select = release_select.filter(
                EnsemblRelease.is_current == 1
            )

        # WHERE ensembl_release.release_type = :release_type_1
        if release_type is not None:
            release_select = release_select.filter(
                EnsemblRelease.release_type.in_(release_type)
            )

        # WHERE ensembl_site.name = :name_1
        if site_name is not None:
            release_select = release_select.filter(
                EnsemblSite.name.in_(site_name)
            )
        with self.metadata_db.session_scope() as session:
            session.expire_on_commit = False
            return session.execute(release_select).all()

    def fetch_releases_for_genome(self, genome_uuid, site_name=None):

        # SELECT genome_release.release_id
        # FROM genome_release
        # JOIN genome ON genome.genome_id = genome_release.genome_id
        # WHERE genome.genome_uuid =:genome_uuid_1
        release_id_select = db.select(
            GenomeRelease.release_id
        ).filter(
            Genome.genome_uuid == genome_uuid
        ).join(
            GenomeRelease.genome
        )

        release_ids = []
        with self.metadata_db.session_scope() as session:
            release_objects = session.execute(release_id_select).all()
            for rid in release_objects:
                release_ids.append(rid[0])
            release_ids = list(dict.fromkeys(release_ids))
        return self.fetch_releases(release_id=release_ids, site_name=site_name)

    def fetch_releases_for_dataset(self, dataset_uuid, site_name=None):

        # SELECT genome_release.release_id
        # FROM genome_dataset
        # JOIN dataset ON dataset.dataset_id = genome_dataset.dataset_id
        # WHERE dataset.dataset_uuid = :dataset_uuid_1
        release_id_select = db.select(
            GenomeDataset.release_id
        ).filter(
            Dataset.dataset_uuid == dataset_uuid
        ).join(
            GenomeDataset.dataset
        )

        release_ids = []
        with self.metadata_db.session_scope() as session:
            release_objects = session.execute(release_id_select).all()
            for rid in release_objects:
                release_ids.append(rid[0])
            release_ids = list(dict.fromkeys(release_ids))
        return self.fetch_releases(release_id=release_ids, site_name=site_name)


class NewReleaseAdaptor(BaseAdaptor):

    def __init__(self, metadata_uri=None):
        super().__init__(metadata_uri)
        # Get current release ID from ensembl_release
        with self.metadata_db.session_scope() as session:
            self.current_release_id = (
                session.execute(db.select(EnsemblRelease.release_id).filter(EnsemblRelease.is_current == 1)).one()[0])
        if self.current_release_id == "":
            raise Exception("Current release not found")
        #   print (self.current_release_id)

        # Get last release ID from ensembl_release
        with self.metadata_db.session_scope() as session:
            ############### Refactor this once done. It is messy.
            current_version = int(session.execute(
                db.select(EnsemblRelease.version).filter(EnsemblRelease.release_id == self.current_release_id)).one()[
                                      0])
            past_versions = session.execute(
                db.select(EnsemblRelease.version).filter(EnsemblRelease.version < current_version)).all()
            sorted_versions = []
            # Do I have to account for 1.12 and 1.2
            for version in past_versions:
                sorted_versions.append(float(version[0]))
            sorted_versions.sort()
            self.previous_release_id = (session.execute(
                db.select(EnsemblRelease.release_id).filter(EnsemblRelease.version == sorted_versions[-1])).one()[0])
            if self.previous_release_id == "":
                raise Exception("Previous release not found")

    #     new_genomes (list of new genomes in the new release)
    def fetch_new_genomes(self):
        with self.metadata_db.session_scope() as session:
            genome_selector = db.select(
                EnsemblRelease, EnsemblSite
            ).join(EnsemblRelease.ensembl_site)
            old_genomes = session.execute(
                db.select(EnsemblRelease.version).filter(EnsemblRelease.version < current_version)).all()
        new_genomes = []
        novel_old_genomes = []
        novel_new_genomes = []

        return session.execute(release_select).all()
