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
from __future__ import annotations

from ensembl.utils.database import DBConnection

from . import EnsemblRelease, ReleaseStatus
from .dataset import Dataset
from ...grpc.config import cfg


def check_release_status(meta_dbc, dataset_uuid):
    with meta_dbc.session_scope() as session:
        # Query to check if a release_id exists for the given genome_uuid
        dataset_id = session.query(Dataset.dataset_id).filter(Dataset.dataset_uuid == dataset_uuid).scalar()
        if dataset_id is None:
            return "UUID not found"

        # Now we check if there exists a genome dataset with the corresponding dataset_id and a non-null release_id
        result = session.query(
            session.query(Dataset).filter(Dataset.dataset_id == dataset_id).filter(
                Dataset.status == "Released").exists()
        ).scalar()
        return result


def get_or_new_release(meta_dbc: str, skip_release: EnsemblRelease = None) -> EnsemblRelease:
    db = DBConnection(meta_dbc)
    with db.session_scope() as session:
        next_release = session.query(EnsemblRelease).filter(
            EnsemblRelease.status == ReleaseStatus.PLANNED).order_by(EnsemblRelease.version)
        if skip_release:
            next_release = next_release.filter(EnsemblRelease.release_id != skip_release.release_id)
        next_release = next_release.one_or_none()
        if next_release is None:
            top_release = session.query(EnsemblRelease).order_by(EnsemblRelease.version.desc()).first()
            next_release = EnsemblRelease(
                status=ReleaseStatus.PLANNED,
                is_current=0,
                label=f"New Release (Automated from {top_release.version}",
                release_type=top_release.release_type,
                version=float(top_release.version) + float(0.1),
                site_id=cfg.ensembl_site_id
            )
        import logging
        logging.debug(f"Next release {next_release}")
        session.add(next_release)
        session.expire_on_commit = False
        return next_release
