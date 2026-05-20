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

import logging

from .dataset import Dataset
from .genome import GenomeDataset


def check_release_status(meta_dbc, dataset_uuid):
    with meta_dbc.session_scope() as session:
        # Query to check if a release_id exists for the given genome_uuid
        dataset_id = session.query(Dataset.dataset_id).filter(Dataset.dataset_uuid == dataset_uuid).scalar()
        if dataset_id is None:
            return "UUID not found"

        # Now we check if there exists a genome dataset with the corresponding dataset_id and a non-null release_id
        result = session.query(
            session.query(GenomeDataset).filter(GenomeDataset.dataset_id == dataset_id,
                                                GenomeDataset.ensembl_release != None).exists()
        ).scalar()
        return result


def fetch_proper_dataset(genome_datasets):
    """
    Selects the appropriate dataset(s) based on release type.

    Logic:
      - If more than one dataset is available, prefer those with a partial release.
      - If only one dataset is available, return it directly.

    Reference: https://genomes-ebi.slack.com/archives/C010QF119N1/p1746094298003789
    """
    if not genome_datasets:
        logging.warning("No genome datasets provided.")
        return []

    # Multiple datasets: prefer partial releases
    if len(genome_datasets) > 1:
        filtered = []
        for ds in genome_datasets:
            try:
                if (
                        ds.is_current
                        and ds.dataset.status.value == 'Released'
                        and getattr(ds.ensembl_release, "release_type", None) == 'partial'
                ):
                    filtered.append(ds)
            except Exception as e:
                logging.error(
                    f"Error processing dataset {getattr(ds, 'id', repr(ds))}: {e}",
                    exc_info=True
                )

        if filtered:
            logging.debug(f"Filtered partial datasets: {filtered}")
            return filtered
        else:
            logging.warning(
                "Multiple datasets provided but no partial release found. Returning all."
            )

    logging.debug(f"Returning dataset(s): {genome_datasets}")
    return genome_datasets
