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
