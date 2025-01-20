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

from sqlalchemy import and_
from sqlalchemy.orm import aliased

from ensembl.production.metadata.api.adaptors.base import BaseAdaptor
from ensembl.production.metadata.api.models import Organism, Assembly, DatasetAttribute, Genome, GenomeDataset, Dataset


class VepAdaptor(BaseAdaptor):
    def __init__(self, metadata_uri: str, file="all"):
        super().__init__(metadata_uri)
        self.file = file

    def fetch_vep_locations(self, genome_uuid):
        """
        Fetches the FAA and GFF file locations for a given genome UUID.

        :param genome_uuid: The UUID of the genome to fetch locations for.
        :return: A dictionary containing the FAA and GFF locations or a specific location string if 'file' is set.
        """
        with self.metadata_db.session_scope() as session:
            # Aliases for clarity and distinct filtering
            annotation_source_attr = aliased(DatasetAttribute)
            last_geneset_update_attr = aliased(DatasetAttribute)

            query = (
                session.query(
                    Organism.scientific_name,
                    Assembly.accession.label("assembly_accession"),
                    annotation_source_attr.value.label("annotation_source"),
                    last_geneset_update_attr.value.label("last_geneset_update"),
                )
                .join(Genome, Genome.organism_id == Organism.organism_id)
                .join(Assembly, Assembly.assembly_id == Genome.assembly_id)
                .join(GenomeDataset, GenomeDataset.genome_id == Genome.genome_id)
                .join(Dataset, Dataset.dataset_id == GenomeDataset.dataset_id)
                .outerjoin(
                    annotation_source_attr,
                    and_(
                        annotation_source_attr.dataset_id == Dataset.dataset_id,
                        annotation_source_attr.attribute.has(name="genebuild.annotation_source"),
                    ),
                )
                .outerjoin(
                    last_geneset_update_attr,
                    and_(
                        last_geneset_update_attr.dataset_id == Dataset.dataset_id,
                        last_geneset_update_attr.attribute.has(name="genebuild.last_geneset_update"),
                    ),
                )
                .filter(
                    Genome.genome_uuid == genome_uuid,
                    Dataset.name == "genebuild",
                )
            )

            result = query.one_or_none()

            if not result:
                raise ValueError(f"No data found for genome UUID: {genome_uuid}")
            elif not result.annotation_source or not result.last_geneset_update:
                raise ValueError(f"Missing annotation source or last geneset update for genome UUID: {genome_uuid}")

            # Format the scientific name
            scientific_name = result.scientific_name
            scientific_name = re.sub(r"[^a-zA-Z0-9]+", " ", scientific_name)
            scientific_name = re.sub(r" +", "_", scientific_name).strip("_")

            # Format last geneset update
            last_geneset_update = re.sub(r"-", "_", result.last_geneset_update)

            # Construct the locations
            faa_location = f"{scientific_name}/{result.assembly_accession}/vep/genome/softmasked.fa.bgz"
            gff_location = (
                f"{scientific_name}/{result.assembly_accession}/vep/"
                f"{result.annotation_source}/geneset/{last_geneset_update}/genes.gff3.bgz"
            )

            # Return based on the `file` argument
            if self.file == "faa_location":
                return faa_location
            elif self.file == "gff_location":
                return gff_location
            else:
                return {"faa_location": faa_location, "gff_location": gff_location}
