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

from ensembl.utils.database import DBConnection

from ensembl.production.metadata.api.adaptors.base import BaseAdaptor
from ensembl.production.metadata.api.models import Assembly, Genome


class VepAdaptor(BaseAdaptor):
    def __init__(self, metadata_uri: str | DBConnection, file="all"):
        super().__init__(metadata_uri)
        self.file = file

    def fetch_vep_locations(self, genome_uuid):
        """
        Fetches the FAA and GFF file locations for a given genome UUID.

        :param genome_uuid: The UUID of the genome to fetch locations for.
        :return: A dictionary containing the FAA and GFF locations or a specific location string if 'file' is set.
        """
        with self.metadata_db.session_scope() as session:

            query = (
                session.query(Assembly.assembly_uuid)
                .join(Genome, Genome.assembly_id == Assembly.assembly_id)
                .filter(
                    Genome.genome_uuid == genome_uuid,
                )
                .distinct()  # Should be unnecesary.
            )

            result = query.one_or_none()

            if not result:
                raise ValueError(f"No data found for genome UUID: {genome_uuid}")

            def format_uuid(uuid):
                prefix = uuid[:3]
                uuid = uuid.replace("-", "")
                return f"{prefix}/{uuid}"

            formatted_genome_uuid = genome_uuid.replace("-", "")

            formatted_assembly_uuid = format_uuid(result.assembly_uuid)

            # Construct the locations
            faa_location = f"{formatted_assembly_uuid}/softmasked.fa.bgz"
            gff_location = f"{formatted_assembly_uuid}/{formatted_genome_uuid}/genes.gff3.bgz"

            # Return based on the `file` argument
            if self.file == "faa_location":
                return faa_location
            elif self.file == "gff_location":
                return gff_location
            else:
                return {"faa_location": faa_location, "gff_location": gff_location}
