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

from ensembl.production.metadata.api.adaptors.base import BaseAdaptor
from ensembl.production.metadata.api.adaptors.genome import GenomeAdaptor

class VepAdaptor(BaseAdaptor):
    def __init__(self, metadata_uri: str, file="all"):
        super().__init__(metadata_uri)
        self.metadata_uri = metadata_uri
        self.file = file

    def fetch_vep_locations(self, genome_uuid):
        """
        Fetches the FAA and GFF file locations for a given genome UUID.

        :param genome_uuid: The UUID of the genome to fetch locations for.
        :return: A dictionary containing the FAA and GFF locations or a specific location string if 'file' is set.
        """
        genome_adaptor = GenomeAdaptor(self.metadata_uri)
        genebuild_path = genome_adaptor.get_public_path(genome_uuid, dataset_type='genebuild')
        genebuild_path = genebuild_path[0]['path']
        assembly_path = genome_adaptor.get_public_path(genome_uuid, dataset_type='assembly')
        assembly_path = assembly_path[0]['path']

        faa_location = f"{assembly_path}/unmasked.fa.bgz"
        gff_location = f"{genebuild_path}/genes.gff3.bgz"
        # Return based on the `file` argument
        if self.file == "faa_location":
            return faa_location
        elif self.file == "gff_location":
            return gff_location
        else:
            return {"faa_location": faa_location, "gff_location": gff_location}
