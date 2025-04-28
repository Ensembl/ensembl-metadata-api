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

import argparse
import subprocess

from ensembl.utils.database import DBConnection
from sqlalchemy.orm import joinedload

from ensembl.production.metadata.api.models import Genome


# TO BE RUN ON CODON.
# This script will submit a job to SLURM to delete data from the FTP server.

def submit_slurm_job(paths, test=False):
    if not paths:
        print("No paths to delete.")
        return

    delete_command = "rm -rf " + " ".join(paths)
    slurm_command = f"sbatch -t 1:00:00 --mem=1G -p datamover --wrap='{delete_command}'"

    if test:
        print(f"[TEST MODE] Would run:\n{slurm_command}")
    else:
        print(f"Submitting SLURM job to delete the following paths:\n{delete_command}")
        subprocess.run(["sbatch", "--wrap", delete_command])


def generate_full_paths(relative_paths, ftp_root, nfs_root):
    """Create full paths for FTP and NFS locations for each relative path."""
    full_paths = []
    for rel_path in relative_paths:
        full_paths.append(f"{ftp_root}{rel_path}")
        full_paths.append(f"{nfs_root}{rel_path}")
    return full_paths


def main(meta_uri, genome_uuid, dataset_type="all", test=False):
    ftp_root = "/nfs/ftp/public/ensemblorganisms/"
    nfs_root = "/hps/nobackup/flicek/ensembl/production/ensembl_dumps/ftp_mvp/organisms/"

    metadata_db = DBConnection(meta_uri)
    with metadata_db.session_scope() as session:
        delete_rel_paths = []

        genome = (
            session.query(Genome)
            .options(
                joinedload(Genome.organism),
                joinedload(Genome.assembly),
                joinedload(Genome.genome_datasets),
            )
            .filter(Genome.genome_uuid == genome_uuid)
            .one()
        )

        organism = genome.organism
        assembly = genome.assembly

        organism_dir = organism.scientific_name.replace(" ", "_")
        assembly_dir = f"{organism_dir}/{assembly.accession}"

        if dataset_type == "all":
            # Check for shared organism
            other_genomes_same_organism = (
                session.query(Genome)
                .filter(Genome.organism_id == organism.organism_id, Genome.genome_uuid != genome_uuid)
                .count()
            )

            if other_genomes_same_organism == 0:
                # No other genomes, delete whole organism directory
                delete_rel_paths.append(organism_dir)
            else:
                # Check for shared assembly
                other_genomes_same_assembly = (
                    session.query(Genome)
                    .filter(Genome.assembly_id == assembly.assembly_id, Genome.genome_uuid != genome_uuid)
                    .count()
                )

                if other_genomes_same_assembly == 0:
                    # No other genomes using this assembly, delete whole assembly directory
                    delete_rel_paths.append(assembly_dir)
                else:
                    # Assembly is shared, so only delete this genome's dataset-specific dirs
                    links = genome.get_public_path(dataset_type=dataset_type)
                    for link in links:
                        if not link.endswith("genome"):
                            delete_rel_paths.extend(link)
        else:
            # deleting one dataset type — always just get that dataset's path
            links = genome.get_public_path(dataset_type=dataset_type)
            delete_rel_paths.extend(link["path"] for link in links)

    # Expand to FTP + NFS paths and sort in reverse to handle deepest paths first
    all_delete_paths = generate_full_paths(sorted(set(delete_rel_paths), reverse=True), ftp_root, nfs_root)

    submit_slurm_job(all_delete_paths, test=test)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Delete genome FTP directories based on metadata")
    parser.add_argument("--meta-uri", required=True, help="Metadata DB connection URI")
    parser.add_argument("--genome-uuid", required=True, help="UUID of the genome to delete")
    parser.add_argument("--dataset-type", default="all", help="Dataset type to delete (default: all)")
    parser.add_argument("--test", action="store_true", help="Only print what would be deleted (no SLURM submission)")

    args = parser.parse_args()
    main(args.meta_uri, args.genome_uuid, args.dataset_type, args.test)
