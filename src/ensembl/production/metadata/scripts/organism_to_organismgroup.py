import os
import argparse
import logging

from ensembl.core.models import Meta
from ensembl.utils.database import DBConnection
from ensembl.production.metadata.api.models.organism import OrganismGroup, OrganismGroupMember, Organism
from ensembl.production.metadata.api.models.genome import Genome, GenomeDataset, GenomeRelease
from ensembl.production.metadata.api.models.dataset import Dataset, DatasetSource

# Set up the logging configuration
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("assign_organism_to_organismgroup_type.log", mode="w"),
        logging.StreamHandler()
    ]
)


def fetch_division_name(core_db_uri: str) -> str:
    """
    Fetch the division name from the core database.
    """
    with DBConnection(core_db_uri).session_scope() as session:
        query = session.query(Meta).filter(Meta.meta_key == 'species.division').one_or_none()
        return query.meta_value if query else None


def create_or_remove_organism_group(session, organism_id: int, organism_group_id: int, remove: bool = False) -> str:
    """
    Create or remove an organism group member based on the provided parameters.
    """
    try:
        organism_group_member = session.query(OrganismGroupMember).filter(
            OrganismGroupMember.organism_id == organism_id,
            OrganismGroupMember.organism_group_id == organism_group_id
        ).one_or_none()

        if remove:
            if organism_group_member:
                session.delete(organism_group_member)
                msg = f"Organism group member removed successfully for organism group {organism_group_id} and organism {organism_id}"
            else:
                msg = f"Organism group member not found for organism group {organism_group_id} and organism {organism_id}"
        else:
            if not organism_group_member:
                organism_group_member = OrganismGroupMember(
                    organism_id=organism_id,
                    organism_group_id=organism_group_id
                )
                session.add(organism_group_member)
                msg = f"Organism group member created successfully for organism group {organism_group_id} and organism {organism_id}"
            else:
                msg = f"Organism group member already exists for organism group {organism_group_id} and organism {organism_id}"

        session.flush()
        logging.info(msg)
        return msg
    except Exception as e:
        logging.error(f"Error in create_or_remove_organism_group: {e}")
        raise


def process_genomes(session, args, organism_group_id: int = None):
    """
    Process genomes based on the provided arguments and assign/remove them to/from organism groups.
    """
    query = (
        session.query(Genome, DatasetSource)
        .join(GenomeDataset, Genome.genome_id == GenomeDataset.genome_id)
        .join(Dataset, GenomeDataset.dataset_id == Dataset.dataset_id)
        .join(DatasetSource, Dataset.dataset_source_id == DatasetSource.dataset_source_id)
        .join(GenomeRelease, Genome.genome_id == GenomeRelease.genome_id)
        .filter(Dataset.name == 'genebuild')
    )

    if args.release_id:
        query = query.filter(GenomeRelease.release_id.in_(args.release_id))
    if args.genome_uuid:
        query = query.filter(Genome.genome_uuid.in_(args.genome_uuid))
    if args.remove and args.organism_group_type and args.organism_group_name:
        query = query.join(Organism, Organism.organism_id == Genome.organism_id
                           ).join(OrganismGroupMember,
                                  OrganismGroupMember.organism_id == Organism.organism_id
                                  ).join(OrganismGroup,
                                         OrganismGroup.organism_group_id == OrganismGroupMember.organism_group_id
                                         ).filter(OrganismGroup.name == args.organism_group_name,
                                                  OrganismGroup.type == args.organism_group_type)

    for genome, dataset_source in query.all():
        logging.info(f"Processing genome {genome.genome_uuid} for organism {genome.organism_id}")
        if not (args.organism_group_type and args.organism_group_name) and args.core_server_uri:
            division_name = fetch_division_name(os.path.join(args.core_server_uri, dataset_source.name))
            if division_name:
                organism_group = session.query(OrganismGroup).filter(OrganismGroup.name == division_name).one_or_none()
                if organism_group:
                   organism_group_id = organism_group.organism_group_id

        if organism_group_id is None:
            logging.warning(f"Organism group ID is None for genome {genome.genome_uuid}")
            raise

        create_or_remove_organism_group(session, genome.organism_id, organism_group_id, remove=args.remove)


def main():
    parser = argparse.ArgumentParser(
        prog="update_organism_to_organismgroup.py",
        description="Script to assign/remove organisms to/from organism groups."
    )
    parser.add_argument("--metadata_db_uri", type=str, required=True, help="Metadata DB URI")
    parser.add_argument("--core_server_uri", type=str, help="Core DB URI")
    parser.add_argument("--organism_group_type", type=str, help="Organism group type")
    parser.add_argument("--organism_group_name", type=str, help="Organism group name")
    parser.add_argument("--genome_uuid", nargs="*", default=[], help="List of genome UUIDs")
    parser.add_argument("--release_id", nargs="*", default=[], help="List of release IDs")
    parser.add_argument("--remove", action="store_true", help="Remove organisms from the group")
    parser.add_argument("--raise_error", action="store_true", help="Raise errors without committing changes")

    args = parser.parse_args()

    if not args.remove and not args.genome_uuid and not args.release_id:
        raise ValueError("Provide either genome_uuid or release_id.")
    if not args.core_server_uri and (not args.organism_group_type or not args.organism_group_name):
        raise ValueError("Provide core_server_uri or (organism_group_type and organism_group_name).")

    logging.info(f"Starting script with args: {args}")

    try:
        with DBConnection(args.metadata_db_uri).session_scope() as session:
            organism_group_id = None
            if args.organism_group_type and args.organism_group_name:
                organism_group = session.query(OrganismGroup).filter(
                    OrganismGroup.name == args.organism_group_name,
                    OrganismGroup.type == args.organism_group_type
                ).one_or_none()
                if not organism_group:
                    raise ValueError(
                        f"Organism group {args.organism_group_name} of type {args.organism_group_type} does not exist."
                    )
                organism_group_id = organism_group.organism_group_id

            process_genomes(session, args, organism_group_id=organism_group_id)
    except Exception as e:
        logging.error(f"Error occurred: {e}")
        if args.raise_error:
            raise


if __name__ == "__main__":
    main()
