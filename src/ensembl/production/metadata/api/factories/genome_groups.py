# See the NOTICE file distributed with this work for additional information
#   regarding copyright ownership.
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may obtain a copy of the License at
#       http://www.apache.org/licenses/LICENSE-2.0
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

import argparse
import csv
import logging
from pathlib import Path
from typing import Iterable

from ensembl.utils.database import DBConnection

from ensembl.production.metadata.api.models import Genome, GenomeGroup, GenomeGroupMember, EnsemblRelease

logger = logging.getLogger(__name__)


class GenomeGroupFactory:

    def __init__(self, conn_uri: str = None):
        self.conn_uri = conn_uri

    def _db_connection(self):
        if not self.conn_uri:
            raise ValueError("No connection URI provided")
        return DBConnection(self.conn_uri)

    @staticmethod
    def _normalize_csv_row(row: dict[str, str]) -> dict[str, str]:
        normalized = {key.strip().lower(): (value or "").strip() for key, value in row.items()}
        return {
            "production_name": normalized.get("production_name", ""),
            "genome_uuid": normalized.get("genome_uuid", ""),
        }

    @staticmethod
    def load_csv(csv_file: str) -> list[dict[str, str]]:
        path = Path(csv_file)
        if not path.exists():
            raise FileNotFoundError(f"CSV file '{csv_file}' does not exist")

        with path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle, skipinitialspace=True)
            if reader.fieldnames is None:
                raise ValueError("CSV file has no header row")

            expected_fields = {"production_name", "genome_uuid"}
            found_fields = {field.strip().lower() for field in reader.fieldnames}
            if not expected_fields.issubset(found_fields):
                raise ValueError(
                    "CSV file must contain columns: production_name, genome_uuid"
                )

            rows = [GenomeGroupFactory._normalize_csv_row(row) for row in reader]
        logger.debug("Loaded %d rows from CSV %s", len(rows), csv_file)
        return rows

    @staticmethod
    def _get_or_create_genome_group(
        session,
        group_name: str,
        group_type: str = "custom",
        label: str | None = None,
        searchable: bool = False,
        description: str | None = None,
    ) -> GenomeGroup:
        group = session.query(GenomeGroup).filter(GenomeGroup.name == group_name).one_or_none()
        if group is None:
            group = GenomeGroup(
                name=group_name,
                type=group_type,
                label=label or group_name,
                searchable=1 if searchable else 0,
                description=description,
            )
            session.add(group)
            session.flush()
            logger.info(
                "Created genome group '%s' (id=%s, type=%s, label=%s)",
                group_name,
                group.genome_group_id,
                group_type,
                group.label,
            )
        else:
            logger.info("Using existing genome group '%s' (id=%s)", group_name, group.genome_group_id)
        return group

    @staticmethod
    def _get_release(session, release_id: int | None = None) -> EnsemblRelease:
        if release_id is not None:
            release = session.query(EnsemblRelease).filter(EnsemblRelease.release_id == release_id).one_or_none()
            if release is None:
                raise ValueError(f"Release with id {release_id} not found")
            return release

        release = session.query(EnsemblRelease).filter(EnsemblRelease.is_current == 1).filter(EnsemblRelease.release_type == "partial").one_or_none()
        if release is None:
            raise ValueError("No current EnsemblRelease found")
        return release

    def add_genomes_from_csv(
        self,
        metadata_db_uri: str,
        genome_group_name: str,
        csv_file: str,
        release_id: int | None = None,
        group_type: str = "custom",
        label: str | None = None,
        searchable: bool = False,
        description: str | None = None,
    ) -> dict[str, int]:
        if metadata_db_uri and metadata_db_uri != self.conn_uri:
            self.conn_uri = metadata_db_uri

        rows = self.load_csv(csv_file)
        if not rows:
            logger.warning("CSV file %s contained no rows", csv_file)
            return {"added": 0, "skipped_missing": 0, "skipped_suppressed": 0, "skipped_invalid_uuid": 0}

        with self._db_connection().session_scope() as session:
            group = self._get_or_create_genome_group(
                session, genome_group_name, group_type=group_type, label=label, searchable=searchable, description=description
            )
            release = self._get_release(session, release_id)

            genome_uuid_map = {row["genome_uuid"] for row in rows if row["genome_uuid"]}
            genomes = {
                genome.genome_uuid: genome
                for genome in session.query(Genome).filter(Genome.genome_uuid.in_(genome_uuid_map)).all()
            }

            counts = {
                "added": 0,
                "skipped_missing": 0,
                "skipped_suppressed": 0,
                "skipped_invalid_uuid": 0,
                "activated": 0,
                "already_member": 0,
            }

            for index, row in enumerate(rows, start=1):
                genome_uuid = row["genome_uuid"]
                production_name = row["production_name"]
                if not genome_uuid:
                    logger.warning("Row %s missing genome_uuid, skipping", index)
                    counts["skipped_invalid_uuid"] += 1
                    continue

                genome = genomes.get(genome_uuid)
                if genome is None:
                    logger.warning(
                        "Row %s genome_uuid '%s' not found in genome table, skipping",
                        index,
                        genome_uuid,
                    )
                    counts["skipped_missing"] += 1
                    continue

                if genome.suppressed:
                    logger.warning(
                        "Row %s genome_uuid '%s' is suppressed, skipping",
                        index,
                        genome_uuid,
                    )
                    counts["skipped_suppressed"] += 1
                    continue

                if production_name and production_name != genome.production_name:
                    logger.warning(
                        "Row %s production_name '%s' does not match genome.production_name '%s' for genome_uuid '%s'",
                        index,
                        production_name,
                        genome.production_name,
                        genome_uuid,
                    )

                member = (
                    session.query(GenomeGroupMember)
                    .filter(
                        GenomeGroupMember.genome_id == genome.genome_id,
                        GenomeGroupMember.genome_group_id == group.genome_group_id,
                        GenomeGroupMember.release_id == release.release_id,
                    )
                    .one_or_none()
                )

                if member:
                    if member.is_current != 1:
                        member.is_current = 1
                        session.add(member)
                        counts["activated"] += 1
                        logger.info(
                            "Row %s activated existing membership for genome_uuid '%s' in group '%s' for release %s",
                            index,
                            genome_uuid,
                            genome_group_name,
                            release.release_id,
                        )
                    else:
                        counts["already_member"] += 1
                        logger.info(
                            "Row %s genome_uuid '%s' is already a member of group '%s' for release %s",
                            index,
                            genome_uuid,
                            genome_group_name,
                            release.release_id,
                        )
                    continue

                member = GenomeGroupMember(
                    genome_id=genome.genome_id,
                    genome_group_id=group.genome_group_id,
                    release_id=release.release_id,
                    is_current=1,
                )
                session.add(member)
                counts["added"] += 1
                logger.info(
                    "Row %s added genome_uuid '%s' to genome group '%s' (group_id=%s, release_id=%s)",
                    index,
                    genome_uuid,
                    genome_group_name,
                    group.genome_group_id,
                    release.release_id,
                )

            session.commit()
            logger.info(
                "Finished adding genomes to group '%s': added=%s, activated=%s, already_member=%s, skipped_missing=%s, skipped_suppressed=%s, skipped_invalid_uuid=%s",
                genome_group_name,
                counts["added"],
                counts["activated"],
                counts["already_member"],
                counts["skipped_missing"],
                counts["skipped_suppressed"],
                counts["skipped_invalid_uuid"],
            )
            return counts

    def remove_genomes_from_group(
        self,
        metadata_db_uri: str,
        genome_group_name: str,
        genome_uuids: Iterable[str],
        release_id: int | None = None,
    ) -> dict[str, int]:
        if metadata_db_uri and metadata_db_uri != self.conn_uri:
            self.conn_uri = metadata_db_uri

        genome_uuids = [uuid.strip() for uuid in genome_uuids if uuid and uuid.strip()]
        if not genome_uuids:
            raise ValueError("At least one genome_uuid is required to remove genomes from a group")

        with self._db_connection().session_scope() as session:
            group = session.query(GenomeGroup).filter(GenomeGroup.name == genome_group_name).one_or_none()
            if group is None:
                raise ValueError(f"Genome group '{genome_group_name}' does not exist")

            release = self._get_release(session, release_id)
            genomes = {
                genome.genome_uuid: genome
                for genome in session.query(Genome).filter(Genome.genome_uuid.in_(genome_uuids)).all()
            }

            counts = {"removed": 0, "not_found": 0, "not_member": 0}
            for genome_uuid in genome_uuids:
                genome = genomes.get(genome_uuid)
                if genome is None:
                    logger.warning("Genome_uuid '%s' not found, cannot remove from group '%s'", genome_uuid, genome_group_name)
                    counts["not_found"] += 1
                    continue

                member = (
                    session.query(GenomeGroupMember)
                    .filter(
                        GenomeGroupMember.genome_id == genome.genome_id,
                        GenomeGroupMember.genome_group_id == group.genome_group_id,
                        GenomeGroupMember.release_id == release.release_id,
                    )
                    .one_or_none()
                )
                if member is None:
                    logger.info(
                        "Genome_uuid '%s' is not a member of group '%s' for release %s",
                        genome_uuid,
                        genome_group_name,
                        release.release_id,
                    )
                    counts["not_member"] += 1
                    continue

                session.delete(member)
                counts["removed"] += 1
                logger.info(
                    "Removed genome_uuid '%s' from genome group '%s' for release %s",
                    genome_uuid,
                    genome_group_name,
                    release.release_id,
                )

            session.commit()
            logger.info(
                "Finished removing genomes from group '%s' for release %s: removed=%s, not_found=%s, not_member=%s",
                genome_group_name,
                release.release_id,
                counts["removed"],
                counts["not_found"],
                counts["not_member"],
            )
            return counts

    def delete_genome_group(self, metadata_db_uri: str, genome_group_name: str) -> dict[str, int]:
        if metadata_db_uri and metadata_db_uri != self.conn_uri:
            self.conn_uri = metadata_db_uri

        with self._db_connection().session_scope() as session:
            group = session.query(GenomeGroup).filter(GenomeGroup.name == genome_group_name).one_or_none()
            if group is None:
                logger.warning("Genome group '%s' does not exist", genome_group_name)
                return {"deleted": 0, "members_deleted": 0}

            member_count = session.query(GenomeGroupMember).filter(
                GenomeGroupMember.genome_group_id == group.genome_group_id
            ).delete(synchronize_session=False)
            session.delete(group)
            session.commit()
            logger.info(
                "Deleted genome group '%s' (id=%s) and %s associated member rows",
                genome_group_name,
                group.genome_group_id,
                member_count,
            )
            return {"deleted": 1, "members_deleted": member_count}


def main():
    parser = argparse.ArgumentParser(
        description="Manage genome groups from a CSV file and release membership."
    )
    parser.add_argument("--metadata_db_uri", required=True, help="Metadata DB URI")
    parser.add_argument("--genome_group_name", required=True, help="Genome group name")
    parser.add_argument(
        "--action",
        required=True,
        choices=["add", "remove", "delete"],
        help="Action to perform against the genome group",
    )
    parser.add_argument(
        "--csv_file",
        type=str,
        help="CSV file containing production_name and genome_uuid rows for add action",
    )
    parser.add_argument(
        "--genome_uuid",
        nargs="*",
        default=[],
        help="Genome UUIDs to remove from the group",
    )
    parser.add_argument(
        "--release_id",
        type=int,
        help="Release ID to attach/remove genome group members. Defaults to current release",
    )
    parser.add_argument(
        "--group_type",
        type=str,
        default="custom",
        help="Genome group type for add action",
    )
    parser.add_argument("--label", type=str, help="Group label for add action")
    parser.add_argument(
        "--searchable",
        action="store_true",
        help="Mark the genome group as searchable when creating it",
    )
    parser.add_argument("--description", type=str, help="Group description for add action")

    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    logger.info("Running genome group action '%s' for group '%s'", args.action, args.genome_group_name)

    factory = GenomeGroupFactory(args.metadata_db_uri)

    if args.action == "add":
        if not args.csv_file:
            parser.error("--csv_file is required for add action")
        result = factory.add_genomes_from_csv(
            metadata_db_uri=args.metadata_db_uri,
            genome_group_name=args.genome_group_name,
            csv_file=args.csv_file,
            release_id=args.release_id,
            group_type=args.group_type,
            label=args.label,
            searchable=args.searchable,
            description=args.description,
        )
    elif args.action == "remove":
        if not args.genome_uuid:
            parser.error("--genome_uuid is required for remove action")
        result = factory.remove_genomes_from_group(
            metadata_db_uri=args.metadata_db_uri,
            genome_group_name=args.genome_group_name,
            genome_uuids=args.genome_uuid,
            release_id=args.release_id,
        )
    else:
        result = factory.delete_genome_group(
            metadata_db_uri=args.metadata_db_uri,
            genome_group_name=args.genome_group_name,
        )

    logger.info("Genome group action '%s' completed: %s", args.action, result)


if __name__ == "__main__":
    main()
