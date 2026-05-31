#!/usr/bin/env python
#  See the NOTICE file distributed with this work for additional information
#  regarding copyright ownership.
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#      http://www.apache.org/licenses/LICENSE-2.0
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

"""
Fetch Genome Info From New Metadata Database
"""

import csv
import json
import logging
import re
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Iterator, List

from ensembl.utils.database import DBConnection
from ensembl.utils.argparse import ArgumentParser

from sqlalchemy import select, exists

from ensembl.production.metadata.api.factories.datasets import DatasetFactory
from ensembl.production.metadata.api.models.dataset import DatasetType, Dataset, DatasetSource, DatasetStatus
from ensembl.production.metadata.api.models.genome import Genome, GenomeDataset, GenomeRelease
from ensembl.production.metadata.api.models.organism import Organism, OrganismGroup, OrganismGroupMember
from ensembl.production.metadata.api.models.release import EnsemblRelease
from sqlalchemy.orm import aliased
from sqlalchemy.sql.operators import and_

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class OutputFormat(str, Enum):
    JSON = "json"
    TSV = "tsv"
    PARQUET = "parquet"

    @classmethod
    def from_string(cls, value: str) -> "OutputFormat":
        if not value:
            return cls.JSON
        normalized = value.strip().lower()
        return cls(normalized)


class GenomeOutputWriter:
    @staticmethod
    def write(records: Iterator[dict], output_file: str, output_format: OutputFormat, columns: List[str]) -> None:
        output_path = Path(output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        if output_format == OutputFormat.JSON:
            GenomeOutputWriter._write_json(records, output_path)
        elif output_format == OutputFormat.TSV:
            GenomeOutputWriter._write_tsv(records, output_path, columns)
        elif output_format == OutputFormat.PARQUET:
            GenomeOutputWriter._write_parquet(records, output_path, columns)
        else:
            raise ValueError(f"Unsupported output format: {output_format}")

    @staticmethod
    def _write_json(records: Iterator[dict], output_path: Path) -> None:
        with output_path.open("w", encoding="utf-8") as handle:
            handle.write("[")
            first = True
            for record in records:
                if not first:
                    handle.write(",\n")
                json.dump(record, handle, default=str)
                first = False
            handle.write("]\n")

    @staticmethod
    def _write_tsv(records: Iterator[dict], output_path: Path, columns: List[str]) -> None:
        with output_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=columns, delimiter="\t", extrasaction="ignore")
            writer.writeheader()
            for record in records:
                writer.writerow({key: record.get(key, "") for key in columns})

    @staticmethod
    def _write_parquet(records: Iterator[dict], output_path: Path, columns: List[str]) -> None:
        try:
            import pandas as pd
        except ImportError as exc:
            raise RuntimeError("Parquet export requires pandas and pyarrow or fastparquet") from exc

        rows = [record for record in records]
        frame = pd.DataFrame.from_records(rows, columns=columns)
        try:
            frame.to_parquet(output_path, index=False)
        except Exception as exc:
            raise RuntimeError(
                "Parquet export failed. Install pyarrow or fastparquet and ensure the output path is writable."
            ) from exc


@dataclass
class GenomeInputFilters:
    metadata_db_uri: str
    genome_uuid: List[str] = field(default_factory=list)
    dataset_uuid: List[str] = field(default_factory=list)
    division: List[str] = field(default_factory=list)
    dataset_type: str = ""
    dataset_names: str = ""
    dataset_is_current: int = 0
    species: List[str] = field(default_factory=list)
    antispecies: List[str] = field(default_factory=list)
    dataset_status: List[str] = field(default_factory=lambda: [])
    dataset_release_id: List[int] = field(default_factory=lambda: [])
    release_id: List[int] = field(default_factory=lambda: [])
    release_name: List[int] = field(default_factory=lambda: [])
    release_type: List[str] = field(default_factory=lambda: [])
    batch_size: int = 50
    page: int = 1
    organism_group_type: str = ""
    run_all: int = 0
    update_dataset_status: str = ""
    update_dataset_attribute: dict = field(default_factory=lambda: {})
    column_names: List[str] = field(default_factory=list)

    @staticmethod
    def default_columns() -> List:
        return [
            Genome.genome_uuid.label("genome_uuid"),
            Genome.production_name.label("species"),
            Dataset.dataset_uuid.label("dataset_uuid"),
            Dataset.status.label("dataset_status"),
            DatasetSource.name.label("dataset_source"),
            DatasetSource.location.label("dataset_source_location"),
            Dataset.name.label("dataset_name"),
            DatasetType.name.label("dataset_type"),
            EnsemblRelease.name.label("genome_release"),
            GenomeDataset.release_id.label("dataset_release"),
        ]

    @classmethod
    def available_columns(cls) -> dict[str, object]:
        return {
            "genome_uuid": Genome.genome_uuid.label("genome_uuid"),
            "species": Genome.production_name.label("species"),
            "dataset_uuid": Dataset.dataset_uuid.label("dataset_uuid"),
            "dataset_status": Dataset.status.label("dataset_status"),
            "dataset_source": DatasetSource.name.label("dataset_source"),
            "dataset_source_location": DatasetSource.location.label("dataset_source_location"),
            "dataset_name": Dataset.name.label("dataset_name"),
            "dataset_type": DatasetType.name.label("dataset_type"),
            "genome_release": EnsemblRelease.name.label("genome_release"),
            "dataset_release": GenomeDataset.release_id.label("dataset_release"),
        }

    @classmethod
    def resolve_columns(cls, column_names: List[str]) -> List:
        if not column_names:
            return cls.default_columns()

        available = cls.available_columns()
        resolved = []
        invalid = []
        for name in column_names:
            if name in available:
                resolved.append(available[name])
            else:
                invalid.append(name)

        if invalid:
            logger.warning(
                f"Invalid genome column names provided: {invalid}. Falling back to default columns for invalid entries."
            )

        return resolved if resolved else cls.default_columns()

    def __post_init__(self) -> None:
        if self.column_names:
            self.columns = self.resolve_columns(self.column_names)

    columns: List = field(default_factory=default_columns)


class GenomeQueryBuilder:
    def __init__(self, filters: GenomeInputFilters):
        self.filters = filters

    def build(self):
        query = (
            select(*self.filters.columns)
            .select_from(Genome)
            .join(Genome.assembly)
            .join(Genome.organism)
            .join(Genome.genome_datasets)
            .join(GenomeDataset.dataset)
            .join(Dataset.dataset_type)
            .join(Dataset.dataset_source)
            .join(Genome.genome_releases)
            .join(GenomeRelease.ensembl_release)
        )

        return self._apply_filters(query)

    def _apply_filters(self, query):
        genomes_release = aliased(EnsemblRelease)
        genomes_dataset_release = aliased(EnsemblRelease)
        ensembl_release_type_filter = 'integrated'

        if self.filters.run_all:
            self.filters.division = [
                'EnsemblBacteria',
                'EnsemblVertebrates',
                'EnsemblPlants',
                'EnsemblProtists',
                'EnsemblMetazoa',
                'EnsemblFungi',
            ]

        if self.filters.division or self.filters.organism_group_type or any(
                [i.element.table.name in ['organism_group', 'organism_group_member'] for i in self.filters.columns]):
            query = query.outerjoin(Organism.organism_group_members).outerjoin(OrganismGroupMember.organism_group)
            ensembl_divisions = self.filters.division

            if self.filters.division and not self.filters.organism_group_type:
                self.filters.organism_group_type = 'Division'

            if self.filters.organism_group_type == 'Division':
                pattern = re.compile(r'^(ensembl)?', re.IGNORECASE)
                ensembl_divisions = ['Ensembl' + pattern.sub('', d).capitalize() for d in ensembl_divisions if d]

            if self.filters.organism_group_type:
                query = query.filter(OrganismGroup.type == self.filters.organism_group_type)

            if ensembl_divisions:
                query = query.filter(OrganismGroup.name.in_(ensembl_divisions))

        if self.filters.genome_uuid:
            query = query.filter(Genome.genome_uuid.in_(self.filters.genome_uuid))

        if self.filters.dataset_uuid:
            query = query.filter(Dataset.dataset_uuid.in_(self.filters.dataset_uuid))

        if self.filters.species:
            species = set(self.filters.species) - set(self.filters.antispecies)

            if species:
                query = query.filter(Genome.production_name.in_(self.filters.species))
            else:
                query = query.filter(~Genome.production_name.in_(self.filters.antispecies))

        elif self.filters.antispecies:
            query = query.filter(~Genome.production_name.in_(self.filters.antispecies))

        if self.filters.release_type == 'integrated':
            ensembl_release_type_filter = 'partial'

        query = query.filter(
            ~exists().where(
                and_(
                    genomes_release.release_id == GenomeRelease.release_id,
                    genomes_release.release_type == ensembl_release_type_filter
                )
            )
        )
        query = query.filter(
            ~exists().where(
                and_(
                    genomes_dataset_release.release_id == GenomeDataset.release_id,
                    genomes_dataset_release.release_type == ensembl_release_type_filter
                )
            )
        )

        if self.filters.release_type:
            query = query.filter(EnsemblRelease.release_type == self.filters.release_type)

        if self.filters.dataset_release_id:
            query = query.filter(GenomeDataset.release_id.in_(self.filters.dataset_release_id))

        if self.filters.release_id:
            query = query.filter(GenomeRelease.release_id.in_(self.filters.release_id))

        if self.filters.release_name:
            filter_release_type = EnsemblRelease.name.in_(self.filters.release_name)
            if self.filters.release_type == 'integrated':
                filter_release_type = GenomeDataset.release_id.in_(self.filters.release_name)

            query = query.filter(filter_release_type)

        if self.filters.dataset_type:
            query = query.filter(DatasetType.name == self.filters.dataset_type)

        if self.filters.dataset_names:
            query = query.filter(Dataset.name.in_(self.filters.dataset_names))

        if self.filters.dataset_is_current:
            query = query.filter(GenomeDataset.is_current == self.filters.dataset_is_current)

        if self.filters.dataset_status:
            status_enums = GenomeFactory._normalize_status_to_enum(self.filters.dataset_status)
            if status_enums:
                query = query.filter(Dataset.status.in_(status_enums))
            else:
                logger.warning(f"No valid status values to filter on: {self.filters.dataset_status}")

        if self.filters.batch_size:
            self.filters.page = self.filters.page if self.filters.page > 0 else 1
            query = query.offset((self.filters.page - 1) * self.filters.batch_size).limit(self.filters.batch_size)

        logger.debug(f"Filter Query {query}")
        return query


@dataclass
class GenomeFactory:

    @staticmethod
    def _normalize_status_to_enum(status_list):
        """
        Convert a list of status strings to DatasetStatus enum values.
        This ensures compatibility between SQLite and MySQL.

        Args:
            status_list: List of status strings or enums

        Returns:
            List of DatasetStatus enum values
        """
        if not status_list:
            return []

        normalized = []
        for status in status_list:
            if isinstance(status, DatasetStatus):
                normalized.append(status)
            elif isinstance(status, str):
                try:
                    normalized.append(DatasetStatus(status))
                except ValueError:
                    logger.warning(f"Invalid status value: {status}")
            else:
                logger.warning(f"Unexpected status type: {type(status)} for value {status}")

        return normalized

    def _build_query(self, filters: GenomeInputFilters):
        return GenomeQueryBuilder(filters).build()

    def get_genomes(self, **filters: GenomeInputFilters):

        filters = GenomeInputFilters(**filters)
        logger.info(f"Get Genomes with filters {filters}")

        with DBConnection(filters.metadata_db_uri).session_scope() as session:
            query = self._build_query(filters)
            logger.info(f"Executing SQL query: {query}")

            result = session.execute(query)
            row_count = 0
            for row in result.mappings():
                row_count += 1
                genome_info = dict(row)
                dataset_uuid = genome_info.get("dataset_uuid")

                dataset_status = genome_info.get("dataset_status")
                if dataset_status and isinstance(dataset_status, DatasetStatus):
                    genome_info["dataset_status"] = dataset_status.value

                if not dataset_uuid:
                    logger.warning(
                        f"No dataset uuid found for genome {genome_info.get('genome_uuid')} skipping this genome"
                    )
                    continue

                if filters.update_dataset_status:
                    update_status_enum = filters.update_dataset_status
                    if isinstance(update_status_enum, str):
                        try:
                            update_status_enum = DatasetStatus(update_status_enum)
                        except ValueError:
                            logger.error(f"Invalid update_dataset_status: {filters.update_dataset_status}")
                            genome_info["updated_dataset_status"] = None
                            yield genome_info
                            continue

                    _, status = DatasetFactory(filters.metadata_db_uri).update_dataset_status(
                        dataset_uuid, update_status_enum.value, session=session
                    )

                    if update_status_enum == status:
                        logger.info(
                            f"Updated Dataset status for dataset uuid: {dataset_uuid} from "
                            f"{genome_info.get('dataset_status')} to {status.value} "
                            f"for genome {genome_info['genome_uuid']}"
                        )
                        genome_info["updated_dataset_status"] = status.value
                    else:
                        logger.warning(
                            f"Cannot update status for dataset uuid: {dataset_uuid} "
                            f"from {genome_info.get('dataset_status')} to {status.value} "
                            f"for genome {genome_info['genome_uuid']}"
                        )
                        genome_info["updated_dataset_status"] = None
                    session.flush()

                yield genome_info

            logger.debug(f"Query returned {row_count} results")


def main():
    parser = ArgumentParser(
        prog="genomes.py", description="Fetch Ensembl genome information from the new metadata database"
    )
    parser.add_argument(
        "--genome_uuid",
        type=str,
        nargs="*",
        default=[],
        required=False,
        help="List of genome UUIDs to filter the query",
    )
    parser.add_argument(
        "--dataset_uuid",
        type=str,
        nargs="*",
        default=[],
        required=False,
        help="List of dataset UUIDs to filter the query",
    )

    parser.add_argument(
        "--dataset_is_current",
        action="store_true",
        help="Filter datasets that are marked as current",
    )

    parser.add_argument(
        "--organism_group_type",
        type=str,
        default="",
        required=False,
        help='Organism group type to filter the query. eg. Division Popular etc.',
    )
    parser.add_argument(
        "--division",
        type=str,
        nargs="*",
        default=[],
        required=False,
        help="List of organism group names to filter the query. eg. EnsemblVertebrates, EnsemblPlants etc.",
    )
    parser.add_argument(
        "--dataset_type",
        type=str,
        default="",
        required=False,
        help="List of dataset types to filter the query. eg. assembly, genebuild, variation etc.",
    )
    parser.add_argument(
        "--dataset_names",
        nargs="*",
        type=str,
        default=["genebuild"],
        required=False,
        help="List of dataset types to filter the query. eg. assembly, genebuild, variation etc.",
    )
    parser.add_argument(
        "--species",
        type=str,
        nargs="*",
        default=[],
        required=False,
        help="List of Species Production names to filter the query. eg. homo_sapiens, mus_musculus etc.",
    )
    parser.add_argument(
        "--antispecies",
        type=str,
        nargs="*",
        default=[],
        required=False,
        help="List of Species Production names to exclude from the query. eg. homo_sapiens, mus_musculus etc.",
    )
    parser.add_numeric_argument(
        "--release_id",
        type=int,
        nargs="*",
        default=[],
        required=False,
        help="Genome_dataset release_id to filter genomes and datasets.",
    )
    parser.add_numeric_argument(
        "--release_name",
        type=int,
        nargs="*",
        default=[],
        required=False,
        help="Fetch Genomes for a given release. "
             "(for integrated release it filters similar to the dataset_release_id)",
    )
    parser.add_argument(
        "--release_type",
        type=str,
        default="partial",
        choices=["partial", 'integrated'],
        required=False,
        help="""
        Fetch genome datasets and apply release-type filtering to eliminate duplicates introduced during 
        integration release.
        """,
    )
    parser.add_numeric_argument(
        "--dataset_release_id",
        type=int,
        nargs="*",
        default=[],
        required=False,
        help="Datasets are attached to different release ids, filter the query based on dataset release id.",
    )

    parser.add_argument(
        "--dataset_status",
        nargs="*",
        default=[],
        choices=["Submitted", "Processing", "Processed", "Released"],
        required=False,
        help="List of dataset statuses to filter the query.",
    )
    parser.add_argument(
        "--update_dataset_status",
        type=str,
        default="",
        required=False,
        choices=["Submitted", "Processing", "Processed", "Released", ""],
        help="Update the status of the selected datasets to the specified value. ",
    )
    parser.add_numeric_argument(
        "--batch_size",
        type=int,
        default=0,
        min_value=0,
        required=False,
        help="Number of results to retrieve per batch. Default is 0 (no limit).",
    )
    parser.add_numeric_argument(
        "--page",
        default=0,
        required=False,
        min_value=0,
        type=int,
        help="The page number for pagination",
    )
    parser.add_argument(
        "--metadata_db_uri",
        type=str,
        required=True,
        help="metadata db mysql uri, ex: mysql://ensro@localhost:3366/ensembl_genome_metadata",
    )
    parser.add_argument("--output", type=str, required=True, help="output file ex: genome_info.json")
    parser.add_argument(
        "--columns",
        nargs="*",
        type=str,
        default=[],
        required=False,
        help=(
            "Select columns to return in the query result. "
            "If omitted, default columns are used. "
            "Supported values: genome_uuid, species, dataset_uuid, dataset_status, "
            "dataset_source, dataset_name, dataset_type, genome_release, dataset_release."
        ),
    )
    parser.add_argument(
        "--output_format",
        type=str,
        default=OutputFormat.JSON.value,
        choices=[format.value for format in OutputFormat],
        required=False,
        help="Output serialization format for the genome result file.",
    )

    args = parser.parse_args()
    output_format = OutputFormat.from_string(args.output_format)

    meta_details = re.match(r"mysql:\/\/.*:?(.*?)@(.*?):\d+\/(.*)", args.metadata_db_uri)
    logger.info(
        f"Connecting Metadata Database with host:{meta_details.group(2)} & dbname:{meta_details.group(3)}"
    )

    genome_fetcher = GenomeFactory()
    filters = GenomeInputFilters(
        metadata_db_uri=args.metadata_db_uri,
        genome_uuid=args.genome_uuid,
        dataset_uuid=args.dataset_uuid,
        division=args.division,
        dataset_type=args.dataset_type,
        dataset_names=args.dataset_names,
        dataset_is_current=args.dataset_is_current,
        species=args.species,
        antispecies=args.antispecies,
        dataset_status=args.dataset_status,
        batch_size=args.batch_size,
        page=args.page,
        release_id=args.release_id,
        release_name=args.release_name,
        release_type=args.release_type,
        organism_group_type=args.organism_group_type,
        update_dataset_status=args.update_dataset_status,
        column_names=args.columns,
    )
    genome_records = genome_fetcher.get_genomes(**vars(filters))

    output_columns = [
        getattr(column, "key", getattr(column, "name", None)) for column in filters.columns
    ]
    logger.info(f"Writing results to {args.output} in {output_format.value} format")
    GenomeOutputWriter.write(genome_records, args.output, output_format, output_columns)
    logger.info("Completed !")


if __name__ == "__main__":
    logger.info("Fetching Genome Information From New Metadata Database")
    main()
