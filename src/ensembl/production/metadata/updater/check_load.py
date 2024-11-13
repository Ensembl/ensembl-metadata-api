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
#   limitations under the License.`
import argparse
import json
import os

from ensembl.core.models import Meta, CoordSystem
from ensembl.ncbi_taxonomy.models import NCBITaxaName
from ensembl.utils.database import DBConnection
from sqlalchemy.exc import NoResultFound

from ensembl.production.metadata.api.api_utils import GetSourceFromGenomeUUID, GetGenomeUUIDFromSource
from ensembl.production.metadata.api.models import Genome, Assembly, Organism, DatasetAttribute, Attribute, Dataset, \
    GenomeDataset


class CheckLoading:
    def __init__(self, metadata_uri, db_uri, metadata_db_name="ensembl_genome_metadata", core_db_names=None,
                 genome_uuids=None, tests="all", output=False, patch_file=False):

        # Ensure only one of core_db_names, genome_uuids, or matched is provided
        if sum(bool(param) for param in [core_db_names, genome_uuids) != 1:
            raise ValueError("Please provide only one of 'core_db_names' or 'genome_uuids'.")

        # Parse core_db_names, genome_uuids, or matched based on the input type (file or string)
        if core_db_names:
            self.core_db_names = self._parse_input(core_db_names)
        elif genome_uuids:
            self.genome_uuids = self._parse_input(genome_uuids)

        # Initialize other attributes
        self.metadata_uri = metadata_uri + metadata_db_name
        self.db_uri = db_uri
        self.metadata_db = DBConnection(metadata_uri)
        self.tests = tests
        self.output = output
        self.patch_file = patch_file
        self.discrepancies = {
            "assembly": {},
            "organism": {},
            "genome": {},
            "assembly_dataset": {},
            "genebuild_dataset": {},
        }

    def _parse_input(self, input_value):
        """
        Parse the input to handle either a single string or a file path.
        If a file path is provided, read each line and return as a list.
        """
        if isinstance(input_value, str) and os.path.isfile(input_value):
            with open(input_value, 'r') as file:
                return [line.strip() for line in file]
        elif isinstance(input_value, str):
            return [input_value]
        else:
            raise ValueError("Input must be a string or a valid file path.")

    def _get_species_id_from_genome_uuid(self, genome_uuid, db_uri):
        with self.metadata_db.session_scope() as session:
            prod_name = session.query(Genome.production_name).filter(Genome.genome_uuid == genome_uuid).one_or_none()
            if prod_name is None:
                raise ValueError("Genome_uuid not found in metadata database.")
            prod_name = str(prod_name[0])
        db = DBConnection(db_uri)
        with db.session_scope() as session:
            species_id = session.query(Meta.species_id).filter(Meta.meta_key == "species.production_name").filter(
                Meta.meta_value == prod_name).one_or_none()
            if species_id is None:
                raise ValueError("Production name not found in core database")
            return str(species_id[0])

    def load_meta_dict(self, db_uri):
        """Load metadata into meta_dict from the database."""
        db = DBConnection(db_uri)
        with db.session_scope() as session:
            results = session.query(Meta).filter(Meta.meta_value.isnot(None),
                                                 Meta.meta_value.notin_(['', 'Null', 'NULL'])).all()
            meta_dict = {}
            for result in results:
                species_id = result.species_id
                meta_key = result.meta_key
                meta_value = result.meta_value
                if species_id not in meta_dict:
                    meta_dict[species_id] = {}
                meta_dict[species_id][meta_key] = meta_value
        return meta_dict

    def get_meta_single_meta_key(self, meta_dict, species_id, parameter):
        species_meta = meta_dict.get(species_id)
        if species_meta is None:
            return None
        return species_meta.get(parameter)

    def get_meta_list_from_prefix_meta_key(self, meta_dict, species_id, prefix):
        species_meta = meta_dict.get(species_id)
        if species_meta is None:
            return None
        result_dict = {k: v for k, v in species_meta.items() if k.startswith(prefix)}
        return result_dict

    def CheckAssemby(self, genome_uuid, core_db):
        db_uri = self.db_uri + core_db
        meta_dict = self.load_meta_dict(db_uri)
        species_id = self._get_species_id_from_genome_uuid(genome_uuid, db_uri)

        # Get all the corresponding details from the core for assembly.

        ###UPDATE HERE IF UPDATER IS UPDATED #################
        db = DBConnection(db_uri)
        with db.session_scope() as session:
            level = session.query(CoordSystem.name).filter(
                CoordSystem.species_id == species_id).order_by(CoordSystem.rank).all()[0][0]

        core_assembly = {
            "accession": self.get_meta_single_meta_key(meta_dict, species_id, "assembly.accession"),
            "name": self.get_meta_single_meta_key(meta_dict, species_id, "assembly.name"),
            "ucsc_name": self.get_meta_single_meta_key(meta_dict, species_id, "assembly.ucsc_alias"),
            "level": level,
            "is_reference": int(self.get_meta_single_meta_key(meta_dict, species_id, "assembly.is_reference") or 0)
            "tol_id": self.get_meta_single_meta_key(meta_dict, species_id, "assembly.tol_id"),
            "accession_body": self.get_meta_single_meta_key(meta_dict, species_id, "assembly.provider"),
            "assembly_default": self.get_meta_single_meta_key(meta_dict, species_id, "assembly.default"),
            "alt_accession": self.get_meta_single_meta_key(meta_dict, species_id, "assembly.default"),
            "url_name": self.get_meta_single_meta_key(meta_dict, species_id, "assembly.url_name"),
        }
        #################################################
        with self.metadata_db.session_scope() as session:
            try:
                assembly = (
                    session.query(Assembly)
                    .join(Genome)
                    .filter(Genome.genome_uuid == genome_uuid)
                    .one()
                )
            except NoResultFound:
                raise ValueError(f"No assembly found for genome UUID {genome_uuid} in metadata database.")

        discrepancies = {}
        for field, core_value in core_assembly.items():
            metadata_value = getattr(assembly, field)
            if core_value != metadata_value:
                discrepancies[field] = {"core": core_value, "metadata": metadata_value}
        if discrepancies:
            self.discrepancies["assembly"][genome_uuid] = discrepancies
        else:
            self.discrepancies["assembly"][genome_uuid] = "Match"
        return self.discrepancies["assembly"][genome_uuid]

    def CheckGenome(self, genome_uuid, core_db):
        db_uri = self.db_uri + core_db
        meta_dict = self.load_meta_dict(db_uri)
        species_id = self._get_species_id_from_genome_uuid(genome_uuid, db_uri)

        # Get all the corresponding details from the core for assembly.

        ###UPDATE HERE IF UPDATER IS UPDATED #################

        core_genome = {
            "production_name": self.get_meta_single_meta_key(meta_dict, species_id, "organism.production_name"),
            "genebuild_version": self.get_meta_single_meta_key(meta_dict, species_id, "genebuild.version"),
            "genebuild_date": self.get_meta_single_meta_key(meta_dict, species_id, "genebuild.last_geneset_update"),
        }
        #################################################
        with self.metadata_db.session_scope() as session:
            try:
                genome = (
                    session.query(Genome)
                    .filter(Genome.genome_uuid == genome_uuid)
                    .one()
                )
            except NoResultFound:
                raise ValueError(f"No genome found for genome UUID {genome_uuid} in metadata database.")

        discrepancies = {}
        for field, core_value in core_genome.items():
            metadata_value = getattr(genome, field)
            if core_value != metadata_value:
                discrepancies[field] = {"core": core_value, "metadata": metadata_value}
        if discrepancies:
            self.discrepancies["genome"][genome_uuid] = discrepancies
        else:
            self.discrepancies["genome"][genome_uuid] = "Match"
        return self.discrepancies["genome"][genome_uuid]

    def CheckOrganism(self, genome_uuid, core_db):
        db_uri = self.db_uri + core_db
        meta_dict = self.load_meta_dict(db_uri)
        species_id = self._get_species_id_from_genome_uuid(genome_uuid, db_uri)

        ###UPDATE HERE IF UPDATER IS UPDATED #################
        # Getting the common name from the meta table, otherwise we grab it from ncbi.
        common_name = self.get_meta_single_meta_key(species_id, "organism.common_name")
        taxid = self.get_meta_single_meta_key(species_id, "organism.taxonomy_id")
        if common_name is None:
            with self.metadata_db.session_scope() as session:
                common_name = session.query(NCBITaxaName).filter(
                    NCBITaxaName.taxon_id == taxid,
                    NCBITaxaName.name_class == "genbank common name"
                ).one_or_none()
                common_name = common_name.name if common_name is not None else '-'
        # Ensure that the first character is upper case.
        common_name = common_name[0].upper() + common_name[1:]
        species_taxonomy_id = self.get_meta_single_meta_key(species_id, "organism.species_taxonomy_id")
        if species_taxonomy_id is None:
            species_taxonomy_id = taxid
        core_organism = {
            "biosample_id": self.get_meta_single_meta_key(meta_dict, species_id, "organism.biosample_id"),
            "species_taxonomy_id": species_taxonomy_id,
            "taxonomy_id": self.get_meta_single_meta_key(meta_dict, species_id, "organism.taxonomy_id"),
            "common_name": common_name,
            "scientific_name": self.get_meta_single_meta_key(meta_dict, species_id, "organism.scientific_name"),
            "strain": self.get_meta_single_meta_key(meta_dict, species_id, "organism.strain"),
            "strain_type": self.get_meta_single_meta_key(meta_dict, species_id, "organism.type"),
            "scientific_parlance_name": self.get_meta_single_meta_key(meta_dict, species_id,
                                                                      "organism.scientific_parlance_name"),

        }
        #################################################
        with self.metadata_db.session_scope() as session:
            try:
                organism = (
                    session.query(Organism)
                    .join(Genome)
                    .filter(Genome.genome_uuid == genome_uuid)
                    .one()
                )
            except NoResultFound:
                raise ValueError(f"No organism found for genome UUID {genome_uuid} in metadata database.")

        discrepancies = {}
        for field, core_value in core_organism.items():
            metadata_value = getattr(organism, field)
            if core_value != metadata_value:
                discrepancies[field] = {"core": core_value, "metadata": metadata_value}
        if discrepancies:
            self.discrepancies["organism"][genome_uuid] = discrepancies
        else:
            self.discrepancies["organism"][genome_uuid] = "Match"
        return self.discrepancies["organism"][genome_uuid]

    def CheckDatasetAttributes(self, genome_uuid, core_db, dataset_type, ignore_keys=None):
        if ignore_keys is None:
            ignore_keys = []

        if dataset_type == "assembly":
            prefix = "assembly."
            dataset_name = "assembly"
        elif dataset_type == "genebuild":
            prefix = "genebuild."
            dataset_name = "genebuild"
        else:
            raise ValueError("Invalid dataset_type. Expected 'assembly' or 'genebuild'.")

        db_uri = self.db_uri + core_db
        meta_dict = self.load_meta_dict(db_uri)
        species_id = self._get_species_id_from_genome_uuid(genome_uuid, db_uri)

        core_attributes = self.get_meta_list_from_prefix_meta_key(meta_dict, species_id, prefix)

        with self.metadata_db.session_scope() as session:
            try:
                dataset_attributes = (
                    session.query(DatasetAttribute.value, Attribute.name)
                    .join(Dataset, DatasetAttribute.dataset_id == Dataset.dataset_id)
                    .join(GenomeDataset, Dataset.dataset_id == GenomeDataset.dataset_id)
                    .join(Genome, Genome.genome_id == GenomeDataset.genome_id)
                    .join(Attribute, DatasetAttribute.attribute_id == Attribute.attribute_id)
                    .filter(Genome.genome_uuid == genome_uuid, Dataset.name == dataset_name)
                    .all()
                )
            except NoResultFound:
                raise ValueError(f"No {dataset_name} dataset found for genome UUID {genome_uuid} in metadata database.")

        metadata_attributes = {name: value for value, name in dataset_attributes}

        discrepancies = {}
        for key, core_value in core_attributes.items():
            if key in ignore_keys:
                continue
            metadata_value = metadata_attributes.get(key)
            if core_value != metadata_value:
                discrepancies[key] = {"core": core_value, "metadata": metadata_value}

        # Check for extra keys in metadata not present in core, except those in ignore_keys
        for key in metadata_attributes:
            if key not in core_attributes and key not in ignore_keys:
                discrepancies[key] = {"core": None, "metadata": metadata_attributes[key]}

        # Log discrepancies under the appropriate dataset type
        result_key = f"{dataset_type}_dataset"
        if discrepancies:
            self.discrepancies[result_key][genome_uuid] = discrepancies
        else:
            self.discrepancies[result_key][genome_uuid] = "Match"

        return self.discrepancies[result_key][genome_uuid]

    def log_results(self):
        if self.output:
            with open(self.output, "w") as file:
                json.dump(self.discrepancies, file, indent=2)


def main():
    # Set up argument parsing for command-line inputs
    parser = argparse.ArgumentParser(description="Check data loading between MySQL core and metadata databases.")
    parser.add_argument("--metadata_uri", required=True, help="URI for the metadata database connection.")
    parser.add_argument("--db_uri", required=True, help="URI prefix for the core database connections.")
    parser.add_argument("--metadata_db_name", default="ensembl_genome_metadata", help="Name of the metadata database.")
    parser.add_argument("--core_db_names", help="Comma-separated list or file of core database names.")
    parser.add_argument("--genome_uuids", help="Comma-separated list or file of genome UUIDs.")
    parser.add_argument("--tests", default="all",
                        help="Specify the tests to run (all, assembly, genome, organism, assembly_dataset, or genebuild_dataset).")
    parser.add_argument("--output", default=False, help="File to store results, otherwise stdout.")
    parser.add_argument("--patch_file", action="store_true", help="Generate a patch file with discrepancies.")

    args = parser.parse_args()

    checker = CheckLoading(
        metadata_uri=args.metadata_uri,
        db_uri=args.db_uri,
        metadata_db_name=args.metadata_db_name,
        core_db_names=args.core_db_names,
        genome_uuids=args.genome_uuids,
        tests=args.tests,
        output=args.output,
        patch_file=args.patch_file
    )

    # If starting with cores, swap them to genome_uuids.
    if checker.core_db_names:
        for db_name in checker.core_db_names:
            UUID = GetGenomeUUIDFromSource(checker.metadata_uri, db_name)
            checker.genome_uuids.append(UUID)

    # dataset keys to ignore
    ignore = []
    # Perform checks based on the provided input
    for genome_uuid in checker.genome_uuids:
        core_db_name = GetSourceFromGenomeUUID(checker.metadata_uri, genome_uuid)
        if checker.tests == "all" or checker.tests == "assembly":
            checker.CheckAssembly(genome_uuid, core_db_name)
        elif checker.tests == "all" or checker.tests == "assembly_dataset":
            checker.CheckDatasetAttributes(genome_uuid, core_db_name, "assembly", ignore)
        elif checker.tests == "all" or checker.tests == "genebuild_dataset":
            checker.CheckDatasetAttributes(genome_uuid, core_db_name, "genebuild", ignore)
        elif checker.tests == "all" or checker.tests == "genome":
            checker.CheckGenome(genome_uuid, core_db_name)
        elif checker.tests == "all" or checker.tests == "organism":
            checker.CheckOrganism(genome_uuid, core_db_name)
    # Output discrepancies if enabled
    if checker.output:
        checker.log_results()


if __name__ == "__main__":
    main()
