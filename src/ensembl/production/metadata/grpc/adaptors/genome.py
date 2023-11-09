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

import sqlalchemy as db
from sqlalchemy.orm import aliased
from ensembl.database import DBConnection
from ensembl.ncbi_taxonomy.models import NCBITaxaName
from ensembl.production.metadata.grpc.adaptors.base import BaseAdaptor, check_parameter
from ensembl.production.metadata.api.models import Genome, Organism, Assembly, OrganismGroup, OrganismGroupMember, \
	GenomeRelease, EnsemblRelease, EnsemblSite, AssemblySequence, GenomeDataset, Dataset, DatasetType, DatasetSource, \
	Attribute, DatasetAttribute
import logging

logger = logging.getLogger(__name__)


class GenomeAdaptor(BaseAdaptor):
	def __init__(self, metadata_uri, taxonomy_uri=None):
		super().__init__(metadata_uri)
		self.taxonomy_db = DBConnection(taxonomy_uri)

	def fetch_taxonomy_names(self, taxonomy_ids, synonyms=[]):

		taxonomy_ids = check_parameter(taxonomy_ids)
		synonyms = [
			"common name",
			"equivalent name",
			"genbank synonym",
			"synonym",
		] if len(check_parameter(synonyms)) == 0 else synonyms
		required_class_name = ["genbank common name", "scientific name"]
		taxons = {}
		with self.taxonomy_db.session_scope() as session:
			for tid in taxonomy_ids:
				taxons[tid] = {"scientific_name": None, "genbank_common_name": None, "synonym": []}

				taxonomyname_query = db.select(
					NCBITaxaName.name,
					NCBITaxaName.name_class,
				).filter(
					NCBITaxaName.taxon_id == tid,
					NCBITaxaName.name_class.in_(required_class_name + synonyms),
				)

				for taxon_name in session.execute(taxonomyname_query).all():
					if taxon_name[1] in synonyms:
						taxons[tid]['synonym'].append(taxon_name[0])
					if taxon_name[1] in required_class_name:
						taxon_format_name = "_".join(taxon_name[1].split(' '))
						taxons[tid][taxon_format_name] = taxon_name[0]
			return taxons

	def fetch_taxonomy_ids(self, taxonomy_names):
		taxids = []
		taxonomy_names = check_parameter(taxonomy_names)
		for taxon in taxonomy_names:
			taxa_name_select = db.select(
				NCBITaxaName.taxon_id
			).filter(
				NCBITaxaName.name == taxon
			)
			with self.taxonomy_db.session_scope() as session:
				logger.debug(taxa_name_select)
				taxid = session.execute(taxa_name_select).one()
				taxids.append(taxid[0])
		return taxids

	def fetch_genomes(self, genome_id=None, genome_uuid=None, genome_tag=None, organism_uuid=None, assembly_uuid=None,
					  assembly_accession=None, assembly_name=None, use_default_assembly=False, ensembl_name=None,
					  taxonomy_id=None, group=None, group_type=None, allow_unreleased=False, unreleased_only=False,
					  site_name=None, release_type=None, release_version=None, current_only=True):
		"""
		Fetches genome information based on the specified parameters.

		Args:
			genome_id (Union[int, List[int]]): The ID(s) of the genome(s) to fetch.
			genome_uuid (Union[str, List[str]]): The UUID(s) of the genome(s) to fetch.
			genome_tag (Union[str, List[str]]): genome_tag value is either in Assembly.url_name or told_id.
			organism_uuid (Union[str, List[str]]): The UUID(s) of the organism(s) to fetch.
			assembly_uuid (Union[str, List[str]]): The UUID(s) of the assembly(s) to fetch.
			assembly_accession (Union[str, List[str]]): The assenbly accession of the assembly(s) to fetch.
			assembly_name (Union[str, List[str]]): The name(s) of the assembly(s) to fetch.
			use_default_assembly (bool): Whether to use default assembly name or not.
			ensembl_name (Union[str, List[str]]): The Ensembl name(s) of the organism(s) to fetch.
			taxonomy_id (Union[int, List[int]]): The taxonomy ID(s) of the organism(s) to fetch.
			group (Union[str, List[str]]): The name(s) of the organism group(s) to filter by.
			group_type (Union[str, List[str]]): The type(s) of the organism group(s) to filter by.
			allow_unreleased (bool): Whether to fetch unreleased genomes too or not (default: False).
			unreleased_only (bool): Fetch only unreleased genomes (default: False). allow_unreleased is used by gRPC
									 to fetch both released and unreleased genomes, while unreleased_only
									 is used in production pipelines (fetches only unreleased genomes)
			site_name (str): The name of the Ensembl site to filter by.
			release_type (str): The type of the Ensembl release to filter by.
			release_version (int): The maximum version of the Ensembl release to filter by.
			current_only (bool): Whether to fetch only current genomes.

		Returns:
			List[Tuple[Genome, Organism, Assembly, EnsemblRelease]]: A list of tuples containing the fetched genome information.
			Each tuple contains the following elements:
				- Genome: An instance of the Genome class.
				- Organism: An instance of the Organism class.
				- Assembly: An instance of the Assembly class.
				- EnsemblRelease: An instance of the EnsemblRelease class.

		Notes:
			- The parameters are not mutually exclusive, meaning more than one of them can be provided at a time.
			- The function uses a database session to execute the query and returns the results as a list of tuples.
			- The results are ordered by the Ensembl name.

		Example usage:
			genome_info = fetch_genomes(genome_id=12345)
		"""
		# Parameter validation
		genome_id = check_parameter(genome_id)
		genome_uuid = check_parameter(genome_uuid)
		genome_tag = check_parameter(genome_tag)
		organism_uuid = check_parameter(organism_uuid)
		assembly_uuid = check_parameter(assembly_uuid)
		assembly_accession = check_parameter(assembly_accession)
		assembly_name = check_parameter(assembly_name)
		ensembl_name = check_parameter(ensembl_name)
		taxonomy_id = check_parameter(taxonomy_id)
		group = check_parameter(group)
		group_type = check_parameter(group_type)

		# Construct the initial database query
		genome_select = db.select(
			Genome, Organism, Assembly
		).select_from(Genome) \
			.join(Organism, Organism.organism_id == Genome.organism_id) \
			.join(Assembly, Assembly.assembly_id == Genome.assembly_id)

		# Apply group filtering if group parameter is provided
		if group:
			group_type = group_type if group_type else ['Division']
			genome_select = db.select(
				Genome, Organism, Assembly, OrganismGroup, OrganismGroupMember
			).join(Genome.assembly).join(Genome.organism) \
				.join(Organism.organism_group_members) \
				.join(OrganismGroupMember.organism_group) \
				.filter(OrganismGroup.type.in_(group_type)).filter(OrganismGroup.name.in_(group))

		# Apply additional filters based on the provided parameters
		if genome_id is not None:
			genome_select = genome_select.filter(Genome.genome_id.in_(genome_id))

		if genome_uuid is not None:
			genome_select = genome_select.filter(Genome.genome_uuid.in_(genome_uuid))

		if genome_tag is not None:
			genome_select = genome_select.filter(
				db.or_(
					Assembly.url_name.in_(genome_tag),
					Assembly.tol_id.in_(genome_tag)
				)
			)

		if organism_uuid is not None:
			genome_select = genome_select.filter(Organism.organism_uuid.in_(organism_uuid))

		if assembly_uuid is not None:
			genome_select = genome_select.filter(Assembly.assembly_uuid.in_(assembly_uuid))

		if assembly_accession is not None:
			genome_select = genome_select.filter(Assembly.accession.in_(assembly_accession))

		if assembly_name is not None:
			# case() function is used to conditionally select between columns, sql equivalent is:
			# CASE
			#     WHEN :use_default_assembly = 1 THEN assembly.assembly_default
			#     ELSE assembly.name
			# END
			conditional_column = db.case(
				# literal is used to prevent evaluating use_default_assembly to a boolean (True or False)
				[(db.literal(use_default_assembly) == 1, Assembly.assembly_default)],
				else_=Assembly.name
			)
			lowered_assemblies = [name.lower() for name in assembly_name]
			genome_select = genome_select.filter(db.func.lower(conditional_column).in_(lowered_assemblies))

		if ensembl_name is not None:
			genome_select = genome_select.filter(Organism.ensembl_name.in_(ensembl_name))

		if taxonomy_id is not None:
			genome_select = genome_select.filter(Organism.taxonomy_id.in_(taxonomy_id))

		if allow_unreleased:
			# fetch everything (released + unreleased)
			pass
		elif unreleased_only:
			# fetch unreleased only
			# this filter will get all Genome entries where there's no associated GenomeRelease
			# the tilde (~) symbol is used for negation.
			genome_select = genome_select.filter(~Genome.genome_releases.any())
		else:
			# fetch released only
			# Check if genome is released
			# TODO: why did I add this check?! -> removing this breaks the test_update tests
			with self.metadata_db.session_scope() as session:
				session.expire_on_commit = False
				# copy genome_select as we don't want to include GenomeDataset
				# because it results in multiple row for a given genome (genome can have many datasets)
				check_query = genome_select
				prep_query = check_query.add_columns(GenomeDataset) \
					.join(GenomeDataset, Genome.genome_id == GenomeDataset.genome_id) \
					.filter(GenomeDataset.release_id.isnot(None))
				is_genome_released = session.execute(prep_query).first()
			if is_genome_released:
				# Include release related info if is_genome_released is True
				genome_select = genome_select.add_columns(GenomeRelease, EnsemblRelease, EnsemblSite) \
					.join(GenomeRelease, Genome.genome_id == GenomeRelease.genome_id) \
					.join(EnsemblRelease, GenomeRelease.release_id == EnsemblRelease.release_id) \
					.join(EnsemblSite, EnsemblSite.site_id == EnsemblRelease.site_id)

				if release_version is not None and release_version > 0:
					# if release is specified
					genome_select = genome_select.filter(EnsemblRelease.version <= release_version)
					current_only = False

				if current_only:
					genome_select = genome_select.filter(GenomeRelease.is_current == 1)

				if site_name is not None:
					genome_select = genome_select.add_columns(EnsemblSite).filter(EnsemblSite.name == site_name)

				if release_type is not None:
					genome_select = genome_select.filter(EnsemblRelease.release_type == release_type)

			# if is_genome_released and allow_unreleased are False
			# the query shouldn't return anything
			else:
				# This represents an empty list, simulating zero rows returned
				# it prevents returning unreleased data
				return []

		# print(f"genome_select query ====> {str(genome_select)}")
		with self.metadata_db.session_scope() as session:
			session.expire_on_commit = False
			return session.execute(genome_select.order_by("ensembl_name")).all()

	def fetch_genomes_by_genome_uuid(self, genome_uuid, allow_unreleased=False, site_name=None, release_type=None,
									 release_version=None, current_only=True):
		return self.fetch_genomes(
			genome_uuid=genome_uuid,
			allow_unreleased=allow_unreleased,
			site_name=site_name,
			release_type=release_type,
			release_version=release_version,
			current_only=current_only,
		)

	def fetch_genomes_by_assembly_accession(self, assembly_accession, allow_unreleased=False, site_name=None,
											release_type=None, release_version=None, current_only=True):
		return self.fetch_genomes(
			assembly_accession=assembly_accession,
			allow_unreleased=allow_unreleased,
			site_name=site_name,
			release_type=release_type,
			release_version=release_version,
			current_only=current_only,
		)

	def fetch_genomes_by_ensembl_name(self, ensembl_name, allow_unreleased=False, site_name=None, release_type=None,
									  release_version=None, current_only=True):
		return self.fetch_genomes(
			ensembl_name=ensembl_name,
			allow_unreleased=allow_unreleased,
			site_name=site_name,
			release_type=release_type,
			release_version=release_version,
			current_only=current_only,
		)

	def fetch_genomes_by_taxonomy_id(self, taxonomy_id, allow_unreleased=False, site_name=None, release_type=None,
									 release_version=None, current_only=True):
		return self.fetch_genomes(
			taxonomy_id=taxonomy_id,
			allow_unreleased=allow_unreleased,
			site_name=site_name,
			release_type=release_type,
			release_version=release_version,
			current_only=current_only,
		)

	def fetch_genomes_by_scientific_name(
			self,
			scientific_name,
			allow_unreleased=False,
			site_name=None,
			release_type=None,
			release_version=None,
			current_only=True,
	):
		taxonomy_ids = self.fetch_taxonomy_ids(scientific_name)

		return self.fetch_genomes_by_taxonomy_id(
			taxonomy_ids,
			allow_unreleased=allow_unreleased,
			site_name=site_name,
			release_type=release_type,
			release_version=release_version,
			current_only=current_only,
		)

	def fetch_genome_by_keyword(self, keyword=None, release_version=None):
		"""
		Fetches genomes based on a keyword and release version.

		Args:
			keyword (str or None): Keyword to search for in various attributes of genomes, assemblies, and organisms.
			release_version (int or None): Release version to filter by. If set to 0 or None, fetches only current genomes.

		Returns:
			list: A list of fetched genomes matching the keyword and release version.
		"""
		genome_query = db.select(
			Genome, GenomeRelease, EnsemblRelease, Assembly, Organism, EnsemblSite
		).select_from(Genome) \
			.outerjoin(Organism, Organism.organism_id == Genome.organism_id) \
			.outerjoin(Assembly, Assembly.assembly_id == Genome.assembly_id) \
			.outerjoin(GenomeRelease, Genome.genome_id == GenomeRelease.genome_id) \
			.outerjoin(EnsemblRelease, GenomeRelease.release_id == EnsemblRelease.release_id) \
			.outerjoin(EnsemblSite, EnsemblSite.site_id == EnsemblRelease.site_id)

		if keyword is not None:
			genome_query = genome_query.where(db.or_(db.func.lower(Assembly.tol_id) == keyword.lower(),
													 db.func.lower(Assembly.accession) == keyword.lower(),
													 db.func.lower(Assembly.name) == keyword.lower(),
													 db.func.lower(Assembly.ensembl_name) == keyword.lower(),
													 db.func.lower(Organism.common_name) == keyword.lower(),
													 db.func.lower(Organism.scientific_name) == keyword.lower(),
													 db.func.lower(
														 Organism.scientific_parlance_name) == keyword.lower(),
													 db.func.lower(Organism.species_taxonomy_id) == keyword.lower()))

		if release_version == 0 or release_version is None:
			genome_query = genome_query.where(EnsemblRelease.is_current == 1)
		else:
			genome_query = genome_query.where(EnsemblRelease.version <= release_version)

		with self.metadata_db.session_scope() as session:
			session.expire_on_commit = False
			return session.execute(genome_query).all()

	def fetch_sequences(self, genome_id=None, genome_uuid=None, assembly_uuid=None, assembly_accession=None,
						assembly_sequence_accession=None, assembly_sequence_name=None, chromosomal_only=False):
		"""
		Fetches sequences based on the provided parameters.

		Args:
			genome_id (int or None): Genome ID to filter by.
			genome_uuid (str or None): Genome UUID to filter by.
			assembly_uuid (Union[str, List[str]]): The assembly_uuid of the assembly(s) to fetch.
			assembly_accession (str or None): Assembly accession to filter by.
			assembly_sequence_accession (str or None): Assembly Sequence accession to filter by.
			assembly_sequence_name (str or None): Assembly Sequence name to filter by.
			chromosomal_only (bool): Flag indicating whether to fetch only chromosomal sequences.

		Returns:
			list: A list of fetched sequences.
		"""
		genome_id = check_parameter(genome_id)
		genome_uuid = check_parameter(genome_uuid)
		assembly_uuid = check_parameter(assembly_uuid)
		assembly_accession = check_parameter(assembly_accession)
		assembly_sequence_accession = check_parameter(assembly_sequence_accession)
		assembly_sequence_name = check_parameter(assembly_sequence_name)

		seq_select = db.select(
			Genome, Assembly, AssemblySequence
		).select_from(Genome) \
			.join(Assembly, Assembly.assembly_id == Genome.assembly_id) \
			.join(AssemblySequence, AssemblySequence.assembly_id == Assembly.assembly_id)

		if chromosomal_only:
			seq_select = seq_select.filter(AssemblySequence.chromosomal == 1)

		# These options are in order of decreasing specificity,
		# and thus the ones later in the list can be redundant.
		if genome_id is not None:
			seq_select = seq_select.filter(Genome.genome_id == genome_id)

		if genome_uuid is not None:
			seq_select = seq_select.filter(Genome.genome_uuid == genome_uuid)

		if assembly_accession is not None:
			seq_select = seq_select.filter(Assembly.accession == assembly_accession)

		if assembly_uuid is not None:
			seq_select = seq_select.filter(Assembly.assembly_uuid.in_(assembly_uuid))

		if assembly_sequence_accession is not None:
			seq_select = seq_select.filter(AssemblySequence.accession == assembly_sequence_accession)

		if assembly_sequence_name is not None:
			seq_select = seq_select.filter(AssemblySequence.name == assembly_sequence_name)

		with self.metadata_db.session_scope() as session:
			session.expire_on_commit = False
			return session.execute(seq_select).all()

	def fetch_sequences_by_genome_uuid(self, genome_uuid, chromosomal_only=False):
		return self.fetch_sequences(
			genome_uuid=genome_uuid, chromosomal_only=chromosomal_only
		)

	def fetch_sequences_by_assembly_accession(
			self, assembly_accession, chromosomal_only=False
	):
		return self.fetch_sequences(
			assembly_accession=assembly_accession, chromosomal_only=chromosomal_only
		)

	def fetch_genome_datasets(self, genome_id=None, genome_uuid=None, organism_uuid=None, allow_unreleased=False,
							  unreleased_only=False, dataset_uuid=None, dataset_name=None, dataset_source=None,
							  dataset_type=None, release_version=None, dataset_attributes=None):
		"""
		Fetches genome datasets based on the provided parameters.

		Args:
			genome_id (int or list or None): Genome ID(s) to filter by.
			genome_uuid (str or list or None): Genome UUID(s) to filter by.
			organism_uuid (str or list or None): Organism UUID(s) to filter by.
			allow_unreleased (bool): Flag indicating whether to allowing fetching unreleased datasets too or not.
			unreleased_only (bool): Fetch only unreleased datasets (default: False). allow_unreleased is used by gRPC
									 to fetch both released and unreleased datasets, while unreleased_only
									 is used in production pipelines (fetches only unreleased datasets)
			dataset_uuid (str or list or None): Dataset UUID(s) to filter by.
			dataset_name (str or None): Dataset name to filter by, default is 'assembly'.
			dataset_source (str or None): Dataset source to filter by.
			dataset_type (str or None): Dataset type to filter by.
			release_version (float or None): EnsemblRelease version to filter by.
			dataset_attributes (bool): Flag to include dataset attributes

		Returns:
			List[Tuple[
					Genome, GenomeDataset, Dataset, DatasetType,
					DatasetSource, EnsemblRelease, DatasetAttribute, Attribute
				]]: A list of tuples containing the fetched genome information.
			Each tuple contains the following elements:
				- Genome: An instance of the Genome class.
				- Organism: An instance of the Organism class.
				- GenomeDataset: An instance of the GenomeDataset class.
				- Dataset: An instance of the Dataset class.
				- DatasetType: An instance of the DatasetType class.
				- DatasetSource: An instance of the DatasetSource class.
				- EnsemblRelease: An instance of the EnsemblRelease class.
				- DatasetAttribute: An instance of the DatasetAttribute class.
				- Attribute: An instance of the Attribute class.

		Raises:
			ValueError: If an exception occurs during the fetch process.

		"""
		try:
			genome_select = db.select(
				Genome,
				GenomeDataset,
				Dataset,
				DatasetType,
				DatasetSource,
			).select_from(Genome) \
				.join(GenomeDataset, Genome.genome_id == GenomeDataset.genome_id) \
				.join(Dataset, GenomeDataset.dataset_id == Dataset.dataset_id) \
				.join(DatasetType, Dataset.dataset_type_id == DatasetType.dataset_type_id) \
				.join(DatasetSource, Dataset.dataset_source_id == DatasetSource.dataset_source_id).order_by(
				Genome.genome_uuid, Dataset.dataset_uuid)

			# set default group topic as 'assembly' to fetch unique datasource
			if not dataset_name:
				dataset_name = "assembly"

			genome_id = check_parameter(genome_id)
			genome_uuid = check_parameter(genome_uuid)
			organism_uuid = check_parameter(organism_uuid)
			dataset_uuid = check_parameter(dataset_uuid)
			dataset_name = check_parameter(dataset_name)
			dataset_source = check_parameter(dataset_source)
			dataset_type = check_parameter(dataset_type)

			if genome_id is not None:
				genome_select = genome_select.filter(Genome.genome_id.in_(genome_id))

			if genome_uuid is not None:
				genome_select = genome_select.filter(Genome.genome_uuid.in_(genome_uuid))

			if organism_uuid is not None:
				genome_select = genome_select.join(Organism, Organism.organism_id == Genome.organism_id) \
					.filter(Organism.organism_uuid.in_(organism_uuid))

			if dataset_uuid is not None:
				genome_select = genome_select.filter(Dataset.dataset_uuid.in_(dataset_uuid))

			if "all" in dataset_name:
				# TODO: fetch the list dynamically from the DB
				dataset_type_names = [
					'assembly', 'genebuild', 'variation', 'evidence',
					'regulation_build', 'homologies', 'regulatory_features'
				]
				genome_select = genome_select.filter(DatasetType.name.in_(dataset_type_names))
			else:
				genome_select = genome_select.filter(DatasetType.name.in_(dataset_name))

			if dataset_source is not None:
				genome_select = genome_select.filter(DatasetSource.name.in_(dataset_source))

			if dataset_type is not None:
				genome_select = genome_select.filter(DatasetType.name.in_(dataset_type))

			if dataset_attributes:
				genome_select = genome_select.add_columns(DatasetAttribute, Attribute) \
					.join(DatasetAttribute, DatasetAttribute.dataset_id == Dataset.dataset_id) \
					.join(Attribute, Attribute.attribute_id == DatasetAttribute.attribute_id).order_by(Attribute.name)

			if allow_unreleased:
				# Get everything
				pass
			elif unreleased_only:
				# Get only unreleased datasets
				# this filter will get all Datasets entries where there's no associated GenomeDataset
				# the tilde (~) symbol is used for negation.
				genome_select = genome_select.filter(~GenomeDataset.ensembl_release.has())
			else:
				# Get released datasets only
				# Check if dataset is released
				with self.metadata_db.session_scope() as session:
					# This is needed in order to ovoid tests throwing:
					# sqlalchemy.orm.exc.DetachedInstanceError: Instance <DatasetType at 0x7fc5c05a13d0>
					# is not bound to a Session; attribute refresh operation cannot proceed
					# (Background on this error at: https://sqlalche.me/e/14/bhk3)
					session.expire_on_commit = False
					# Check if GenomeDataset HAS an ensembl_release
					prep_query = genome_select.filter(GenomeDataset.ensembl_release.has())
					is_dataset_released = session.execute(prep_query).first()

				if is_dataset_released:
					# Include release related info
					genome_select = genome_select.add_columns(EnsemblRelease) \
						.join(EnsemblRelease, GenomeDataset.release_id == EnsemblRelease.release_id)

					if release_version:
						genome_select = genome_select.filter(EnsemblRelease.version <= release_version)

			# print(f"genome_select str ====> {str(genome_select)}")
			logger.debug(genome_select)
			with self.metadata_db.session_scope() as session:
				session.expire_on_commit = False
				return session.execute(genome_select).all()

		except Exception as e:
			raise ValueError(str(e))

	def fetch_genomes_info(
			self,
			genome_id=None,
			genome_uuid=None,
			allow_unreleased_genomes=False,
			ensembl_name=None,
			group=None,
			group_type=None,
			allow_unreleased_datasets=False,
			dataset_name=None,
			dataset_source=None,
			dataset_attributes=True,

	):
		try:
			genome_id = check_parameter(genome_id)
			genome_uuid = check_parameter(genome_uuid)
			ensembl_name = check_parameter(ensembl_name)
			group = check_parameter(group)
			group_type = check_parameter(group_type)
			dataset_name = check_parameter(dataset_name)
			dataset_source = check_parameter(dataset_source)

			if group is None:
				group_type = group_type if group_type else ['Division']
				with self.metadata_db.session_scope() as session:
					session.expire_on_commit = False
					group = [org_type[0] for org_type in session.execute(
						db.select(OrganismGroup.name).filter(OrganismGroup.type.in_(group_type))).all()]

			# get genome, assembly and organism information
			genomes = self.fetch_genomes(
				genome_id=genome_id,
				genome_uuid=genome_uuid,
				allow_unreleased=allow_unreleased_genomes,
				ensembl_name=ensembl_name,
				group=group,
				group_type=group_type,
			)

			for genome in genomes:
				dataset = self.fetch_genome_datasets(
					genome_uuid=genome[0].genome_uuid,
					allow_unreleased=allow_unreleased_datasets,
					dataset_name=dataset_name,
					dataset_source=dataset_source,
					dataset_attributes=dataset_attributes
				)
				res = [{'genome': genome, 'datasets': dataset}]
				yield res
		except Exception as e:
			raise ValueError(str(e))

	def fetch_organisms_group_counts(self, release_version=None, group_code='popular'):
		o_species = aliased(Organism)
		o = aliased(Organism)
		if not release_version:
			# Get latest released organisms
			query = db.select(
				o_species.species_taxonomy_id,
				o_species.ensembl_name,
				o_species.common_name,
				o_species.scientific_name,
				OrganismGroupMember.order.label('order'),
				db.func.count().label('count')
			)

			query = query.join(o, o_species.species_taxonomy_id == o.species_taxonomy_id)
			query = query.join(Genome, o.organism_id == Genome.organism_id)
			query = query.join(Assembly, Genome.assembly_id == Assembly.assembly_id)
			query = query.join(OrganismGroupMember, o_species.organism_id == OrganismGroupMember.organism_id)
			query = query.join(OrganismGroup,
							   OrganismGroupMember.organism_group_id == OrganismGroup.organism_group_id)
			query = query.filter(OrganismGroup.code == group_code)

			query = query.group_by(
				o_species.species_taxonomy_id,
				o_species.ensembl_name,
				o_species.common_name,
				o_species.scientific_name,
				OrganismGroupMember.order
			)
			query = query.order_by(OrganismGroupMember.order)
		else:
			# change group to release_version_state and related genomes
			raise NotImplementedError('Not implemented yet')
			pass

		with self.metadata_db.session_scope() as session:
			# TODO check if we should return a dictionary instead
			return session.execute(query).all()

	def fetch_related_assemblies_count(self, species_taxonomy_id=None, release_version=None):
		o_species = aliased(Organism)
		o = aliased(Organism)
		if not release_version:
			# Get latest released organisms
			query = db.select(
				o_species.species_taxonomy_id,
				o_species.ensembl_name,
				o_species.common_name,
				o_species.scientific_name,
				db.func.count().label('count')
			)

			query = query.join(o, o_species.species_taxonomy_id == o.species_taxonomy_id)
			query = query.join(Genome, o.organism_id == Genome.organism_id)
			query = query.join(Assembly, Genome.assembly_id == Assembly.assembly_id)
			query = query.filter(o_species.species_taxonomy_id == species_taxonomy_id)

			query = query.group_by(
				o_species.species_taxonomy_id,
				o_species.ensembl_name,
				o_species.common_name,
				o_species.scientific_name,
			)
		else:
			# change group to release_version_state and related genomes
			raise NotImplementedError('Not implemented yet')
			pass

		# print(f"query ---> {query}")
		with self.metadata_db.session_scope() as session:
			# TODO check if we should return a dictionary instead
			return session.execute(query).all()
