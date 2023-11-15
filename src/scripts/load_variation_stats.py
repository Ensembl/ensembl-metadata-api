#!/usr/env/bin/python

from pathlib import Path
import json
import os
from ensembl.database import DBConnection

from ensembl.production.metadata.api.models import *
from sqlalchemy import select, delete

metadata_uri = os.getenv('METADATA_URI', 'mysql://ensembl@localhost:3306/ensembl_genome_metadata')
taxonomy_uri = os.getenv('TAXONOMY_URI', 'mysql://ensembl@localhost:3306/ncbi_taxonomy')

db_conn = DBConnection(metadata_uri)

with db_conn.session_scope() as session:
	# delete all dataset associated with following data
	dataset_type = session.execute(select(DatasetType).filter(DatasetType.name == 'variation')).fetchone()[0]
	session.query(GenomeDataset).filter(GenomeDataset.dataset.has(Dataset.dataset_type == dataset_type)).delete(
		synchronize_session='fetch')
	session.execute(delete(Dataset).filter(Dataset.dataset_type == dataset_type))
	p = Path('../scripts/variation/')
	files = list(p.glob('*.json'))
	for file in files:
		all_stats = json.loads(file.read_text())
		for stats in all_stats:
			dataset_attribute = stats.get('dataset_attribute')
			dataset_source = stats.get('dataset_source')
			dataset_type.description = stats.get('description')
			dataset_type.label = stats.get('label')
			genome_uuid = stats.get('genome_uuid')
			genome = session.query(Genome).where(Genome.genome_uuid == genome_uuid).one()
			if genome:
				print(f"Found from ensembl_name {genome.organism.ensembl_name} {genome.organism.organism_uuid}")
				print(f'Genome {genome.genome_uuid}')
				data_source = session.execute(
					select(DatasetSource).filter(DatasetSource.name == dataset_source['name'])).fetchone()
				if not data_source:
					print("Creating datasource ")
					data_source = DatasetSource(type=dataset_source['type'],
					                            name=dataset_source['name'])
					session.add(data_source)
				else:
					data_source = data_source[0]

				variation_dataset = session.execute(
					select(Dataset).join_from(Dataset,
					                          DatasetSource).filter(Dataset.name == 'regulatory_features',
					                                                DatasetSource.name == f'{dataset_source["name"]}')).first()
				if not variation_dataset:
					variation_dataset = Dataset(dataset_uuid=str(uuid.uuid4()),
					                            dataset_source=data_source,
					                            dataset_type=dataset_type,
					                            name=stats['name'],
					                            version='1.0',
					                            label=stats['label'],
					                            status='Submitted')
					genome_dataset = GenomeDataset(dataset=variation_dataset, genome=genome, is_current=1)
					session.add(variation_dataset)
					session.add(genome_dataset)

				for attr in dataset_attribute:
					attribute = session.execute(
						select(Attribute).filter(Attribute.name == f"{attr['name']}")).first()
					if not attribute:
						print("Creating attribute ")
						attr_type = 'integer' if attr['name'] == 'variation.short_variants' else 'string'
						attribute = Attribute(name=f"{attr['name']}",
						                      label=f"{attr['name']}",
						                      description=f"{attr['name']}",
						                      type=attr_type)
						session.add(attribute)
					else:
						attribute = attribute[0]
					variation_dataset.dataset_attributes.append(DatasetAttribute(attribute=attribute,
					                                                             value=f"{attr['value']}"))
			else:
				print('Error!')
				continue
			session.commit()
			print('Genomedatasets', [dataset for dataset in genome.genome_datasets])
		# exit(1)
