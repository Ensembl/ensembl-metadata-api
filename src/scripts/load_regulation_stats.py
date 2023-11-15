#!/usr/env/bin/python
import os
from pathlib import Path
import json

from ensembl.database import DBConnection

from ensembl.production.metadata.api.models import *
from sqlalchemy import select, delete

metadata_uri = os.getenv('METADATA_URI', 'mysql://ensembl@localhost:3306/ensembl_genome_metadata')
taxonomy_uri = os.getenv('TAXONOMY_URI', 'mysql://ensembl@localhost:3306/ncbi_taxonomy')

db_conn = DBConnection(metadata_uri)

with db_conn.session_scope() as session:
    # delete all dataset associated with following data
    dataset_type = session.execute(select(DatasetType).filter(DatasetType.name == 'regulatory_features')).fetchone()[0]
    session.query(GenomeDataset).filter(GenomeDataset.dataset.has(Dataset.dataset_type == dataset_type)).delete(
        synchronize_session='fetch')
    session.execute(delete(Dataset).filter(Dataset.dataset_type == dataset_type))
    p = Path('../scripts/regulation/')
    files = list(p.glob('*/*/*.json'))
    for file in files:
        assembly = file.parent
        organism = assembly.parent
        stats = json.loads(file.read_text())
        dataset_attribute = stats.get('dataset_attribute')
        dataset_source = stats.get('dataset_source')
        dataset_type.description = stats.get('description')
        dataset_type.label = stats.get('label')
        genome_uuid = stats.get('genome_uuid')
        print("File", file, assembly.name, organism.name, dataset_attribute)
        print("OrganismName", organism.name)
        organism = session.execute(select(Organism).where(Organism.ensembl_name == organism.name)).scalars().first()
        if organism:
            print(f"Found from ensembl_name {organism.ensembl_name} {organism.organism_uuid}")
            if organism.ensembl_name == 'homo_sapiens':
                genome = [genome for genome in organism.genomes if genome.assembly.name == 'GRCh38.p14'][0]
            else:
                genome = organism.genomes[0]
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

            regulation_dataset = session.execute(
                select(Dataset).join_from(Dataset,
                                          DatasetSource).filter(Dataset.name == 'regulatory_features',
                                                                DatasetSource.name == f'{dataset_source["name"]}')).first()
            if not regulation_dataset:
                regulation_dataset = Dataset(dataset_uuid=str(uuid.uuid4()),
                                             dataset_source=data_source,
                                             dataset_type=dataset_type,
                                             name=stats['name'],
                                             version='1.0',
                                             label=stats['label'],
                                             status='Submitted')
                genome_dataset = GenomeDataset(dataset=regulation_dataset, genome=genome, is_current=1)
                session.add(regulation_dataset)
                session.add(genome_dataset)

            for attr in dataset_attribute:
                attribute = session.execute(
                    select(Attribute).filter(Attribute.name == f"{attr['name']}")).first()
                if not attribute:
                    print("Creating attribute ")
                    attribute = Attribute(name=f"{attr['name']}",
                                          label=f"{attr['name']}",
                                          description=f"{attr['description']}",
                                          type='integer')
                    session.add(attribute)
                else:
                    attribute = attribute[0]
                regulation_dataset.dataset_attributes.append(DatasetAttribute(attribute=attribute,
                                                                              value=f"{attr['value']}"))
        else:
            print('Error!')
            continue
        # session.add(datasets)
        session.commit()
        print('Genomedatasets', [dataset for dataset in genome.genome_datasets])
        # exit(1)
