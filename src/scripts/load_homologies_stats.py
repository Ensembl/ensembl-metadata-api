#!/usr/env/bin/python

from pathlib import Path
import json
import re
import os
from ensembl.database import DBConnection

from ensembl.production.metadata.api.models import *
from sqlalchemy import select, delete

metadata_uri = os.getenv('METADATA_URI', 'mysql://ensembl@localhost:3306/ensembl_genome_metadata')
taxonomy_uri = os.getenv('TAXONOMY_URI', 'mysql://ensembl@localhost:3306/ncbi_taxonomy')

db_conn = DBConnection(metadata_uri)

with db_conn.session_scope() as session:
    # delete all dataset associated with following data
    dataset_type = session.execute(select(DatasetType).filter(DatasetType.name == 'homologies')).fetchone()[0]
    #session.query(GenomeDataset).filter(GenomeDataset.dataset.has(Dataset.dataset_type == dataset_type)).delete(
    #    synchronize_session='fetch')
    #session.execute(delete(Dataset).filter(Dataset.dataset_type == dataset_type))
    p = Path('../scripts/homology_stats_241/')
    files = list(p.glob('*.json'))
    for file in files:
        organism_name = file.name.split('_compara')[0]
        stats = json.loads(file.read_text())
        against_1 = [(key, stats[key]) for key in stats.keys()]
        against = against_1[0][0].split('against_')[1]
        stat = against_1[0][1]
        print("File", file, file.name, organism_name, stats, against, stat)
        print("OrganismName", organism_name)
        organism = session.execute(select(Organism).where(Organism.ensembl_name == organism_name)).scalars().first()
        if organism:
            print(f"Found from ensembl_name {organism_name} {organism.organism_uuid}")
            if organism.ensembl_name == 'homo_sapiens':
                genome = [genome for genome in organism.genomes if genome.assembly.name == 'GRCh38.p14'][0]
            else:
                genome = organism.genomes[0]
        else:
            # try to get from gca
            gca = organism_name.split('_')[2]
            p = re.compile('(gca\_?)([0-9]+)v([0-9]+)')
            gca_infos = p.match(gca)
            if gca_infos:
                print(f"Found from gca {gca_infos.group(0)}")
                number = gca_infos.group(2)
                version = gca_infos.group(3)
                print(gca, gca_infos.group(0), number, version)
                query = select(Assembly).where(Assembly.accession.like(f'%{number}%'))
                print(query)
                result = session.execute(query).scalars().all()
                assembly = result[0]
                genome = assembly.genomes[0]
            elif organism_name == 'homo_sapiens_37':
                print(f"Homo sapiens GRC37")
                query = select(Assembly).filter(Assembly.name == 'GRCh37.p13')
                assembly = session.execute(query).fetchone()[0]
                genome = assembly.genomes[0]
            else:
                print(f"no match for organism {organism_name}")
                continue
        session.add(genome)
        organism = genome.organism
        genome_uuid = genome.genome_uuid
        print('GenomeUUID/Organism', genome_uuid, organism.ensembl_name)
        datasets = genome.genome_datasets
        print('GenomeDatasets', [dataset for dataset in datasets])
        data_source = session.execute(select(DatasetSource).filter(DatasetSource.name == file.stem)).fetchone()
        if not data_source:
            print("Creating datasource")
            data_source = DatasetSource(type='compara',
                                        name=file.stem)

            session.add(data_source)
        else:
            data_source = data_source[0]
        attribute = session.execute(
            select(Attribute).filter(Attribute.name == f'compara.homologs_{against}')).fetchone()

        if not attribute:
            print("Creating attribute ")
            attribute = Attribute(name=f'compara.homologs_{against}',
                                  label=f'compara.homologs_{against}',
                                  description=f'compara.homologs_{against}',
                                  type='float')
            session.add(attribute)
        else:
            attribute = attribute[0]

        homology_datasets = session.execute(
                select(Dataset).join_from(Dataset,
                                          DatasetSource).filter(Dataset.name == 'compara_homologies',
                                                                DatasetSource.name == f'compara.homologs_{against}')).first()

        if not homology_datasets:
            homology_dataset = Dataset(dataset_uuid=str(uuid.uuid4()),
                                       dataset_source=data_source,
                                       dataset_type=dataset_type,
                                       name='compara_homologies',
                                       version='1.0',
                                       label='Compara homologies ',
                                       status='Submitted')
            session.add(homology_dataset)
            genome_dataset = GenomeDataset(dataset=homology_dataset, genome=genome, is_current=1)
            session.add(genome_dataset)
            homology_dataset.dataset_attributes.append(DatasetAttribute(attribute=attribute,
                                                                        value=str(stat)))
        # session.add(datasets)
        session.commit()
        print('Genomedatasets', [dataset for dataset in datasets])
        # exit(1)
