insert into ensembl_site (name, label, uri) values
('vertebrates', 'Ensembl', 'https://ensembl.org/'),
('bacteria', 'Ensembl Bacteria', 'https://bacteria.ensembl.org/'),
('fungi', 'Ensembl Fungi', 'https://fungi.ensembl.org/'),
('metazoa', 'Ensembl Metazoa', 'https://metazoa.ensembl.org/'),
('protists', 'Ensembl Protists', 'https://protists.ensembl.org/'),
('plants', 'Ensembl Plants', 'https://plants.ensembl.org/'),
('rapid', 'Rapid Release', 'https://rapid.ensembl.org/'),
('covid-19', 'COVID-19', 'https://covid-19.ensembl.org/');

insert into ensembl_release
  (version, release_date, is_current, release_type, site_id)
select ensembl_genomes_version, release_date, is_current, 'partial', site_id
from ensembl_metadata.data_release, ensembl_site
where ensembl_site.name = 'rapid';

insert into dataset_type
  (name, label, topic, description)
values
  ('assembly', 'Genomic assembly', 'assembly', NULL),
  ('checksum_xrefs', 'Cross-references', 'geneset_annotation', 'Cross-references assigned by checksum (i.e. sequence identity)'),
  ('alignment_xrefs', 'Cross-references', 'geneset_annotation', 'Cross-references assigned by alignment (i.e. sequence similarity)'),
  ('dependent_xrefs', 'Cross-references', 'geneset_annotation', 'Cross-references assigned via an intermediate annotation'),
  ('direct_xrefs', 'Cross-references', 'geneset_annotation', 'Cross-references imported from external annotation'),
  ('dna_alignments', 'DNA alignments', 'assembly_annotation', 'Features aligned against the genome'),
  ('geneset', 'Geneset', 'geneset_annotation', NULL),
  ('gene_families', 'Gene families', 'comparative', NULL),
  ('gene_trees', 'Gene trees', 'comparative', NULL),
  ('multiple_alignment', 'Multiple genome alignment', 'comparative', NULL),
  ('pairwise_alignment', 'Pairwise genome alignment', 'comparative', NULL),
  ('go_terms', 'GO terms', 'geneset_annotation', NULL),
  ('homologies', 'Homologies', 'comparative', NULL),
  ('microarrays', 'Microarrays', 'geneset_annotation', NULL),
  ('phenotypes', 'Phenotypes', 'variation', NULL),
  ('protein_alignments', 'Protein alignments', 'geneset_annotation', 'Features aligned against proteins'),
  ('protein_features', 'Protein domains and features', 'geneset_annotation', NULL),
  ('repeat_features', 'Repeat features', 'assembly_annotation', NULL),
  ('rnaseq_alignments', 'RNAseq alignments', 'assembly_annotation', 'RNAseq data aligned against the genome'),
  ('syntenies', 'Syntenies', 'comparative', NULL),
  ('variants', 'Variants', 'variation', NULL);

insert into assembly
  (accession, name, ucsc_name, level)
select assembly_accession, assembly_name, assembly_ucsc, assembly_level
from ensembl_metadata.assembly;

update assembly set ucsc_name = NULL where ucsc_name = '';

insert into organism
  (display_name, ensembl_name, scientific_name, species_taxonomy_id, strain, taxonomy_id, url_name)
select o1.display_name, o1.name, o1.scientific_name, o1.species_taxonomy_id, o1.strain, o1.taxonomy_id, o1.url_name
from ensembl_metadata.organism o1
group by o1.display_name, o1.name, o1.scientific_name, o1.species_taxonomy_id, o1.strain, o1.taxonomy_id, o1.url_name;

update organism set strain=null where strain='reference';

-- Temporarily add columns, to ease data population
alter table genome add column assembly_accession varchar(128);
alter table genome add column genebuild varchar(128);

insert into genome
  (genome_uuid, assembly_id, organism_id, assembly_accession, genebuild, created)
select uuid(), a2.assembly_id, o2.organism_id, a1.assembly_accession, g1.genebuild, min(ge1.creation_time)
from ensembl_metadata.genome g1 inner join
  ensembl_metadata.organism o1 on g1.organism_id = o1.organism_id inner join
  ensembl_metadata.assembly a1 on g1.assembly_id = a1.assembly_id inner join
  ensembl_metadata.genome_event ge1 on g1.genome_id = ge1.genome_id inner join
  assembly a2 on a1.assembly_accession = a2.accession inner join
  organism o2 on o1.name = o2.ensembl_name
group by a2.assembly_id, o2.organism_id, a1.assembly_accession, g1.genebuild;

insert into dataset_source
  (type, name)
select type, dbname
from ensembl_metadata.genome_database
group by type, dbname
order by dbname;

insert into dataset_source
  (type, name)
select 'compara', dbname
from ensembl_metadata.compara_analysis
group by dbname
order by dbname;

-- Create assembly datasets
insert into dataset
  (dataset_uuid, dataset_type_id, name, label, version, created, dataset_source_id)
select uuid(), dt.dataset_type_id, 'assembly', a1.assembly_accession, NULL, min(ge1.creation_time), ds.dataset_source_id
from ensembl_metadata.genome g1 inner join
  ensembl_metadata.assembly a1 on g1.assembly_id = a1.assembly_id inner join
  ensembl_metadata.genome_event ge1 on g1.genome_id = ge1.genome_id inner join
  ensembl_metadata.genome_database gd1 on g1.genome_id = gd1.genome_id inner join
  dataset_type dt inner join
  dataset_source ds on gd1.dbname = ds.name inner join
  genome g2 on a1.assembly_accession = g2.assembly_accession
where gd1.type = 'core' and dt.name = 'assembly'
group by dt.dataset_type_id, a1.assembly_accession, ds.dataset_source_id;

-- Create geneset datasets
insert into dataset
  (dataset_uuid, dataset_type_id, name, label, version, created, dataset_source_id)
select uuid(), dt.dataset_type_id, 'gene_core', g1.genebuild, a1.assembly_accession, min(ge1.creation_time), ds.dataset_source_id
from ensembl_metadata.genome g1 inner join
  ensembl_metadata.assembly a1 on g1.assembly_id = a1.assembly_id inner join
  ensembl_metadata.genome_event ge1 on g1.genome_id = ge1.genome_id inner join
  ensembl_metadata.genome_database gd1 on g1.genome_id = gd1.genome_id inner join
  dataset_type dt inner join
  dataset_source ds on gd1.dbname = ds.name inner join
  genome g2 on a1.assembly_accession = g2.assembly_accession
where gd1.type = 'core' and dt.name = 'geneset' and g1.genebuild = g2.genebuild
group by dt.dataset_type_id, g1.genebuild, a1.assembly_accession, ds.dataset_source_id;

-- Link assembly datasets to genomes
insert into genome_dataset
  (genome_id, dataset_id, release_id, is_current)
select g2.genome_id, d2.dataset_id, min(r2.release_id), 1
from ensembl_metadata.genome g1 inner join
  ensembl_metadata.assembly a1 on g1.assembly_id = a1.assembly_id inner join
  ensembl_metadata.data_release dr1 on g1.data_release_id = dr1.data_release_id inner join
  ensembl_metadata.genome_database gd1 on g1.genome_id = gd1.genome_id inner join
  genome g2 on a1.assembly_accession = g2.assembly_accession inner join
  dataset d2 on g2.assembly_accession = d2.label inner join
  dataset_source ds on gd1.dbname = ds.name inner join
  ensembl_release r2 on dr1.ensembl_genomes_version = r2.version inner join
  ensembl_site s on r2.site_id = s.site_id
where s.name = 'rapid' and g1.genebuild = g2.genebuild and d2.dataset_source_id = ds.dataset_source_id
group by g2.genome_id, d2.dataset_id;

-- Link genomes to a release
insert into genome_release
  (genome_id, release_id, is_current)
select genome_id, min(release_id), 1
from genome_dataset
group by genome_id;

-- Link geneset datasets to genomes
insert into genome_dataset
  (genome_id, dataset_id, release_id, is_current)
select g2.genome_id, d2.dataset_id, min(r2.release_id), 1
from ensembl_metadata.genome g1 inner join
  ensembl_metadata.assembly a1 on g1.assembly_id = a1.assembly_id inner join
  ensembl_metadata.data_release dr1 on g1.data_release_id = dr1.data_release_id inner join
  ensembl_metadata.genome_database gd1 on g1.genome_id = gd1.genome_id inner join
  genome g2 on a1.assembly_accession = g2.assembly_accession inner join
  dataset d2 on g2.genebuild = d2.label and g2.assembly_accession = d2.version inner join
  dataset_source ds on gd1.dbname = ds.name inner join
  ensembl_release r2 on dr1.ensembl_genomes_version = r2.version inner join
  ensembl_site s on r2.site_id = s.site_id
where s.name = 'rapid' and g1.genebuild = g2.genebuild and d2.dataset_source_id = ds.dataset_source_id
group by g2.genome_id, d2.dataset_id;

-- Rapid release doesn't use strain groups, but we can create a dog group,
-- so that we've got a bit of data in the tables to play with.
insert into organism_group (type, name) values ('breeds', 'Dog breeds');
insert into organism_group_member (is_reference, organism_id, organism_group_id)
  select 0, o.organism_id, og.organism_group_id
  from organism o, organism_group og
  where o.scientific_name = 'Canis lupus familiaris' and og.name = 'Dog breeds';
update organism_group_member ogm inner join
  organism o on ogm.organism_id = o.organism_id
  set is_reference = 1
  where o.strain = 'Labrador retriever';

-- Remove temporary genebuild version
update dataset set version = NULL where name = 'gene_core';

-- Remove temporary columns
alter table genome drop column assembly_accession;
alter table genome drop column genebuild;

-- Import dumped data from core databases
insert into dataset
  (dataset_uuid, dataset_type_id, name, label, version, created, dataset_source_id)
select uuid(), dt.dataset_type_id, td.name, td.label, td.version, now(), ds.dataset_source_id
from
  tmp_dataset td inner join
  dataset_type dt on td.type = dt.name inner join
  dataset_source ds on td.dbname = ds.name
;

insert into genome_dataset
  (dataset_id, genome_id, release_id, is_current)
select d2.dataset_id, gd.genome_id, gd.release_id, 1
from
  genome_dataset gd inner join
  dataset d1 on gd.dataset_id = d1.dataset_id inner join
  dataset_source ds1 on d1.dataset_source_id = ds1.dataset_source_id inner join
  dataset_source ds2 on ds1.name =
    replace(replace(ds2.name, 'otherfeatures', 'core'), 'rnaseq', 'core') inner join
  dataset d2 on d2.dataset_source_id = ds2.dataset_source_id
where
  d1.name = 'gene_core' and
  d2.name not in ('assembly', 'gene_core')
;

update
  dataset d1 inner join
  dataset d2 using (dataset_source_id) inner join
  genome_dataset gd1 on d2.dataset_id = gd1.dataset_id inner join
  genome_dataset gd2 on d2.dataset_id = gd2.dataset_id
set
  d1.created = d2.created
where
  gd1.genome_id = gd2.genome_id and
  d1.name not in ('assembly', 'gene_core') and
  d2.name = 'gene_core'
;

insert into attribute (name, label)
select distinct name, label from tmp_attribute order by name;

update attribute set label = 'Protein-coding exon' where name = 'exon_coding';
update attribute set label = 'Long ncRNA exon' where name = 'exon_lnoncoding';
update attribute set label = 'Non-coding exon' where name = 'exon_mnoncoding';
update attribute set label = 'Pseudogenic exon' where name = 'exon_pseudogene';
update attribute set label = 'Short ncRNA exon' where name = 'exon_snoncoding';
update attribute set label = 'Protein-coding gene' where name = 'gene_coding';
update attribute set label = 'Long ncRNA gene' where name = 'gene_lnoncoding';
update attribute set label = 'Non-coding gene' where name = 'gene_mnoncoding';
update attribute set label = 'Pseudogene' where name = 'gene_pseudogene';
update attribute set label = 'Short ncRNA gene' where name = 'gene_snoncoding';
update attribute set label = 'Protein-coding transcript' where name = 'transcript_coding';
update attribute set label = 'Long ncRNA transcript' where name = 'transcript_lnoncoding';
update attribute set label = 'Non-coding transcript' where name = 'transcript_mnoncoding';
update attribute set label = 'Pseudogenic transcript' where name = 'transcript_pseudogene';
update attribute set label = 'Short ncRNA transcript' where name = 'transcript_snoncoding';
update attribute set label = 'Protein-coding cDNA' where name = 'cdna_coding';
update attribute set label = 'Long ncRNA cDNA' where name = 'cdna_lnoncoding';
update attribute set label = 'Non-coding cDNA' where name = 'cdna_mnoncoding';
update attribute set label = 'Pseudogenic cDNA' where name = 'cdna_pseudogene';
update attribute set label = 'Short ncRNA cDNA' where name = 'cdna_snoncoding';
update attribute set label = 'Protein-coding gene' where name = 'gene_genomic_coding';
update attribute set label = 'Long ncRNA gene' where name = 'gene_genomic_lnoncoding';
update attribute set label = 'Non-coding gene' where name = 'gene_genomic_mnoncoding';
update attribute set label = 'Pseudogene' where name = 'gene_genomic_pseudogene';
update attribute set label = 'Short ncRNA gene' where name = 'gene_genomic_snoncoding';
update attribute set label = 'Protein-coding transcript' where name = 'transcript_genomic_coding';
update attribute set label = 'Long ncRNA transcript' where name = 'transcript_genomic_lnoncoding';
update attribute set label = 'Non-coding transcript' where name = 'transcript_genomic_mnoncoding';
update attribute set label = 'Pseudogenic transcript' where name = 'transcript_genomic_pseudogene';
update attribute set label = 'Short ncRNA transcript' where name = 'transcript_genomic_snoncoding';

insert into dataset_attribute
  (type, value, attribute_id, dataset_id)
select ta.type, ta.value, a.attribute_id, d.dataset_id
from
  tmp_attribute ta inner join
  attribute a on ta.name = a.name inner join
  dataset d on ta.dataset_name = d.name inner join
  dataset_source ds on d.dataset_source_id = ds.dataset_source_id inner join
  dataset_type dt on d.dataset_type_id = dt.dataset_type_id inner join
  genome_dataset gd on d.dataset_id = gd.dataset_id inner join
  genome g on gd.genome_id = g.genome_id inner join
  organism o on g.organism_id = o.organism_id
where
  ds.name = ta.dbname and
  o.ensembl_name = ta.ensembl_name and
  dt.name = ta.dataset_type
group by
  ta.type, ta.value, a.attribute_id, d.dataset_id
;

insert into assembly_sequence
  (assembly_id, accession, name, length, chromosomal,
   sequence_location, sequence_checksum, ga4gh_identifier)
select distinct a.assembly_id, tas.accession, tas.name, tas.length, tas.chromosomal,
       tas.sequence_location, tas.sequence_checksum, tas.ga4gh_identifier
from
  tmp_assembly_sequence tas inner join
  assembly a on tas.assembly_accession = a.accession;

create temporary table tmp_chromosomal as
  select assembly_id, max(chromosomal) as chr from assembly_sequence
  group by assembly_id;

update assembly a inner join tmp_chromosomal tc using (assembly_id)
  set level = if(chr = 1, 'chromosome', 'scaffold')
where level = 'primary_assembly';

drop table tmp_dataset;
drop table tmp_attribute;
drop table tmp_assembly_sequence;
drop temporary table tmp_chromosomal;
