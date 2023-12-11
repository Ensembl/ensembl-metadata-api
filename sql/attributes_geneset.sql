select
  database() as dbname,
  m1.meta_value as ensembl_release,
  m2.meta_value as biosample_id,
  'geneset' as dataset_type,
  'gene_core' as dataset_name,
  'count' as type,
  concat('gene_', biotype_group) as name,
  '' as label,
  count(*) as value
from
  meta m1,
  gene inner join
  seq_region using (seq_region_id) inner join
  coord_system using (coord_system_id) inner join
  meta m2 on coord_system.species_id = m2.species_id inner join
  biotype on gene.biotype = biotype.name
where
  m1.meta_key = 'schema_version' and
  m2.meta_key = 'species.production_name' and
  object_type = 'gene' and
  biotype_group <> 'no_group'
group by m1.meta_value, m2.meta_value, biotype_group
;

select
  database() as dbname,
  m1.meta_value as ensembl_release,
  m2.meta_value as biosample_id,
  'geneset' as dataset_type,
  'gene_core' as dataset_name,
  'length_bp' as type,
  concat('gene_genomic_', biotype_group) as name,
  '' as label,
  sum(CAST(seq_region_end as SIGNED) - CAST(seq_region_start as SIGNED) + 1) as value
from
  meta m1,
  gene inner join
  seq_region using (seq_region_id) inner join
  coord_system using (coord_system_id) inner join
  meta m2 on coord_system.species_id = m2.species_id inner join
  biotype on gene.biotype = biotype.name
where
  m1.meta_key = 'schema_version' and
  m2.meta_key = 'species.production_name' and
  object_type = 'gene' and
  biotype_group <> 'no_group'
group by m1.meta_value, m2.meta_value, biotype_group
;

select
  database() as dbname,
  m1.meta_value as ensembl_release,
  m2.meta_value as biosample_id,
  'geneset' as dataset_type,
  'gene_core' as dataset_name,
  'max_bp' as type,
  concat('gene_genomic_', biotype_group) as name,
  '' as label,
  max(CAST(seq_region_end as SIGNED) - CAST(seq_region_start as SIGNED) + 1) as value
from
  meta m1,
  gene inner join
  seq_region using (seq_region_id) inner join
  coord_system using (coord_system_id) inner join
  meta m2 on coord_system.species_id = m2.species_id inner join
  biotype on gene.biotype = biotype.name
where
  m1.meta_key = 'schema_version' and
  m2.meta_key = 'species.production_name' and
  object_type = 'gene' and
  biotype_group <> 'no_group'
group by m1.meta_value, m2.meta_value, biotype_group
;

select
  database() as dbname,
  m1.meta_value as ensembl_release,
  m2.meta_value as biosample_id,
  'geneset' as dataset_type,
  'gene_core' as dataset_name,
  'min_bp' as type,
  concat('gene_genomic_', biotype_group) as name,
  '' as label,
  min(CAST(seq_region_end as SIGNED) - CAST(seq_region_start as SIGNED) + 1) as value
from
  meta m1,
  gene inner join
  seq_region using (seq_region_id) inner join
  coord_system using (coord_system_id) inner join
  meta m2 on coord_system.species_id = m2.species_id inner join
  biotype on gene.biotype = biotype.name
where
  m1.meta_key = 'schema_version' and
  m2.meta_key = 'species.production_name' and
  object_type = 'gene' and
  biotype_group <> 'no_group'
group by m1.meta_value, m2.meta_value, biotype_group
;

select
  database() as dbname,
  m1.meta_value as ensembl_release,
  m2.meta_value as biosample_id,
  'geneset' as dataset_type,
  'gene_core' as dataset_name,
  'count' as type,
  concat('transcript_', biotype_group) as name,
  '' as label,
  count(*) as value
from
  meta m1,
  transcript inner join
  seq_region using (seq_region_id) inner join
  coord_system using (coord_system_id) inner join
  meta m2 on coord_system.species_id = m2.species_id inner join
  biotype on transcript.biotype = biotype.name
where
  m1.meta_key = 'schema_version' and
  m2.meta_key = 'species.production_name' and
  object_type = 'transcript' and
  biotype_group <> 'no_group'
group by m1.meta_value, m2.meta_value, biotype_group
;

select
  database() as dbname,
  m1.meta_value as ensembl_release,
  m2.meta_value as biosample_id,
  'geneset' as dataset_type,
  'gene_core' as dataset_name,
  'length_bp' as type,
  concat('transcript_genomic_', biotype_group) as name,
  '' as label,
  sum(CAST(seq_region_end as SIGNED) - CAST(seq_region_start as SIGNED) + 1) as value
from
  meta m1,
  transcript inner join
  seq_region using (seq_region_id) inner join
  coord_system using (coord_system_id) inner join
  meta m2 on coord_system.species_id = m2.species_id inner join
  biotype on transcript.biotype = biotype.name
where
  m1.meta_key = 'schema_version' and
  m2.meta_key = 'species.production_name' and
  object_type = 'transcript' and
  biotype_group <> 'no_group'
group by m1.meta_value, m2.meta_value, biotype_group
;

select
  database() as dbname,
  m1.meta_value as ensembl_release,
  m2.meta_value as biosample_id,
  'geneset' as dataset_type,
  'gene_core' as dataset_name,
  'length_bp' as type,
  concat('cdna_', biotype_group) as name,
  '' as label,
  sum(CAST(exon.seq_region_end as signed) - cast(exon.seq_region_start as signed) + 1) as value
from
  meta m1,
  exon inner join
  exon_transcript using (exon_id) inner join
  transcript using (transcript_id) inner join
  seq_region on transcript.seq_region_id = seq_region.seq_region_id inner join
  coord_system using (coord_system_id) inner join
  meta m2 on coord_system.species_id = m2.species_id inner join
  biotype on transcript.biotype = biotype.name
where
  m1.meta_key = 'schema_version' and
  m2.meta_key = 'species.production_name' and
  object_type = 'transcript' and
  biotype_group <> 'no_group'
group by m1.meta_value, m2.meta_value, biotype_group
;

select
  database() as dbname,
  m1.meta_value as ensembl_release,
  m2.meta_value as biosample_id,
  'geneset' as dataset_type,
  'gene_core' as dataset_name,
  'count' as type,
  concat('exon_', biotype_group) as name,
  '' as label,
  count(*) as value
from
  meta m1,
  exon_transcript inner join
  transcript using (transcript_id) inner join
  seq_region using (seq_region_id) inner join
  coord_system using (coord_system_id) inner join
  meta m2 on coord_system.species_id = m2.species_id inner join
  biotype on transcript.biotype = biotype.name
where
  m1.meta_key = 'schema_version' and
  m2.meta_key = 'species.production_name' and
  object_type = 'transcript' and
  biotype_group <> 'no_group'
group by m1.meta_value, m2.meta_value, biotype_group
;

select
  database() as dbname,
  m1.meta_value as ensembl_release,
  m2.meta_value as biosample_id,
  'geneset' as dataset_type,
  'gene_core' as dataset_name,
  'example' as type,
  'gene' as name,
  'Gene' as label,
  m3.meta_value
from
  meta m1,
  meta m2 inner join
  meta m3 using (species_id)
where
  m1.meta_key = 'schema_version' and
  m2.meta_key = 'species.production_name' and
  m3.meta_key = 'databases.gene_param'
;

select
  database() as dbname,
  m1.meta_value as ensembl_release,
  m2.meta_value as biosample_id,
  'geneset' as dataset_type,
  'gene_core' as dataset_name,
  'example' as type,
  'location' as name,
  'Location' as label,
  m3.meta_value
from
  meta m1,
  meta m2 inner join
  meta m3 using (species_id)
where
  m1.meta_key = 'schema_version' and
  m2.meta_key = 'species.production_name' and
  m3.meta_key = 'databases.location_param'
;
