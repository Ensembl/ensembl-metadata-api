-- data_files
select
  database() as dbname,
  m1.meta_value as ensembl_release,
  m2.meta_value as ensembl_name,
  'rnaseq_alignments' as dataset_type,
  concat('datafiles_', lower(data_file.file_type)) as dataset_name,
  'name' as type,
  'databases' as name,
  'RNA-seq samples' as label,
  replace(display_label, ' RNA-seq BWA alignments', '') as value
from
  meta m1,
  data_file inner join
  coord_system using (coord_system_id) inner join
  meta m2 on coord_system.species_id = m2.species_id inner join
  analysis using (analysis_id) inner join
  analysis_description using (analysis_id)
where
  m1.meta_key = 'schema_version' and
  m2.meta_key = 'species.production_name'
;

-- gene attributes
select
  database() as dbname,
  m1.meta_value as ensembl_release,
  m2.meta_value as ensembl_name,
  'geneset' as dataset_type,
  'gene_rnaseq' as dataset_name,
  'count' as type,
  'gene_rnaseq' as name,
  'RNA-seq genes' as label,
  count(*) as value
from
  meta m1,
  gene inner join
  seq_region using (seq_region_id) inner join
  coord_system using (coord_system_id) inner join
  meta m2 on coord_system.species_id = m2.species_id inner join
  analysis using (analysis_id) inner join
  analysis_description using (analysis_id)
where
  m1.meta_key = 'schema_version' and
  m2.meta_key = 'species.production_name' and
  logic_name like '%rnaseq%'
group by m1.meta_value, m2.meta_value
;

select
  database() as dbname,
  m1.meta_value as ensembl_release,
  m2.meta_value as ensembl_name,
  'geneset' as dataset_type,
  logic_name as dataset_name,
  'count' as type,
  logic_name as name,
  display_label as label,
  count(*) as value
from
  meta m1,
  gene inner join
  seq_region using (seq_region_id) inner join
  coord_system using (coord_system_id) inner join
  meta m2 on coord_system.species_id = m2.species_id inner join
  analysis using (analysis_id) inner join
  analysis_description using (analysis_id)
where
  m1.meta_key = 'schema_version' and
  m2.meta_key = 'species.production_name' and
  logic_name not like '%rnaseq%'
group by m1.meta_value, m2.meta_value, logic_name
;
