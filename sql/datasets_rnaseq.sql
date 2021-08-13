-- data_files
select
  'rnaseq_alignments' as type,
  database() as dbname,
  m1.meta_value as ensembl_release,
  m2.meta_value as ensembl_name,
  concat('datafiles_', lower(data_file.file_type)) as name,
  concat(replace(data_file.file_type, 'BAMCOV', 'BAM coverage'), ' files') as label,
  max(date(analysis.created)) as version
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
group by m1.meta_value, m2.meta_value, data_file.file_type
;

-- dafs are essentially the same as data_files so don't need those.
-- pafs are not displayed as separate tracks, they exist to support
-- other features, so we don't count them as a dataset in their own right.

-- gene datasets
select
  'geneset' as type,
  database() as dbname,
  m1.meta_value as ensembl_release,
  m2.meta_value as ensembl_name,
  'gene_rnaseq' as name,
  'RNA-seq gene models' as label,
  max(date(analysis.created)) as version
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
  analysis.logic_name like '%rnaseq%'
group by m1.meta_value, m2.meta_value
;

select
  'geneset' as type,
  database() as dbname,
  m1.meta_value as ensembl_release,
  m2.meta_value as ensembl_name,
  analysis.logic_name as name,
  analysis_description.display_label as label,
  ifnull(analysis.db_version, date(analysis.created)) as version
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
  analysis.logic_name not like '%rnaseq%'
group by m1.meta_value, m2.meta_value, analysis.logic_name, analysis_description.display_label, ifnull(analysis.db_version, date(analysis.created))
;
