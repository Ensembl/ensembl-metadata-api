-- daf datasets
select
  'dna_alignments' as type,
  database() as dbname,
  m1.meta_value as ensembl_release,
  m2.meta_value as biosample_id,
  'alignment_isoseq' as name,
  'Iso-seq alignments' as label,
  max(date(analysis.created)) as version
from
  meta m1,
  dna_align_feature inner join
  seq_region using (seq_region_id) inner join
  coord_system using (coord_system_id) inner join
  meta m2 on coord_system.species_id = m2.species_id inner join
  analysis using (analysis_id) inner join
  analysis_description using (analysis_id)
where
  m1.meta_key = 'schema_version' and
  m2.meta_key = 'species.production_name' and
  analysis_description.displayable = 1 and
  analysis.logic_name like '%isoseq%'
group by m1.meta_value, m2.meta_value
;

select
  'dna_alignments' as type,
  database() as dbname,
  m1.meta_value as ensembl_release,
  m2.meta_value as biosample_id,
  analysis.logic_name as name,
  analysis_description.display_label as label,
  ifnull(analysis.db_version, date(analysis.created)) as version
from
  meta m1,
  dna_align_feature inner join
  seq_region using (seq_region_id) inner join
  coord_system using (coord_system_id) inner join
  meta m2 on coord_system.species_id = m2.species_id inner join
  analysis using (analysis_id) inner join
  analysis_description using (analysis_id)
where
  m1.meta_key = 'schema_version' and
  m2.meta_key = 'species.production_name' and
  analysis_description.displayable = 1 and
  analysis.logic_name not like '%isoseq%'
group by m1.meta_value, m2.meta_value, analysis.logic_name, display_label, ifnull(analysis.db_version, date(analysis.created))
;

-- paf datasets
select
  'protein_alignments' as type,
  database() as dbname,
  m1.meta_value as ensembl_release,
  m2.meta_value as biosample_id,
  analysis.logic_name as name,
  analysis_description.display_label as label,
  ifnull(analysis.db_version, date(analysis.created)) as version
from
  meta m1,
  protein_align_feature inner join
  seq_region using (seq_region_id) inner join
  coord_system using (coord_system_id) inner join
  meta m2 on coord_system.species_id = m2.species_id inner join
  analysis using (analysis_id) inner join
  analysis_description using (analysis_id)
where
  m1.meta_key = 'schema_version' and
  m2.meta_key = 'species.production_name' and
  analysis_description.displayable = 1
group by m1.meta_value, m2.meta_value, analysis.logic_name, display_label, ifnull(analysis.db_version, date(analysis.created))
;

-- gene datasets
select
  'geneset' as type,
  database() as dbname,
  m1.meta_value as ensembl_release,
  m2.meta_value as biosample_id,
  'gene_isoseq' as name,
  'Iso-seq gene models' as label,
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
  analysis_description.displayable = 1 and
  analysis.logic_name like '%isoseq%'
group by m1.meta_value, m2.meta_value
;

select
  'geneset' as type,
  database() as dbname,
  m1.meta_value as ensembl_release,
  m2.meta_value as biosample_id,
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
  analysis_description.displayable = 1 and
  analysis.logic_name not like '%isoseq%'
group by m1.meta_value, m2.meta_value, analysis.logic_name, analysis_description.display_label, ifnull(analysis.db_version, date(analysis.created))
;
