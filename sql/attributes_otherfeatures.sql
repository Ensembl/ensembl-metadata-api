-- daf attributes
select
  database() as dbname,
  m1.meta_value as ensembl_release,
  m2.meta_value as biosample_id,
  'dna_alignments' as dataset_type,
  'alignment_isoseq' as dataset_name,
  'count' as type,
  logic_name as name,
  display_label as label,
  count(*) as value
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
  displayable = 1 and
  analysis.logic_name like '%isoseq%'
group by m1.meta_value, m2.meta_value, logic_name, display_label
;

select
  database() as dbname,
  m1.meta_value as ensembl_release,
  m2.meta_value as biosample_id,
  'dna_alignments' as dataset_type,
  logic_name as dataset_name,
  'count' as type,
  logic_name as name,
  display_label as label,
  count(*) as value
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
  displayable = 1 and
  analysis.logic_name not like '%isoseq%'
group by m1.meta_value, m2.meta_value, logic_name, display_label
;

-- paf attributes
select
  database() as dbname,
  m1.meta_value as ensembl_release,
  m2.meta_value as biosample_id,
  'protein_alignments' as dataset_type,
  logic_name as dataset_name,
  'count' as type,
  logic_name as name,
  display_label as label,
  count(*) as value
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
  displayable = 1
group by m1.meta_value, m2.meta_value, logic_name
;

-- gene attributes
select
  database() as dbname,
  m1.meta_value as ensembl_release,
  m2.meta_value as biosample_id,
  'geneset' as dataset_type,
  'gene_isoseq' as dataset_name,
  'count' as type,
  'gene_isoseq' as name,
  'Iso-seq genes' as label,
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
  logic_name like '%isoseq%'
group by m1.meta_value, m2.meta_value
;

select
  database() as dbname,
  m1.meta_value as ensembl_release,
  m2.meta_value as biosample_id,
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
  logic_name not like '%isoseq%'
group by m1.meta_value, m2.meta_value, logic_name
;
