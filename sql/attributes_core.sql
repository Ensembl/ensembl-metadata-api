-- Assembly attributes
select
  database() as dbname,
  m1.meta_value as ensembl_release,
  m2.meta_value as biosample_id,
  'assembly' as dataset_type,
  'assembly' as dataset_name,
  'length_bp' as type,
  'ungapped_genome' as name,
  'Base pairs' as label,
  genome_statistics.value as value
from
  meta m1,
  genome_statistics inner join
  meta m2 on genome_statistics.species_id = m2.species_id
where
  m1.meta_key = 'schema_version' and
  m2.meta_key = 'species.production_name' and
  genome_statistics.statistic = 'ref_length'
;

-- Protein feature attributes
select
  database() as dbname,
  m1.meta_value as ensembl_release,
  m2.meta_value as biosample_id,
  'protein_features' as dataset_type,
  lower(analysis.program) as dataset_name,
  'count' as type,
  analysis.logic_name as name,
  analysis_description.display_label as label,
  count(*) as value
from
  meta m1,
  protein_feature inner join
  translation using (translation_id) inner join
  transcript using (transcript_id) inner join
  seq_region using (seq_region_id) inner join
  coord_system using (coord_system_id) inner join
  meta m2 on coord_system.species_id = m2.species_id inner join
  analysis on protein_feature.analysis_id = analysis.analysis_id inner join
  analysis_description on analysis.analysis_id = analysis_description.analysis_id
where
  m1.meta_key = 'schema_version' and
  m2.meta_key = 'species.production_name'
group by m1.meta_value, m2.meta_value, analysis.program, analysis.logic_name
;

-- GO attributes
select
  database() as dbname,
  m1.meta_value as ensembl_release,
  m2.meta_value as biosample_id,
  'go_terms' as dataset_type,
  analysis.logic_name as dataset_name,
  'count' as type,
  analysis.logic_name as name,
  analysis_description.display_label as label,
  count(*) as value
from
  meta m1,
  object_xref inner join
  transcript on ensembl_id = transcript_id inner join
  seq_region using (seq_region_id) inner join
  coord_system using (coord_system_id) inner join
  meta m2 on coord_system.species_id = m2.species_id inner join
  analysis on object_xref.analysis_id = analysis.analysis_id inner join
  analysis_description on analysis.analysis_id = analysis_description.analysis_id
where
  m1.meta_key = 'schema_version' and
  m2.meta_key = 'species.production_name' and
  analysis.logic_name in ('goa_import', 'interpro2go') and
  object_xref.ensembl_object_type = 'Transcript'
group by m1.meta_value, m2.meta_value, analysis.logic_name
;

-- Checksum-based xref attributes
select
  cast(database() as char(255)) as dbname,
  m1.meta_value as ensembl_release,
  m2.meta_value as biosample_id,
  'checksum_xrefs' as dataset_type,
  analysis.logic_name as dataset_name,
  'count' as type,
  concat('transcript_', lower(external_db.db_name), '_xref') as name,
  analysis_description.display_label as label,
  count(distinct transcript.stable_id) as value
from
  meta m1,
  object_xref inner join
  xref using (xref_id) inner join
  external_db using (external_db_id) inner join
  transcript on ensembl_id = transcript_id inner join
  seq_region using (seq_region_id) inner join
  coord_system using (coord_system_id) inner join
  meta m2 on coord_system.species_id = m2.species_id inner join
  analysis on object_xref.analysis_id = analysis.analysis_id inner join
  analysis_description on analysis.analysis_id = analysis_description.analysis_id
where
  m1.meta_key = 'schema_version' and
  m2.meta_key = 'species.production_name' and
  analysis.logic_name in ('rnacentral_checksum', 'uniparc_checksum', 'uniprot_checksum') and
  object_xref.ensembl_object_type = 'Transcript'
group by m1.meta_value, m2.meta_value, external_db.db_name

union

select
  database() as dbname,
  m1.meta_value as ensembl_release,
  m2.meta_value as biosample_id,
  'checksum_xrefs' as dataset_type,
  analysis.logic_name as dataset_name,
  'count' as type,
  concat('transcript_', lower(external_db.db_name), '_xref') as name,
  analysis_description.display_label as label,
  count(distinct transcript.stable_id) as value
from
  meta m1,
  object_xref inner join
  xref using (xref_id) inner join
  external_db using (external_db_id) inner join
  translation on ensembl_id = translation_id inner join
  transcript using (transcript_id) inner join
  seq_region using (seq_region_id) inner join
  coord_system using (coord_system_id) inner join
  meta m2 on coord_system.species_id = m2.species_id inner join
  analysis on object_xref.analysis_id = analysis.analysis_id inner join
  analysis_description on analysis.analysis_id = analysis_description.analysis_id
where
  m1.meta_key = 'schema_version' and
  m2.meta_key = 'species.production_name' and
  analysis.logic_name in ('rnacentral_checksum', 'uniparc_checksum', 'uniprot_checksum') and
  object_xref.ensembl_object_type = 'Translation'
group by m1.meta_value, m2.meta_value, external_db.db_name

union

select
  database() as dbname,
  m1.meta_value as ensembl_release,
  m2.meta_value as biosample_id,
  'checksum_xrefs' as dataset_type,
  concat(lower(external_db.db_name), '_checksum') as dataset_name,
  'count' as type,
  concat('transcript_', lower(external_db.db_name), '_xref') as name,
  analysis_description.display_label as label,
  count(distinct transcript.stable_id) as value
from
  meta m1,
  object_xref inner join
  xref using (xref_id) inner join
  external_db using (external_db_id) inner join
  transcript on ensembl_id = transcript_id inner join
  seq_region using (seq_region_id) inner join
  coord_system using (coord_system_id) inner join
  meta m2 on coord_system.species_id = m2.species_id inner join
  analysis on object_xref.analysis_id = analysis.analysis_id inner join
  analysis_description on analysis.analysis_id = analysis_description.analysis_id
where
  m1.meta_key = 'schema_version' and
  m2.meta_key = 'species.production_name' and
  analysis.logic_name in ('xref_checksum') and
  object_xref.ensembl_object_type = 'Transcript'
group by m1.meta_value, m2.meta_value, external_db.db_name

union

select
  database() as dbname,
  m1.meta_value as ensembl_release,
  m2.meta_value as biosample_id,
  'checksum_xrefs' as dataset_type,
  concat(lower(external_db.db_name), '_checksum') as dataset_name,
  'count' as type,
  concat('transcript_', lower(external_db.db_name), '_xref') as name,
  analysis_description.display_label as label,
  count(distinct transcript.stable_id) as value
from
  meta m1,
  object_xref inner join
  xref using (xref_id) inner join
  external_db using (external_db_id) inner join
  translation on ensembl_id = translation_id inner join
  transcript using (transcript_id) inner join
  seq_region using (seq_region_id) inner join
  coord_system using (coord_system_id) inner join
  meta m2 on coord_system.species_id = m2.species_id inner join
  analysis on object_xref.analysis_id = analysis.analysis_id inner join
  analysis_description on analysis.analysis_id = analysis_description.analysis_id
where
  m1.meta_key = 'schema_version' and
  m2.meta_key = 'species.production_name' and
  analysis.logic_name in ('xref_checksum') and
  object_xref.ensembl_object_type = 'Translation'
group by m1.meta_value, m2.meta_value, external_db.db_name
;

-- Repeat feature attributes
select
  database() as dbname,
  m1.meta_value as ensembl_release,
  m2.meta_value as biosample_id,
  'repeat_features' as dataset_type,
  analysis.logic_name as dataset_name,
  'count' as type,
  analysis.logic_name as name,
  analysis_description.display_label as label,
  count(*) as value
from
  meta m1,
  repeat_feature inner join
  seq_region using (seq_region_id) inner join
  coord_system using (coord_system_id) inner join
  meta m2 on coord_system.species_id = m2.species_id inner join
  analysis using (analysis_id) inner join
  analysis_description using (analysis_id)
where
  m1.meta_key = 'schema_version' and
  m2.meta_key = 'species.production_name' and
  analysis_description.displayable = 1
group by m1.meta_value, m2.meta_value, analysis.logic_name
;
-- Could do repeat coverage, per logic_name as a repeat feature stat,
-- then across all sources as an assembly stat. But v. difficult to do
-- in SQL, export features to bed file and use bed tools, to account
-- for overlaps.

-- daf attributes
select
  database() as dbname,
  m1.meta_value as ensembl_release,
  m2.meta_value as biosample_id,
  'dna_alignments' as dataset_type,
  analysis.logic_name as dataset_name,
  'count' as type,
  analysis.logic_name as name,
  analysis_description.display_label as label,
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
  analysis_description.displayable = 1
group by m1.meta_value, m2.meta_value, analysis.logic_name
;
