-- No assembly or geneset datasets, yet.

-- Protein feature datasets
select
  'protein_features' as type,
  database() as dbname,
  m1.meta_value as ensembl_release,
  m2.meta_value as biosample_id,
  lower(program) as name,
  program as label,
  max(program_version) as version
from
  meta m1,
  protein_feature inner join
  translation using (translation_id) inner join
  transcript using (transcript_id) inner join
  seq_region using (seq_region_id) inner join
  coord_system using (coord_system_id) inner join
  meta m2 on coord_system.species_id = m2.species_id inner join
  analysis on protein_feature.analysis_id = analysis.analysis_id
where
  m1.meta_key = 'schema_version' and
  m2.meta_key = 'species.production_name'
group by m1.meta_value, m2.meta_value, program
;

-- GO datasets
select
  'go_terms' as type,
  database() as dbname,
  m1.meta_value as ensembl_release,
  m2.meta_value as biosample_id,
  logic_name as name,
  display_label as label,
  ifnull(db_version, date(created)) as version
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
  logic_name in ('goa_import', 'interpro2go') and
  ensembl_object_type = 'Transcript'
group by m1.meta_value, m2.meta_value, logic_name, display_label, ifnull(db_version, date(created))
;

-- Checksum-based xref datasets
select
  'checksum_xrefs' as type,
  cast(database() as char(255)) as dbname,
  m1.meta_value as ensembl_release,
  m2.meta_value as biosample_id,
  logic_name as name,
  display_label as label,
  ifnull(db_version, date(created)) as version
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
  logic_name in ('rnacentral_checksum', 'uniparc_checksum', 'uniprot_checksum') and
  ensembl_object_type = 'Transcript'
group by m1.meta_value, m2.meta_value, logic_name, display_label, ifnull(db_version, date(created))

union

select
  'checksum_xrefs' as type,
  database() as dbname,
  m1.meta_value as ensembl_release,
  m2.meta_value as biosample_id,
  logic_name as name,
  display_label as label,
  ifnull(db_version, date(created)) as version
from
  meta m1,
  object_xref inner join
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
  logic_name in ('rnacentral_checksum', 'uniparc_checksum', 'uniprot_checksum') and
  ensembl_object_type = 'Translation'
group by m1.meta_value, m2.meta_value, logic_name, display_label, ifnull(db_version, date(created))

union

select
  'checksum_xrefs' as type,
  database() as dbname,
  m1.meta_value as ensembl_release,
  m2.meta_value as biosample_id,
  concat(lower(db_name), '_checksum') as name,
  db_display_name as label,
  ifnull(db_release, date(created)) as version
from
  meta m1,
  object_xref inner join
  transcript on ensembl_id = transcript_id inner join
  seq_region using (seq_region_id) inner join
  coord_system using (coord_system_id) inner join
  meta m2 on coord_system.species_id = m2.species_id inner join
  analysis on object_xref.analysis_id = analysis.analysis_id inner join
  xref using (xref_id) inner join
  external_db using (external_db_id)
where
  m1.meta_key = 'schema_version' and
  m2.meta_key = 'species.production_name' and
  logic_name = 'xrefchecksum' and
  ensembl_object_type = 'Transcript'
group by m1.meta_value, m2.meta_value, db_name, db_display_name, ifnull(db_release, date(created))

union

select
  'checksum_xrefs' as type,
  database() as dbname,
  m1.meta_value as ensembl_release,
  m2.meta_value as biosample_id,
  concat(lower(db_name), '_checksum') as name,
  db_display_name as label,
  ifnull(db_release, date(created)) as version
from
  meta m1,
  object_xref inner join
  translation on ensembl_id = translation_id inner join
  transcript using (transcript_id) inner join
  seq_region using (seq_region_id) inner join
  coord_system using (coord_system_id) inner join
  meta m2 on coord_system.species_id = m2.species_id inner join
  analysis on object_xref.analysis_id = analysis.analysis_id inner join
  xref using (xref_id) inner join
  external_db using (external_db_id)
where
  m1.meta_key = 'schema_version' and
  m2.meta_key = 'species.production_name' and
  logic_name = 'xrefchecksum' and
  ensembl_object_type = 'Translation'
group by m1.meta_value, m2.meta_value, db_name, db_display_name, ifnull(db_release, date(created))
;

-- Alignment-based xref datasets
select
  'alignment_xrefs' as type,
  cast(database() as char(255)) as dbname,
  m1.meta_value as ensembl_release,
  m2.meta_value as biosample_id,
  'xref_alignment' as name,
  'Alignment-based cross-references' as label,
  max(date(created)) as version
from
  meta m1,
  identity_xref inner join
  object_xref using (object_xref_id) inner join
  gene on ensembl_id = gene_id inner join
  seq_region using (seq_region_id) inner join
  coord_system using (coord_system_id) inner join
  meta m2 on coord_system.species_id = m2.species_id inner join
  analysis on object_xref.analysis_id = analysis.analysis_id
where
  m1.meta_key = 'schema_version' and
  m2.meta_key = 'species.production_name' and
  ensembl_object_type = 'Gene'
group by m1.meta_value, m2.meta_value

union

select
  'alignment_xrefs' as type,
  database() as dbname,
  m1.meta_value as ensembl_release,
  m2.meta_value as biosample_id,
  'xref_alignment' as name,
  'Alignment-based cross-references' as label,
  max(date(created)) as version
from
  meta m1,
  identity_xref inner join
  object_xref using (object_xref_id) inner join
  transcript on ensembl_id = transcript_id inner join
  seq_region using (seq_region_id) inner join
  coord_system using (coord_system_id) inner join
  meta m2 on coord_system.species_id = m2.species_id inner join
  analysis on object_xref.analysis_id = analysis.analysis_id
where
  m1.meta_key = 'schema_version' and
  m2.meta_key = 'species.production_name' and
  ensembl_object_type = 'Transcript'
group by m1.meta_value, m2.meta_value

union

select
  'alignment_xrefs' as type,
  database() as dbname,
  m1.meta_value as ensembl_release,
  m2.meta_value as biosample_id,
  'xref_alignment' as name,
  'Alignment-based cross-references' as label,
  max(date(created)) as version
from
  meta m1,
  identity_xref inner join
  object_xref using (object_xref_id) inner join
  translation on ensembl_id = translation_id inner join
  transcript using (transcript_id) inner join
  seq_region using (seq_region_id) inner join
  coord_system using (coord_system_id) inner join
  meta m2 on coord_system.species_id = m2.species_id inner join
  analysis on object_xref.analysis_id = analysis.analysis_id
where
  m1.meta_key = 'schema_version' and
  m2.meta_key = 'species.production_name' and
  ensembl_object_type = 'Translation'
group by m1.meta_value, m2.meta_value
;

-- Dependent xref datasets
select
  'dependent_xrefs' as type,
  cast(database() as char(255)) as dbname,
  m1.meta_value as ensembl_release,
  m2.meta_value as biosample_id,
  'xref_dependent' as name,
  'Dependent cross-references' as label,
  max(date(created)) as version
from
  meta m1,
  dependent_xref inner join
  object_xref using (object_xref_id) inner join
  gene on ensembl_id = gene_id inner join
  seq_region using (seq_region_id) inner join
  coord_system using (coord_system_id) inner join
  meta m2 on coord_system.species_id = m2.species_id inner join
  analysis on object_xref.analysis_id = analysis.analysis_id
where
  m1.meta_key = 'schema_version' and
  m2.meta_key = 'species.production_name' and
  logic_name not in ('goa_import', 'interpro2go', 'uniprot_checksum') and
  ensembl_object_type = 'Gene'
group by m1.meta_value, m2.meta_value

union

select
  'dependent_xrefs' as type,
  database() as dbname,
  m1.meta_value as ensembl_release,
  m2.meta_value as biosample_id,
  'xref_dependent' as name,
  'Dependent cross-references' as label,
  max(date(created)) as version
from
  meta m1,
  dependent_xref inner join
  object_xref using (object_xref_id) inner join
  transcript on ensembl_id = transcript_id inner join
  seq_region using (seq_region_id) inner join
  coord_system using (coord_system_id) inner join
  meta m2 on coord_system.species_id = m2.species_id inner join
  analysis on object_xref.analysis_id = analysis.analysis_id
where
  m1.meta_key = 'schema_version' and
  m2.meta_key = 'species.production_name' and
  logic_name not in ('goa_import', 'interpro2go', 'uniprot_checksum') and
  ensembl_object_type = 'Transcript'
group by m1.meta_value, m2.meta_value

union

select
  'dependent_xrefs' as type,
  database() as dbname,
  m1.meta_value as ensembl_release,
  m2.meta_value as biosample_id,
  'xref_dependent' as name,
  'Dependent cross-references' as label,
  max(date(created)) as version
from
  meta m1,
  dependent_xref inner join
  object_xref using (object_xref_id) inner join
  translation on ensembl_id = translation_id inner join
  transcript using (transcript_id) inner join
  seq_region using (seq_region_id) inner join
  coord_system using (coord_system_id) inner join
  meta m2 on coord_system.species_id = m2.species_id inner join
  analysis on object_xref.analysis_id = analysis.analysis_id
where
  m1.meta_key = 'schema_version' and
  m2.meta_key = 'species.production_name' and
  logic_name not in ('goa_import', 'interpro2go', 'uniprot_checksum') and
  ensembl_object_type = 'Translation'
group by m1.meta_value, m2.meta_value
;

-- Direct xref datasets
select
  'direct_xrefs' as type,
  cast(database() as char(255)) as dbname,
  m1.meta_value as ensembl_release,
  m2.meta_value as biosample_id,
  'xref_direct' as name,
  'Direct cross-references' as label,
  max(date(created)) as version
from
  meta m1,
  xref inner join
  object_xref using (xref_id) inner join
  gene on ensembl_id = gene_id inner join
  seq_region using (seq_region_id) inner join
  coord_system using (coord_system_id) inner join
  meta m2 on coord_system.species_id = m2.species_id inner join
  analysis on object_xref.analysis_id = analysis.analysis_id
where
  m1.meta_key = 'schema_version' and
  m2.meta_key = 'species.production_name' and
  logic_name not in ('goa_import', 'interpro2go') and
  ensembl_object_type = 'Gene' and
  info_type = 'DIRECT'
group by m1.meta_value, m2.meta_value

union

select
  'direct_xrefs' as type,
  database() as dbname,
  m1.meta_value as ensembl_release,
  m2.meta_value as biosample_id,
  'xref_direct' as name,
  'Direct cross-references' as label,
  max(date(created)) as version
from
  meta m1,
  xref inner join
  object_xref using (xref_id) inner join
  transcript on ensembl_id = transcript_id inner join
  seq_region using (seq_region_id) inner join
  coord_system using (coord_system_id) inner join
  meta m2 on coord_system.species_id = m2.species_id inner join
  analysis on object_xref.analysis_id = analysis.analysis_id
where
  m1.meta_key = 'schema_version' and
  m2.meta_key = 'species.production_name' and
  logic_name not in ('goa_import', 'interpro2go') and
  ensembl_object_type = 'Transcript' and
  info_type = 'DIRECT'
group by m1.meta_value, m2.meta_value

union

select
  'direct_xrefs' as type,
  database() as dbname,
  m1.meta_value as ensembl_release,
  m2.meta_value as biosample_id,
  'xref_direct' as name,
  'Direct cross-references' as label,
  max(date(created)) as version
from
  meta m1,
  xref inner join
  object_xref using (xref_id) inner join
  translation on ensembl_id = translation_id inner join
  transcript using (transcript_id) inner join
  seq_region using (seq_region_id) inner join
  coord_system using (coord_system_id) inner join
  meta m2 on coord_system.species_id = m2.species_id inner join
  analysis on object_xref.analysis_id = analysis.analysis_id
where
  m1.meta_key = 'schema_version' and
  m2.meta_key = 'species.production_name' and
  logic_name not in ('goa_import', 'interpro2go') and
  ensembl_object_type = 'Translation' and
  info_type = 'DIRECT'
group by m1.meta_value, m2.meta_value
;

-- Repeat feature datasets
select
  'repeat_features' as type,
  database() as dbname,
  m1.meta_value as ensembl_release,
  m2.meta_value as biosample_id,
  logic_name as name,
  display_label as label,
  ifnull(program_version, date(created)) as version
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
  displayable = 1
group by m1.meta_value, m2.meta_value, logic_name, display_label, ifnull(program_version, date(created))
;

-- daf datasets
select
  'dna_alignments' as type,
  database() as dbname,
  m1.meta_value as ensembl_release,
  m2.meta_value as biosample_id,
  logic_name as name,
  display_label as label,
  ifnull(db_version, date(created)) as version
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
  displayable = 1
group by m1.meta_value, m2.meta_value, logic_name, display_label, ifnull(db_version, date(created))
;

-- pafs are not displayed as separate tracks, they exist to support
-- other features, so we don't count them as a dataset in their own right.
