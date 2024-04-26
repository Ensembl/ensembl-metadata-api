#### DB updates to reset datasets
# DELETE non root datasets
delete dataset
from dataset where dataset_type_id > 7;
# Reinsert new dataset_types

select * from dataset where dataset_type_id > 7;

delete from dataset_type where dataset_type_id > 7;
INSERT INTO dataset_type (dataset_type_id, name, label, topic, description, details_uri, parent_id, depends_on, filter_on) VALUES (8, 'genebuild_compute', 'External References', 'production_process', 'Xref genome annotation for Genebuild', null, 2, null, null);
INSERT INTO dataset_type (dataset_type_id, name, label, topic, description, details_uri, parent_id, depends_on, filter_on) VALUES (9, 'genebuild_files', 'Files dumps', 'production_process', 'File Dumps, either internal or for public consumption', null, 2, '8', null);
INSERT INTO dataset_type (dataset_type_id, name, label, topic, description, details_uri, parent_id, depends_on, filter_on) VALUES (11, 'genebuild_web', 'Web Geneset content', 'production_process', 'Web Geneset related content', null, 2, null, null);
INSERT INTO dataset_type (dataset_type_id, name, label, topic, description, details_uri, parent_id, depends_on, filter_on) VALUES (12, 'genebuild_prep', 'Genebuild preparation', 'production_preparation', 'Web Content for Geneset publication', null, 2, '8,9,11,12', null);
INSERT INTO dataset_type (dataset_type_id, name, label, topic, description, details_uri, parent_id, depends_on, filter_on) VALUES (13, 'xrefs', 'External References', 'production_process', 'External annotations linking', null, 8, null, null);
INSERT INTO dataset_type (dataset_type_id, name, label, topic, description, details_uri, parent_id, depends_on, filter_on) VALUES (14, 'protein_features', 'Protein Features annotations', 'production_process', 'Proteins annotation', null, 8, '13', null);
INSERT INTO dataset_type (dataset_type_id, name, label, topic, description, details_uri, parent_id, depends_on, filter_on) VALUES (15, 'alpha_fold', 'AlphaFold computation', 'production_process', 'Compute Protein structure with Alphafold', null, 8, '13', null);
INSERT INTO dataset_type (dataset_type_id, name, label, topic, description, details_uri, parent_id, depends_on, filter_on) VALUES (16, 'blast', 'Blast tools', 'production_process', 'Blast Indexes files', null, 9, '8', null);
INSERT INTO dataset_type (dataset_type_id, name, label, topic, description, details_uri, parent_id, depends_on, filter_on) VALUES (17, 'ftp_dumps', 'Public FTP files', 'production_process', 'Public FTP flat files geneset dumps', null, 9, '8', null);
INSERT INTO dataset_type (dataset_type_id, name, label, topic, description, details_uri, parent_id, depends_on, filter_on) VALUES (18, 'thoas_dumps', 'Thoas load flat files', 'production_process', 'Dump flat file to load onto THOAS', null, 11, null, null);
INSERT INTO dataset_type (dataset_type_id, name, label, topic, description, details_uri, parent_id, depends_on, filter_on) VALUES (19, 'thoas_load', 'Thoas MongoDB Load', 'production_preparation', 'Load dumped files onto THOAS', null, 12, '18,23', null);
INSERT INTO dataset_type (dataset_type_id, name, label, topic, description, details_uri, parent_id, depends_on, filter_on) VALUES (20, 'genebuild_browser_files', 'Genome Browser BB Geneset files', 'production_process', 'Production BigBed for Genome Browser', null, 11, null, null);
INSERT INTO dataset_type (dataset_type_id, name, label, topic, description, details_uri, parent_id, depends_on, filter_on) VALUES (21, 'genebuild_track', 'Geneset Tracks API', 'production_preparation', 'Register Geneset Track API BigBed files', null, 12, '20', null);
INSERT INTO dataset_type (dataset_type_id, name, label, topic, description, details_uri, parent_id, depends_on, filter_on) VALUES (23, 'checksums', 'Sequences Checksums', 'production_process', 'Compute core sequence checksums and update metadata', null, 11, null, null);
INSERT INTO dataset_type (dataset_type_id, name, label, topic, description, details_uri, parent_id, depends_on, filter_on) VALUES (24, 'refget_load', 'Refget Loading', 'production_preparation', 'Load sequences and their checksum onto Refget app', null, 12, '22', null);
INSERT INTO dataset_type (dataset_type_id, name, label, topic, description, details_uri, parent_id, depends_on, filter_on) VALUES (25, 'homology_compute', 'Homology annotation', 'production_process', 'Compute Genome homology analysis', null, 6, null, null);
INSERT INTO dataset_type (dataset_type_id, name, label, topic, description, details_uri, parent_id, depends_on, filter_on) VALUES (26, 'homology_load', 'Homology dataload', 'production_preparation', 'Load homology data onto Compara Service (MongoDB)', null, 6, '25', null);
INSERT INTO dataset_type (dataset_type_id, name, label, topic, description, details_uri, parent_id, depends_on, filter_on) VALUES (27, 'homology_ftp', 'Homology tsv public files', 'production_preparation', 'Dump and sync public TSV homology files', null, 6, '25', null);
INSERT INTO dataset_type (dataset_type_id, name, label, topic, description, details_uri, parent_id, depends_on, filter_on) VALUES (28, 'vep', 'VEP filesets', 'variation_annotation', 'VCF annotation file for geneset', null, null, null, null);
INSERT INTO dataset_type (dataset_type_id, name, label, topic, description, details_uri, parent_id, depends_on, filter_on) VALUES (29, 'variation_ftp', 'Public Variation files (vcf)', 'production_preparation', 'VCF files for public FTP', null, 3, null, null);
INSERT INTO dataset_type (dataset_type_id, name, label, topic, description, details_uri, parent_id, depends_on, filter_on) VALUES (31, 'variation_browser_files', 'Variation Browser files', 'production_process', 'Variation track browser file', null, 3, null, null);
INSERT INTO dataset_type (dataset_type_id, name, label, topic, description, details_uri, parent_id, depends_on, filter_on) VALUES (32, 'variation_track', 'Variation Track', 'production_preparation', 'Variation Track API', null, 3, '31', null);
INSERT INTO dataset_type (dataset_type_id, name, label, topic, description, details_uri, parent_id, depends_on, filter_on) VALUES (33, 'regulation_browser_files', 'Regulation Browser files', 'production_process', 'Regulation track browser file', null, 7, null, null);
INSERT INTO dataset_type (dataset_type_id, name, label, topic, description, details_uri, parent_id, depends_on, filter_on) VALUES (34, 'regulation_track', 'Regulation Track', 'production_preparation', 'Regulation Track API', null, 7, '33', null);
INSERT INTO dataset_type (dataset_type_id, name, label, topic, description, details_uri, parent_id, depends_on, filter_on) VALUES (35, 'regulation_ftp', 'Regulation Public files', 'production_preparation', 'Regulation public files', null, 7, '33', null);
INSERT INTO dataset_type (dataset_type_id, name, label, topic, description, details_uri, parent_id, depends_on, filter_on) VALUES (37, 'web_genesearch', 'GeneSearch Index', 'production_publication', 'Gene search indexes provisioning', null, null, '36', null);
INSERT INTO dataset_type (dataset_type_id, name, label, topic, description, details_uri, parent_id, depends_on, filter_on) VALUES (38, 'web_genomediscovery', 'Genome Search indexes loading to EBI search', 'production_publication', 'Load dumped data from genebuild_web onto EBI Search engine (SpecieSelector)', null, null, '37', null);

# DELETE Bombus_terristris unlinked dataset
delete
from dataset
where dataset_uuid = '428d2741-2699-48a4-8830-4f808994f512';

# RELEASE datasets and genomes for 110.2
update dataset
    join genome_dataset using (dataset_id)
set status = 'Processed' # Change to 'Released' when releasing
where release_id = (select release_id from ensembl_release where version = 110.2);

# RESET dataset which are not attached to a release
# Mark all Unreleased assembly dataset as 'Processed'
update dataset
    join genome_dataset using (dataset_id)
set status = 'Processed'
where release_id is null
  and dataset_type_id = (select dataset_type_id
                         from dataset_type
                         where dataset.name = 'assembly'
                           and dataset_type.dataset_type_id = dataset.dataset_type_id);

# Mark all others as 'Submitted'
update dataset
    join genome_dataset using (dataset_id)
set status = 'Submitted'
where release_id is null
  and dataset_type_id = (select dataset_type_id
                         from dataset_type
                         where dataset.name <> 'assembly'
                           and dataset_type.dataset_type_id = dataset.dataset_type_id);

# INSERT compara_homologies from 241 to 110.2
BEGIN;
INSERT INTO dataset (dataset_uuid, name, version, created, label, dataset_source_id, dataset_type_id, status)
SELECT UUID(),
       name,
       '2.0',
       NOW(),
       dataset.label,
       dataset.dataset_source_id,
       6,
       'Released'
from dataset
         join genome_dataset using (dataset_id)
where dataset_type_id = 6
  and genome_dataset.release_id = 1;

# INSERT compara_homologies as supplementary dataset for the 241 in next release.
INSERT INTO genome_dataset (is_current, dataset_id, genome_id, release_id)
select 0,
       dataset_id,
       (select genome.genome_id
        from genome
                 join genome_dataset gd using (genome_id)
                 join dataset d1 using (dataset_id)
        where gd.genome_id in (SELECT genome.genome_id
                               from genome
                                        join genome_dataset using (genome_id)
                                        join dataset using (dataset_id)
                               where dataset_type_id = 6
                                 and genome_dataset.release_id = 1)
          and d1.dataset_source_id = d.dataset_source_id) as genebuild_genome_id,
       2
from dataset d
where version = '2.0';

# UPDATE homologies for 110.1 is_current to 0
UPDATE genome_dataset
    join dataset using (dataset_id)
set is_current = 0
where dataset_type_id = 6
  and release_id = 1;

# UPDATE homologies for 110.2 is_current to 1
UPDATE genome_dataset
    join dataset using (dataset_id)
set is_current = 1
where dataset_type_id = 6
  and release_id = 2;

# UPDATE 110.2 as released
update ensembl_release
set is_current = 0
where version = 110.1;
update ensembl_release
set status     = 'Released',
    is_current = 1
where version = 110.2;
COMMIT;



