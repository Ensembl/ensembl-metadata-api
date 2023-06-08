-- import to SQLite by running: sqlite3.exe db.sqlite3 -init sqlite.sql
-- import to SQLite by running: sqlite3.exe db.sqlite3 -init sqlite.sql

PRAGMA journal_mode = MEMORY;
PRAGMA synchronous = OFF;
PRAGMA foreign_keys = OFF;
PRAGMA ignore_check_constraints = OFF;
PRAGMA auto_vacuum = NONE;
PRAGMA secure_delete = OFF;
BEGIN TRANSACTION;

create table assembly
(
    assembly_id      int
        primary key,
    ucsc_name        TEXT     null,
    accession        TEXT     not null,
    level            TEXT     not null,
    name             TEXT     not null,
    accession_body   TEXT     null,
    assembly_default TEXT     null,
    tol_id           TEXT     null,
    created          datetime null,
    ensembl_name     TEXT     null,
    constraint accession unique (accession),
    constraint assembly_ensembl_name_uindex unique (ensembl_name),
    constraint tol_id_key unique (tol_id)
);
create table assembly_sequence
(
    assembly_sequence_id int
        primary key,
    name                 TEXT        null,
    assembly_id          int         not null,
    accession            TEXT        null,
    chromosomal          tinyINTEGER not null,
    length               int         not null,
    sequence_location    TEXT        null,
    sequence_checksum    TEXT        null,
    ga4gh_identifier     TEXT        null,
    constraint assembly_sequence_assembly_id_accession_5f3e5119_uniq unique (assembly_id, accession),
    constraint assembly_sequence_assembly_id_2a84ddcb_fk_assembly_assembly_id foreign key (assembly_id) references assembly (assembly_id)
);
create index assembly_sequence_assembly_id_2a84ddcb on assembly_sequence (assembly_id);
create table attribute
(
    attribute_id int
        primary key,
    name         TEXT                  not null,
    label        TEXT                  not null,
    description  TEXT                  null,
    type         TEXT default 'string' null,
    constraint name unique (name),
    constraint name_2 unique (name),
    constraint name_3 unique (name)
);
create table dataset_source
(
    dataset_source_id int primary key,
    type              TEXT not null,
    name              TEXT not null,
    constraint name unique (name)
);
create table dataset_type
(
    dataset_type_id int primary key,
    name            TEXT not null,
    label           TEXT not null,
    topic           TEXT not null,
    description     TEXT null,
    details_uri     TEXT null
);
create table dataset
(
    dataset_id        int primary key,
    dataset_uuid      TEXT                     not null,
    dataset_type_id   int                      not null,
    name              TEXT                     not null,
    version           TEXT                     null,
    created           datetime(6)              not null,
    dataset_source_id int                      not null,
    label             TEXT                     not null,
    status            TEXT default 'Submitted' null,
    constraint dataset_uuid unique (dataset_uuid),
    constraint dataset_dataset_source_id_fd96f115_fk_dataset_s foreign key (dataset_source_id) references dataset_source (dataset_source_id),
    constraint dataset_dataset_type_id_47284562_fk_dataset_type_dataset_type_id foreign key (dataset_type_id) references dataset_type (dataset_type_id)
);
create index dataset_type_id_eb55ae9a
    on dataset (dataset_type_id);
create table dataset_attribute
(
    dataset_attribute_id int primary key,
    value                TEXT not null,
    attribute_id         int  not null,
    dataset_id           int  not null,
    constraint dataset_attribute_dataset_id_attribute_id__d3b34d8c_uniq unique (dataset_id, attribute_id, value),
    constraint dataset_attribute_attribute_id_55c51407_fk_attribute foreign key (attribute_id) references attribute (attribute_id),
    constraint dataset_attribute_dataset_id_2e2afe19_fk_dataset_dataset_id foreign key (dataset_id) references dataset (dataset_id)
);
create index dataset_attribute_dataset_id_2e2afe19 on dataset_attribute (dataset_id);
create table django_migrations
(
    id      int primary key,
    app     TEXT        not null,
    name    TEXT        not null,
    applied datetime(6) not null
);
create table ensembl_site
(
    site_id int primary key,
    name    TEXT not null,
    label   TEXT not null,
    uri     TEXT not null
);
create table ensembl_release
(
    release_id   int primary key,
    version      decimal(10, 1) not null,
    release_date date           not null,
    label        TEXT           null,
    is_current   tinyINTEGER    not null,
    site_id      int            null,
    release_type TEXT           not null,
    constraint ensembl_release_version_site_id_b743399a_uniq unique (version, site_id),
    constraint ensembl_release_site_id_7c2f537a_fk_ensembl_site_site_id foreign key (site_id) references ensembl_site (site_id)
);
create table organism
(
    organism_id              int primary key,
    taxonomy_id              int  not null,
    species_taxonomy_id      int  null,
    display_name             TEXT not null,
    strain                   TEXT null,
    scientific_name          TEXT null,
    url_name                 TEXT not null,
    ensembl_name             TEXT not null,
    scientific_parlance_name TEXT null,
    constraint ensembl_name unique (ensembl_name)
);
create table genome
(
    genome_id   int primary key,
    genome_uuid TEXT        not null,
    assembly_id int         not null,
    organism_id int         not null,
    created     datetime(6) not null,
    constraint genome_uuid unique (genome_uuid),
    constraint genome_assembly_id_0a748388_fk_assembly_assembly_id foreign key (assembly_id) references assembly (assembly_id),
    constraint genome_organism_id_99ad7f35_fk_organism_organism_id foreign key (organism_id) references organism (organism_id)
);
create table genome_dataset
(
    genome_dataset_id int primary key,
    dataset_id        int         not null,
    genome_id         int         not null,
    release_id        int         null,
    is_current        tinyINTEGER not null,
    constraint ensembl_metadata_gen_dataset_id_26d7bac7_fk_dataset_d foreign key (dataset_id) references dataset (dataset_id),
    constraint ensembl_metadata_gen_genome_id_7670a2c5_fk_genome_ge foreign key (genome_id) references genome (genome_id),
    constraint ensembl_metadata_gen_release_id_c5440b9a_fk_ensembl_r foreign key (release_id) references ensembl_release (release_id)
);
create table genome_release
(
    genome_release_id int primary key,
    genome_id         int         not null,
    release_id        int         not null,
    is_current        tinyINTEGER not null,
    constraint genome_release_genome_id_3e45dc04_fk foreign key (genome_id) references genome (genome_id),
    constraint genome_release_release_id_bca7e1e5_fk_ensembl_release_release_id foreign key (release_id) references ensembl_release (release_id)
);
create table organism_group
(
    organism_group_id int primary key,
    type              TEXT not null,
    name              TEXT not null,
    code              TEXT null,
    constraint group_type_name_63c2f6ac_uniq unique (type, name),
    constraint organism_group_code_uindex unique (code)
);
create table organism_group_member
(
    organism_group_member_id int primary key,
    is_reference             tinyINTEGER not null,
    organism_id              int         not null,
    organism_group_id        int         not null,
    constraint organism_group_member_organism_id_organism_gro_fe8f49ac_uniq unique (organism_id, organism_group_id),
    constraint organism_group_membe_organism_group_id_533ca128_fk_organism_ foreign key (organism_group_id) references organism_group (organism_group_id),
    constraint organism_group_membe_organism_id_2808252e_fk_organism_ foreign key (organism_id) references organism (organism_id)
);

COMMIT;
PRAGMA ignore_check_constraints = ON;
PRAGMA foreign_keys = ON;
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
