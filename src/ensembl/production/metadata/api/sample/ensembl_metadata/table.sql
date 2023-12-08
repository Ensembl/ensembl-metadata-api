CREATE TABLE assembly
(
    assembly_id      int auto_increment primary key,
    assembly_uuid    varchar(128) not null,
    ucsc_name        varchar(16) null,
    accession        varchar(16)  not null,
    level            varchar(32)  not null,
    name             varchar(128) not null,
    accession_body   varchar(32) null,
    assembly_default varchar(32) null,
    tol_id           varchar(32) null,
    created          datetime null,
    ensembl_name     varchar(255) null,
    alt_accession    varchar(16) null,
    is_reference     tinyint(1) not null default 0,
    url_name         varchar(128) null,

    constraint assembly_uuid
        unique (assembly_uuid),
    constraint accession
        unique (accession),
    constraint assembly_ensembl_name_uindex
        unique (ensembl_name),
    constraint tol_id_key
        unique (tol_id)
);

CREATE TABLE assembly_sequence
(
    assembly_sequence_id int auto_increment
        primary key,
    name                 varchar(128) null,
    assembly_id          int not null,
    accession            varchar(128) null,
    chromosomal          tinyint(1) not null default 0,
    length               int not null,
    chromosome_rank      int null,
    sequence_location    varchar(10) null,
    md5                  varchar(32) null,
    sha512t24u           varchar(128) null,
    type                 varchar(128) not null,
    is_circular          tinyint(1) not null default 0,

    constraint assembly_sequence_assembly_id_accession_5f3e5119_uniq
        unique (assembly_id, accession),
    constraint assembly_sequence_assembly_id_2a84ddcb_fk_assembly_assembly_id
        foreign key (assembly_id) references assembly (assembly_id)
);

create index assembly_sequence_assembly_id_2a84ddcb
    on assembly_sequence (assembly_id);

CREATE TABLE attribute
(
    attribute_id int auto_increment
        primary key,
    name         varchar(128) not null,
    label        varchar(128) not null,
    description  varchar(255) null,
    type         enum ('integer', 'float', 'percent', 'string', 'bp') default 'string' null,
    constraint name
        unique (name),
    constraint name_2
        unique (name),
    constraint name_3
        unique (name)
);

CREATE TABLE dataset_source
(
    dataset_source_id int auto_increment
        primary key,
    type              varchar(32)  not null,
    name              varchar(255) not null,
    constraint name
        unique (name)
);

CREATE TABLE dataset_type
(
    dataset_type_id int auto_increment
        primary key,
    name            varchar(32)  not null,
    label           varchar(128) not null,
    topic           varchar(32)  not null,
    description     varchar(255) null,
    details_uri     varchar(255) null
);

CREATE TABLE dataset
(
    dataset_id        int auto_increment
        primary key,
    dataset_uuid      varchar(128) not null,
    dataset_type_id   int          not null,
    name              varchar(128) not null,
    version           varchar(128) null,
    created           datetime(6) not null,
    dataset_source_id int          not null,
    label             varchar(128) not null,
    status            enum ('Submitted', 'Progressing', 'Processed') default 'Submitted' null,
    constraint dataset_uuid
        unique (dataset_uuid),
    constraint dataset_dataset_source_id_fd96f115_fk_dataset_s
        foreign key (dataset_source_id) references dataset_source (dataset_source_id),
    constraint dataset_dataset_type_id_47284562_fk_dataset_type_dataset_type_id
        foreign key (dataset_type_id) references dataset_type (dataset_type_id)
);

create index dataset_type_id_eb55ae9a
    on dataset (dataset_type_id);

CREATE TABLE dataset_attribute
(
    dataset_attribute_id int auto_increment
        primary key,
    value                varchar(128) null,
    attribute_id         int not null,
    dataset_id           int not null,
    constraint dataset_attribute_dataset_id_attribute_id__d3b34d8c_uniq
        unique (dataset_id, attribute_id),
    constraint dataset_attribute_attribute_id_55c51407_fk_attribute
        foreign key (attribute_id) references attribute (attribute_id),
    constraint dataset_attribute_dataset_id_2e2afe19_fk_dataset_dataset_id
        foreign key (dataset_id) references dataset (dataset_id)
);

create index dataset_attribute_dataset_id_2e2afe19
    on dataset_attribute (dataset_id);

CREATE TABLE ensembl_site
(
    site_id int auto_increment
        primary key,
    name    varchar(64) not null,
    label   varchar(64) not null,
    uri     varchar(64) not null
);

CREATE TABLE ensembl_release
(
    release_id   int auto_increment
        primary key,
    version      decimal(10, 1) not null,
    release_date date           not null,
    label        varchar(64) null,
    is_current   tinyint(1) not null default 0,
    site_id      int null,
    release_type varchar(16)    not null,
    constraint ensembl_release_version_site_id_b743399a_uniq
        unique (version, site_id),
    constraint ensembl_release_site_id_7c2f537a_fk_ensembl_site_site_id
        foreign key (site_id) references ensembl_site (site_id)
);

CREATE TABLE organism
(
    organism_id              int auto_increment
        primary key,
    organism_uuid            varchar(128) not null,
    taxonomy_id              int          not null,
    species_taxonomy_id      int null,
    common_name              varchar(128) not null,
    strain                   varchar(128) null,
    scientific_name          varchar(128) null,
    ensembl_name             varchar(128) not null,
    scientific_parlance_name varchar(255) null,
    strain_type              varchar(255) null,
    constraint organism_uuid
        unique (organism_uuid),
    constraint ensembl_name
        unique (ensembl_name)
);

CREATE TABLE genome
(
    genome_id   int auto_increment
        primary key,
    genome_uuid varchar(128) not null,
    assembly_id int          not null,
    organism_id int          not null,
    created     datetime(6) not null,
    is_best     tinyint(1) not null default 0,
    production_name varchar(256) not null,

    constraint genome_uuid
        unique (genome_uuid),
    constraint genome_assembly_id_0a748388_fk_assembly_assembly_id
        foreign key (assembly_id) references assembly (assembly_id),
    constraint genome_organism_id_99ad7f35_fk_organism_organism_id
        foreign key (organism_id) references organism (organism_id)
);

CREATE TABLE genome_dataset
(
    genome_dataset_id int auto_increment
        primary key,
    dataset_id        int not null,
    genome_id         int not null,
    release_id        int null,
    is_current        tinyint(1) not null default 0,
    constraint ensembl_metadata_gen_dataset_id_26d7bac7_fk_dataset_d
        foreign key (dataset_id) references dataset (dataset_id) on DELETE CASCADE,
    constraint ensembl_metadata_gen_genome_id_7670a2c5_fk_genome_ge
        foreign key (genome_id) references genome (genome_id) ON DELETE CASCADE,
    constraint ensembl_metadata_gen_release_id_c5440b9a_fk_ensembl_r
        foreign key (release_id) references ensembl_release (release_id) ON DELETE CASCADE
);

CREATE TABLE genome_release
(
    genome_release_id int auto_increment
        primary key,
    genome_id         int not null,
    release_id        int not null,
    is_current        tinyint(1) not null default 0,
    constraint genome_release_genome_id_3e45dc04_fk
        foreign key (genome_id) references genome (genome_id),
    constraint genome_release_release_id_bca7e1e5_fk_ensembl_release_release_id
        foreign key (release_id) references ensembl_release (release_id)
);

CREATE TABLE organism_group
(
    organism_group_id int auto_increment
        primary key,
    type              varchar(32)  not null,
    name              varchar(255) not null,
    code              varchar(48) null,
    constraint group_type_name_63c2f6ac_uniq
        unique (type, name),
    constraint organism_group_code_uindex
        unique (code)
);

CREATE TABLE `organism_group_member`
(
    `organism_group_member_id` int NOT NULL AUTO_INCREMENT,
    `is_reference`             tinyint(1) NOT NULL DEFAULT 0,
    `organism_id`              int NOT NULL,
    `organism_group_id`        int NOT NULL,
    `order`                    int DEFAULT NULL,
    PRIMARY KEY (`organism_group_member_id`),
    UNIQUE KEY `organism_group_member_organism_id_organism_gro_fe8f49ac_uniq` (`organism_id`,`organism_group_id`),
    KEY                        `organism_group_membe_organism_group_id_533ca128_fk_organism_` (`organism_group_id`),
    CONSTRAINT `organism_group_membe_organism_group_id_533ca128_fk_organism_` FOREIGN KEY (`organism_group_id`) REFERENCES `organism_group` (`organism_group_id`),
    CONSTRAINT `organism_group_membe_organism_id_2808252e_fk_organism_` FOREIGN KEY (`organism_id`) REFERENCES `organism` (`organism_id`)
);