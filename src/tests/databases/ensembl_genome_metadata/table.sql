CREATE TABLE assembly
(
    assembly_id      int auto_increment primary key,
    ucsc_name        varchar(16)  null,
    accession        varchar(16)  not null,
    level            varchar(32)  not null,
    name             varchar(128) not null,
    accession_body   varchar(32)  null,
    assembly_default varchar(128) null,
    tol_id           varchar(32)  null,
    created          datetime(6)  null,
    ensembl_name     varchar(255) null,
    alt_accession    varchar(16)  null,
    assembly_uuid    char(36)     not null,
    is_reference     tinyint(1)   not null,
    url_name         varchar(128) null,
    constraint accession
        unique (accession),
    constraint assembly_uuid
        unique (assembly_uuid),
    constraint ensembl_name
        unique (ensembl_name)
);

CREATE TABLE assembly_sequence
(
    assembly_sequence_id int auto_increment primary key,
    name                 varchar(128) null,
    accession            varchar(128) not null,
    chromosomal          tinyint(1)   not null,
    length               int          not null,
    sequence_location    varchar(10)  null,
    md5                  varchar(32)  null,
    assembly_id          int          not null,
    chromosome_rank      int          null,
    sha512t24u           varchar(128) null,
    is_circular          tinyint(1)   not null,
    type                 varchar(26)  not null,
    constraint assembly_sequence_assembly_id_accession_5f3e5119_uniq
        unique (assembly_id, accession),
    constraint assembly_sequence_assembly_id_2a84ddcb_fk_assembly_assembly_id
        foreign key (assembly_id) references assembly (assembly_id)
            on delete cascade
);

create index assembly_sequence_assembly_id_chromosomal_index
    on assembly_sequence (assembly_id, chromosomal);

create index assembly_sequence_name_assembly_id_index
    on assembly_sequence (name, assembly_id);

CREATE TABLE attribute
(
    attribute_id int auto_increment primary key,
    name         varchar(128)                                                          not null,
    label        varchar(128)                                                          not null,
    description  varchar(255)                                                          null,
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
    dataset_source_id int auto_increment primary key,
    type              varchar(32)  not null,
    name              varchar(255) not null,
    constraint name
        unique (name)
);

CREATE TABLE dataset_type
(
    dataset_type_id int auto_increment primary key,
    name            varchar(32)  not null,
    label           varchar(128) not null,
    topic           varchar(32)  not null,
    description     varchar(255) null,
    details_uri     varchar(255) null,
    parent_id       int default null,
    depends_on      varchar(128) null,
    filter_on       longtext     null,
    constraint dataset_type_parent_id_fk
        foreign key (parent_id) references dataset_type (dataset_type_id)
            on delete set null
);

CREATE TABLE dataset
(
    dataset_id        int auto_increment primary key,
    dataset_uuid      char(36)  not null,
    name              varchar(128) not null,
    version           varchar(128) null,
    created           datetime(6)  not null,
    label             varchar(128) not null,
    dataset_source_id int          not null,
    dataset_type_id   int          not null,
    status            varchar(12)  not null,
    parent_id         int default null,
    constraint dataset_dataset_source_id_fd96f115_fk_dataset_s
        foreign key (dataset_source_id) references dataset_source (dataset_source_id)
            on delete cascade,
    constraint dataset_dataset_type_id_47284562_fk_dataset_type_dataset_type_id
        foreign key (dataset_type_id) references dataset_type (dataset_type_id),
    constraint dataset_parent_id_fk
        foreign key (parent_id) references dataset (dataset_id)
            on delete cascade
);

CREATE TABLE dataset_attribute
(
    dataset_attribute_id int auto_increment primary key,
    value                varchar(128) null,
    attribute_id         int          not null,
    dataset_id           int          not null,
    constraint dataset_attribute_dataset_id_attribute_id_value_4d1ddfaf_uniq
        unique (dataset_id, attribute_id, value),
    constraint dataset_attribute_attribute_id_55c51407_fk_attribute
        foreign key (attribute_id) references attribute (attribute_id)
            on delete cascade,
    constraint dataset_attribute_dataset_id_2e2afe19_fk_dataset_dataset_id
        foreign key (dataset_id) references dataset (dataset_id)
            on delete cascade
);

create index dataset_attribute_dataset_id_2e2afe19
    on dataset_attribute (dataset_id);

CREATE TABLE ensembl_site
(
    site_id int auto_increment primary key,
    name    varchar(64) not null,
    label   varchar(64) not null,
    uri     varchar(64) not null
);

CREATE TABLE ensembl_release
(
    release_id   int auto_increment primary key,
    version      decimal(10, 1) not null,
    release_date date           null,
    label        varchar(64)    null,
    is_current   tinyint(1)     not null,
    release_type varchar(16)    not null,
    site_id      int            null,
    status       varchar(12)    not null,
    constraint ensembl_release_version_site_id_b743399a_uniq
        unique (version, site_id),
    constraint ensembl_release_site_id_7c2f537a_fk_ensembl_site_site_id
        foreign key (site_id) references ensembl_site (site_id)
);

CREATE TABLE organism
(
    organism_id              int auto_increment primary key,
    taxonomy_id              int           not null,
    species_taxonomy_id      int           null,
    common_name              varchar(128)  not null,
    strain                   varchar(128)  null,
    scientific_name          varchar(128)  null,
    biosample_id             varchar(128)  not null,
    scientific_parlance_name varchar(255)  null,
    organism_uuid            char(36)      not null,
    strain_type              varchar(128)  null,
    `rank`                   int default 0 null,
    constraint ensembl_name
        unique (biosample_id),
    constraint organism_uuid
        unique (organism_uuid)
);
CREATE TABLE genome
(
    genome_id         int auto_increment
        primary key,
    genome_uuid       char(36)             not null,
    created           datetime(6)          not null,
    assembly_id       int                  not null,
    organism_id       int                  not null,
    is_best           tinyint(1) default 0 not null,
    production_name   varchar(255)         not null,
    genebuild_version varchar(20)          null,
    genebuild_date    varchar(20)          null,
    constraint genome_genome_uuid_6b62d0ad_uniq
        unique (genome_uuid),
    constraint genome_assembly_id_0a748388_fk_assembly_assembly_id
        foreign key (assembly_id) references assembly (assembly_id)
            on delete cascade,
    constraint genome_organism_id_99ad7f35_fk_organism_organism_id
        foreign key (organism_id) references organism (organism_id)
            on delete cascade
);


CREATE TABLE genome_dataset
(
    genome_dataset_id int auto_increment primary key,
    is_current        tinyint(1) not null,
    dataset_id        int        not null,
    genome_id         int        not null,
    release_id        int        null,
    constraint uk_genome_dataset UNIQUE KEY (dataset_id, genome_id),
    constraint genome_dataset_dataset_id_0e9b7c99_fk_dataset_dataset_id
        foreign key (dataset_id) references dataset (dataset_id)
            on delete cascade,
    constraint genome_dataset_genome_id_21d55a50_fk_genome_genome_id
        foreign key (genome_id) references genome (genome_id)
            on delete cascade,
    constraint genome_dataset_release_id_1903f87c_fk_ensembl_release_release_id
        foreign key (release_id) references ensembl_release (release_id)
            on delete set null
);

CREATE TABLE genome_release
(
    genome_release_id int auto_increment primary key,
    is_current        tinyint(1) not null,
    genome_id         int        not null,
    release_id        int        not null,
    constraint uk_genome_dataset UNIQUE KEY (release_id, genome_id),
    constraint genome_release_genome_id_3e45dc04_fk_genome_genome_id
        foreign key (genome_id) references genome (genome_id),
    constraint genome_release_release_id_bca7e1e5_fk_ensembl_release_release_id
        foreign key (release_id) references ensembl_release (release_id)
);

CREATE TABLE organism_group
(
    organism_group_id int auto_increment primary key,
    type              varchar(32)  null,
    name              varchar(255) not null,
    code              varchar(48)  null,
    constraint code
        unique (code),
    constraint organism_group_type_name_170b6dae_uniq
        unique (type, name)
);

CREATE TABLE organism_group_member
(
    organism_group_member_id int auto_increment primary key,
    is_reference             tinyint(1) null,
    organism_id              int        not null,
    organism_group_id        int        not null,
    `order`                  int        null,
    constraint organism_group_member_organism_id_organism_gro_fe8f49ac_uniq
        unique (organism_id, organism_group_id),
    constraint organism_group_membe_organism_group_id_533ca128_fk_organism_
        foreign key (organism_group_id) references organism_group (organism_group_id)
            on delete cascade,
    constraint organism_group_membe_organism_id_2808252e_fk_organism_
        foreign key (organism_id) references organism (organism_id)
            on delete cascade
);

