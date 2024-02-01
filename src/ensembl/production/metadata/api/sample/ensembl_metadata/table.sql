create table assembly
(
    assembly_id      int auto_increment
        primary key,
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
    assembly_uuid    char(40)     not null,
    is_reference     tinyint(1)   not null,
    url_name         varchar(128) null,
    constraint accession
        unique (accession),
    constraint assembly_uuid
        unique (assembly_uuid),
    constraint ensembl_name
        unique (ensembl_name)
)
    charset = utf8
    row_format = COMPACT;

create table assembly_sequence
(
    assembly_sequence_id int auto_increment
        primary key,
    name                 varchar(128) null,
    accession            varchar(128) not null,
    chromosomal          tinyint(1)   not null,
    length               int          not null,
    sequence_location    varchar(10)  null,
    md5                  varchar(32)  null,
    assembly_id          int          not null,
    chromosome_rank      int          null,
    sha512t4u            varchar(128) null,
    sha512t24u           varchar(128) null,
    is_circular          tinyint(1)   not null,
    type                 varchar(26)  not null,
    constraint assembly_sequence_assembly_id_accession_5f3e5119_uniq
        unique (assembly_id, accession),
    constraint assembly_sequence_assembly_id_2a84ddcb_fk_assembly_assembly_id
        foreign key (assembly_id) references assembly (assembly_id)
            on delete cascade
)
    charset = utf8
    row_format = COMPACT;

create index assembly_sequence_assembly_id_chromosomal_index
    on assembly_sequence (assembly_id, chromosomal);

create index assembly_sequence_name_assembly_id_index
    on assembly_sequence (name, assembly_id);

create table attribute
(
    attribute_id int auto_increment
        primary key,
    name         varchar(128) not null,
    label        varchar(128) not null,
    description  varchar(255) null,
    type         varchar(8)   not null,
    constraint attribute_name_e1b1f4a7_uniq
        unique (name)
)
    charset = utf8
    row_format = COMPACT;

create table auth_group
(
    id   int auto_increment
        primary key,
    name varchar(150) not null,
    constraint name
        unique (name)
)
    charset = utf8
    row_format = COMPACT;

create table auth_user
(
    id           int auto_increment
        primary key,
    password     varchar(128) not null,
    last_login   datetime(6)  null,
    is_superuser tinyint(1)   not null,
    username     varchar(150) not null,
    first_name   varchar(150) not null,
    last_name    varchar(150) not null,
    email        varchar(254) not null,
    is_staff     tinyint(1)   not null,
    is_active    tinyint(1)   not null,
    date_joined  datetime(6)  not null,
    constraint username
        unique (username)
)
    charset = utf8
    row_format = COMPACT;

create table auth_user_groups
(
    id       int auto_increment
        primary key,
    user_id  int not null,
    group_id int not null,
    constraint auth_user_groups_user_id_group_id_94350c0c_uniq
        unique (user_id, group_id),
    constraint auth_user_groups_group_id_97559544_fk_auth_group_id
        foreign key (group_id) references auth_group (id),
    constraint auth_user_groups_user_id_6a12ed8b_fk_auth_user_id
        foreign key (user_id) references auth_user (id)
)
    charset = utf8
    row_format = COMPACT;

create table dataset_source
(
    dataset_source_id int auto_increment
        primary key,
    type              varchar(32)  not null,
    name              varchar(255) not null,
    constraint name
        unique (name)
)
    charset = utf8
    row_format = COMPACT;

create table dataset_type
(
    dataset_type_id int auto_increment
        primary key,
    name            varchar(32)  not null,
    label           varchar(128) not null,
    topic           varchar(32)  not null,
    description     varchar(255) null,
    details_uri     varchar(255) null
)
    charset = utf8
    row_format = COMPACT;

create table dataset
(
    dataset_id        int auto_increment
        primary key,
    dataset_uuid      varchar(40)  not null,
    name              varchar(128) not null,
    version           varchar(128) null,
    created           datetime(6)  not null,
    label             varchar(128) not null,
    dataset_source_id int          not null,
    dataset_type_id   int          not null,
    status            varchar(12)  not null,
    constraint dataset_dataset_source_id_fd96f115_fk_dataset_s
        foreign key (dataset_source_id) references dataset_source (dataset_source_id)
            on delete cascade,
    constraint dataset_dataset_type_id_47284562_fk_dataset_type_dataset_type_id
        foreign key (dataset_type_id) references dataset_type (dataset_type_id)
)
    charset = utf8
    row_format = COMPACT;

create table dataset_attribute
(
    dataset_attribute_id int auto_increment
        primary key,
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
)
    charset = utf8
    row_format = COMPACT;

create index dataset_attribute_dataset_id_2e2afe19
    on dataset_attribute (dataset_id);

create table django_content_type
(
    id        int auto_increment
        primary key,
    app_label varchar(100) not null,
    model     varchar(100) not null,
    constraint django_content_type_app_label_model_76bd3d3b_uniq
        unique (app_label, model)
)
    charset = utf8
    row_format = COMPACT;

create table auth_permission
(
    id              int auto_increment
        primary key,
    name            varchar(255) not null,
    content_type_id int          not null,
    codename        varchar(100) not null,
    constraint auth_permission_content_type_id_codename_01ab375a_uniq
        unique (content_type_id, codename),
    constraint auth_permission_content_type_id_2f476e4b_fk_django_co
        foreign key (content_type_id) references django_content_type (id)
)
    charset = utf8
    row_format = COMPACT;

create table auth_group_permissions
(
    id            int auto_increment
        primary key,
    group_id      int not null,
    permission_id int not null,
    constraint auth_group_permissions_group_id_permission_id_0cd325b0_uniq
        unique (group_id, permission_id),
    constraint auth_group_permissio_permission_id_84c5c92e_fk_auth_perm
        foreign key (permission_id) references auth_permission (id),
    constraint auth_group_permissions_group_id_b120cbf9_fk_auth_group_id
        foreign key (group_id) references auth_group (id)
)
    charset = utf8
    row_format = COMPACT;

create table auth_user_user_permissions
(
    id            int auto_increment
        primary key,
    user_id       int not null,
    permission_id int not null,
    constraint auth_user_user_permissions_user_id_permission_id_14a6b632_uniq
        unique (user_id, permission_id),
    constraint auth_user_user_permi_permission_id_1fbb5f2c_fk_auth_perm
        foreign key (permission_id) references auth_permission (id),
    constraint auth_user_user_permissions_user_id_a95ead1b_fk_auth_user_id
        foreign key (user_id) references auth_user (id)
)
    charset = utf8
    row_format = COMPACT;

create table django_admin_log
(
    id              int auto_increment
        primary key,
    action_time     datetime(6)       not null,
    object_id       longtext          null,
    object_repr     varchar(200)      not null,
    action_flag     smallint unsigned not null,
    change_message  longtext          not null,
    content_type_id int               null,
    user_id         int               not null,
    constraint django_admin_log_content_type_id_c4bce8eb_fk_django_co
        foreign key (content_type_id) references django_content_type (id),
    constraint django_admin_log_user_id_c564eba6_fk_auth_user_id
        foreign key (user_id) references auth_user (id)
)
    charset = utf8
    row_format = COMPACT;

create table django_migrations
(
    id      int auto_increment
        primary key,
    app     varchar(255) not null,
    name    varchar(255) not null,
    applied datetime(6)  not null
)
    charset = utf8
    row_format = COMPACT;

create table django_session
(
    session_key  varchar(40) not null
        primary key,
    session_data longtext    not null,
    expire_date  datetime(6) not null
)
    charset = utf8
    row_format = COMPACT;

create index django_session_expire_date_a5c62663
    on django_session (expire_date);

create table ensembl_site
(
    site_id int auto_increment
        primary key,
    name    varchar(64) not null,
    label   varchar(64) not null,
    uri     varchar(64) not null
)
    charset = utf8
    row_format = COMPACT;

create table ensembl_release
(
    release_id   int auto_increment
        primary key,
    version      decimal(10, 1) not null,
    release_date date           not null,
    label        varchar(64)    null,
    is_current   tinyint(1)     not null,
    release_type varchar(16)    not null,
    site_id      int            null,
    constraint ensembl_release_version_site_id_b743399a_uniq
        unique (version, site_id),
    constraint ensembl_release_site_id_7c2f537a_fk_ensembl_site_site_id
        foreign key (site_id) references ensembl_site (site_id)
)
    charset = utf8
    row_format = COMPACT;

create table organism
(
    organism_id              int auto_increment
        primary key,
    taxonomy_id              int           not null,
    species_taxonomy_id      int           null,
    common_name              varchar(128)  not null,
    strain                   varchar(128)  null,
    scientific_name          varchar(128)  null,
    biosample_id             varchar(128)  not null,
    scientific_parlance_name varchar(255)  null,
    organism_uuid            char(40)      not null,
    strain_type              varchar(128)  null,
    `rank`                   int default 0 null,
    constraint ensembl_name
        unique (biosample_id),
    constraint organism_uuid
        unique (organism_uuid)
)
    charset = utf8
    row_format = COMPACT;

create table genome
(
    genome_id       int auto_increment
        primary key,
    genome_uuid     varchar(40)                    not null,
    created         datetime(6)                    not null,
    assembly_id     int                            not null,
    organism_id     int                            not null,
    is_best         tinyint(1)   default 0         not null,
    production_name varchar(255) default 'default' not null,
    constraint genome_genome_uuid_6b62d0ad_uniq
        unique (genome_uuid),
    constraint genome_assembly_id_0a748388_fk_assembly_assembly_id
        foreign key (assembly_id) references assembly (assembly_id)
            on delete cascade,
    constraint genome_organism_id_99ad7f35_fk_organism_organism_id
        foreign key (organism_id) references organism (organism_id)
            on delete cascade
)
    charset = utf8
    row_format = COMPACT;

create table genome_dataset
(
    genome_dataset_id int auto_increment
        primary key,
    is_current        tinyint(1) not null,
    dataset_id        int        not null,
    genome_id         int        not null,
    release_id        int        null,
    constraint genome_dataset_dataset_id_0e9b7c99_fk_dataset_dataset_id
        foreign key (dataset_id) references dataset (dataset_id)
            on delete cascade,
    constraint genome_dataset_genome_id_21d55a50_fk_genome_genome_id
        foreign key (genome_id) references genome (genome_id)
            on delete cascade,
    constraint genome_dataset_release_id_1903f87c_fk_ensembl_release_release_id
        foreign key (release_id) references ensembl_release (release_id)
            on delete set null
)
    charset = utf8
    row_format = COMPACT;

create table genome_release
(
    genome_release_id int auto_increment
        primary key,
    is_current        tinyint(1) not null,
    genome_id         int        not null,
    release_id        int        not null,
    constraint genome_release_genome_id_3e45dc04_fk_genome_genome_id
        foreign key (genome_id) references genome (genome_id),
    constraint genome_release_release_id_bca7e1e5_fk_ensembl_release_release_id
        foreign key (release_id) references ensembl_release (release_id)
)
    charset = utf8
    row_format = COMPACT;

create table organism_group
(
    organism_group_id int auto_increment
        primary key,
    type              varchar(32)  null,
    name              varchar(255) not null,
    code              varchar(48)  null,
    constraint code
        unique (code),
    constraint organism_group_type_name_170b6dae_uniq
        unique (type, name)
)
    charset = utf8
    row_format = COMPACT;

create table organism_group_member
(
    organism_group_member_id int auto_increment
        primary key,
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
)
    charset = utf8
    row_format = COMPACT;

