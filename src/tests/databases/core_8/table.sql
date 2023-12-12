CREATE TABLE coord_system
(
    coord_system_id int unsigned auto_increment
        primary key,
    species_id      int unsigned default 1                    not null,
    name            varchar(40)                               not null,
    version         varchar(255)                              null,
    `rank`          int                                       not null,
    attrib          set ('default_version', 'sequence_level') null,
    constraint name_idx
        unique (name, version, species_id),
    constraint rank_idx
        unique (`rank`, species_id)
);

CREATE INDEX species_idx
    on coord_system (species_id);

CREATE TABLE meta
(
    meta_id    int auto_increment
        primary key,
    species_id int unsigned default 1 null,
    meta_key   varchar(40)            not null,
    meta_value varchar(255)           not null,
    constraint species_key_value_idx
        unique (species_id, meta_key, meta_value)
);

CREATE INDEX species_value_idx
    on meta (species_id, meta_value);

CREATE TABLE seq_region
(
    seq_region_id   int unsigned auto_increment
        primary key,
    name            varchar(255) not null,
    coord_system_id int unsigned not null,
    length          int unsigned not null,
    constraint name_cs_idx
        unique (name, coord_system_id)
);

CREATE INDEX cs_idx
    on seq_region (coord_system_id);

CREATE TABLE seq_region_attrib
(
    seq_region_id  int unsigned      default 0 not null,
    attrib_type_id smallint unsigned default 0 not null,
    value          text                        not null,
    constraint region_attribx
        unique (seq_region_id, attrib_type_id, value(500))
);

CREATE INDEX seq_region_idx
    on seq_region_attrib (seq_region_id);

CREATE INDEX type_val_idx
    on seq_region_attrib (attrib_type_id, value(40));

CREATE INDEX val_only_idx
    on seq_region_attrib (value(40));

CREATE TABLE seq_region_synonym
(
    seq_region_synonym_id int unsigned auto_increment
        primary key,
    seq_region_id         int unsigned not null,
    synonym               varchar(250) not null,
    external_db_id        int unsigned null,
    constraint syn_idx
        unique (synonym, seq_region_id)
);

CREATE INDEX seq_region_idx
    on seq_region_synonym (seq_region_id);

CREATE TABLE `attrib_type` (
  `attrib_type_id` smallint(5) unsigned NOT NULL AUTO_INCREMENT,
  `code` varchar(20) NOT NULL DEFAULT '',
  `name` varchar(255) NOT NULL DEFAULT '',
  `description` text,
  PRIMARY KEY (`attrib_type_id`),
  UNIQUE KEY `code_idx` (`code`)
);