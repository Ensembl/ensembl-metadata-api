CREATE TABLE `assembly`
(
    `assembly_id`      int(11) NOT NULL AUTO_INCREMENT,
    `ucsc_name`        varchar(16)  DEFAULT NULL,
    `accession`        varchar(16)  NOT NULL,
    `level`            varchar(32)  NOT NULL,
    `name`             varchar(128) NOT NULL,
    `accession_body`   varchar(32)  DEFAULT NULL,
    `assembly_default` varchar(128) DEFAULT NULL,
    `tol_id`           varchar(32)  DEFAULT NULL,
    `created`          datetime(6) DEFAULT NULL,
    `ensembl_name`     varchar(255) DEFAULT NULL,
    `assembly_uuid`    char(36)     NOT NULL,
    `is_reference`     tinyint(1) NOT NULL,
    PRIMARY KEY (`assembly_id`),
    UNIQUE KEY `accession` (`accession`),
    UNIQUE KEY `assembly_uuid` (`assembly_uuid`),
    UNIQUE KEY `ensembl_name` (`ensembl_name`)
) ENGINE=InnoDB AUTO_INCREMENT=220 DEFAULT CHARSET=latin1;

CREATE TABLE `assembly_sequence`
(
    `assembly_sequence_id` int(11) NOT NULL AUTO_INCREMENT,
    `name`                 varchar(128) DEFAULT NULL,
    `accession`            varchar(128) NOT NULL,
    `chromosomal`          tinyint(1) NOT NULL DEFAULT '0',
    `length`               int(11) NOT NULL,
    `sequence_location`    varchar(10)  DEFAULT NULL,
    `md5`                  varchar(32)  DEFAULT NULL,
    `assembly_id`          int(11) NOT NULL,
    `chromosome_rank`      int(11) DEFAULT NULL,
    `sha512t24u`           varchar(128) DEFAULT NULL,
    `is_circular`          tinyint(1) NOT NULL DEFAULT '0',
    `type`                 varchar(26)  NOT NULL,
    `additional`           tinyint(1) NOT NULL DEFAULT '0',
    `source`               varchar(120) DEFAULT NULL,
    PRIMARY KEY (`assembly_sequence_id`),
    UNIQUE KEY `assembly_sequence_assembly_id_accession_5f3e5119_uniq` (`assembly_id`,`accession`),
    KEY                    `assembly_sequence_assembly_id_chromosomal_index` (`assembly_id`,`chromosomal`),
    KEY                    `assembly_sequence_name_assembly_id_index` (`name`,`assembly_id`),
    CONSTRAINT `assembly_sequence_assembly_id_2a84ddcb_fk_assembly_assembly_id` FOREIGN KEY (`assembly_id`) REFERENCES `assembly` (`assembly_id`) ON DELETE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=3785696 DEFAULT CHARSET=latin1;

CREATE TABLE `attribute`
(
    `attribute_id` int(11) NOT NULL AUTO_INCREMENT,
    `name`         varchar(128) NOT NULL,
    `label`        varchar(128) NOT NULL,
    `description`  varchar(255) DEFAULT NULL,
    `type`         enum('string','integer','bp','percent','float') NOT NULL,
    `required`     tinyint(1) NOT NULL DEFAULT '0',
    PRIMARY KEY (`attribute_id`),
    UNIQUE KEY `name` (`name`),
    UNIQUE KEY `name_2` (`name`),
    UNIQUE KEY `name_3` (`name`)
) ENGINE=InnoDB AUTO_INCREMENT=198 DEFAULT CHARSET=latin1;

CREATE TABLE `dataset`
(
    `dataset_id`        int(11) NOT NULL AUTO_INCREMENT,
    `dataset_uuid`      char(36)     NOT NULL,
    `name`              varchar(128) NOT NULL,
    `version`           varchar(128) DEFAULT NULL,
    `created`           datetime(6) NOT NULL,
    `label`             varchar(128) NOT NULL,
    `dataset_source_id` int(11) NOT NULL,
    `dataset_type_id`   int(11) NOT NULL,
    `status`            enum('Submitted','Processing','Processed','Released','Faulty','Suppressed') NOT NULL DEFAULT 'Submitted',
    `parent_id`         int(11) DEFAULT NULL,
    PRIMARY KEY (`dataset_id`),
    KEY                 `dataset_dataset_source_id_fd96f115_fk_dataset_s` (`dataset_source_id`),
    KEY                 `dataset_dataset_type_id_47284562_fk_dataset_type_dataset_type_id` (`dataset_type_id`),
    KEY                 `dataset_parent_id_fk` (`parent_id`),
    CONSTRAINT `dataset_dataset_source_id_fd96f115_fk_dataset_s` FOREIGN KEY (`dataset_source_id`) REFERENCES `dataset_source` (`dataset_source_id`) ON DELETE CASCADE,
    CONSTRAINT `dataset_dataset_type_id_47284562_fk_dataset_type_dataset_type_id` FOREIGN KEY (`dataset_type_id`) REFERENCES `dataset_type` (`dataset_type_id`),
    CONSTRAINT `dataset_parent_id_fk` FOREIGN KEY (`parent_id`) REFERENCES `dataset` (`dataset_id`) ON DELETE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=9075 DEFAULT CHARSET=latin1;

CREATE TABLE `dataset_attribute`
(
    `dataset_attribute_id` int(11) NOT NULL AUTO_INCREMENT,
    `value`                varchar(128) DEFAULT NULL,
    `attribute_id`         int(11) NOT NULL,
    `dataset_id`           int(11) NOT NULL,
    PRIMARY KEY (`dataset_attribute_id`),
    UNIQUE KEY `dataset_attribute_dataset_id_attribute_id_value_4d1ddfaf_uniq` (`dataset_id`,`attribute_id`,`value`),
    KEY                    `dataset_attribute_attribute_id_55c51407_fk_attribute` (`attribute_id`),
    KEY                    `dataset_attribute_dataset_id_2e2afe19` (`dataset_id`),
    CONSTRAINT `dataset_attribute_attribute_id_55c51407_fk_attribute` FOREIGN KEY (`attribute_id`) REFERENCES `attribute` (`attribute_id`) ON DELETE CASCADE,
    CONSTRAINT `dataset_attribute_dataset_id_2e2afe19_fk_dataset_dataset_id` FOREIGN KEY (`dataset_id`) REFERENCES `dataset` (`dataset_id`) ON DELETE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=211873 DEFAULT CHARSET=latin1;

CREATE TABLE `dataset_source`
(
    `dataset_source_id` int(11) NOT NULL AUTO_INCREMENT,
    `type`              varchar(32)  NOT NULL,
    `name`              varchar(255) NOT NULL,
    `location`          varchar(120) DEFAULT NULL,
    PRIMARY KEY (`dataset_source_id`),
    UNIQUE KEY `name` (`name`)
) ENGINE=InnoDB AUTO_INCREMENT=4353 DEFAULT CHARSET=latin1;

CREATE TABLE `dataset_type`
(
    `dataset_type_id` int(11) NOT NULL AUTO_INCREMENT,
    `name`            varchar(32)  NOT NULL,
    `label`           varchar(128) NOT NULL,
    `topic`           varchar(32)  NOT NULL,
    `description`     varchar(255) DEFAULT NULL,
    `parent_id`       int(11) DEFAULT NULL,
    PRIMARY KEY (`dataset_type_id`),
    UNIQUE KEY `name` (`name`),
    KEY               `dataset_type_parent_id_fk` (`parent_id`),
    CONSTRAINT `dataset_type_parent_id_fk` FOREIGN KEY (`parent_id`) REFERENCES `dataset_type` (`dataset_type_id`) ON DELETE SET NULL
) ENGINE=InnoDB AUTO_INCREMENT=39 DEFAULT CHARSET=latin1;

CREATE TABLE `ensembl_release`
(
    `release_id`   int(11) NOT NULL AUTO_INCREMENT,
    `version`      decimal(10, 1) NOT NULL,
    `release_date` date           NOT NULL,
    `label`        varchar(64)    NOT NULL,
    `is_current`   tinyint(1) NOT NULL DEFAULT '0',
    `release_type` enum('integrated','partial') NOT NULL,
    `site_id`      int(11) NOT NULL,
    `status`       varchar(12)    NOT NULL,
    `name`         varchar(3) DEFAULT NULL,
    PRIMARY KEY (`release_id`),
    UNIQUE KEY `ensembl_release_version_site_id_b743399a_uniq` (`version`,`site_id`),
    KEY            `ensembl_release_site_id_7c2f537a_fk_ensembl_site_site_id` (`site_id`),
    CONSTRAINT `ensembl_release_site_id_7c2f537a_fk_ensembl_site_site_id` FOREIGN KEY (`site_id`) REFERENCES `ensembl_site` (`site_id`)
) ENGINE=InnoDB AUTO_INCREMENT=7 DEFAULT CHARSET=latin1;

CREATE TABLE `ensembl_site`
(
    `site_id` int(11) NOT NULL AUTO_INCREMENT,
    `name`    varchar(64) NOT NULL,
    `label`   varchar(64) NOT NULL,
    `uri`     varchar(64) NOT NULL,
    PRIMARY KEY (`site_id`)
) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=latin1;

CREATE TABLE `genome`
(
    `genome_id`           int(11) NOT NULL AUTO_INCREMENT,
    `genome_uuid`         char(36)     NOT NULL,
    `created`             datetime(6) NOT NULL,
    `assembly_id`         int(11) NOT NULL,
    `organism_id`         int(11) NOT NULL,
    `production_name`     varchar(120) NOT NULL,
    `genebuild_version`   varchar(64)  NOT NULL,
    `genebuild_date`      varchar(20)  NOT NULL,
    `annotation_source`   varchar(120) NOT NULL,
    `suppressed`          tinyint(1) NOT NULL DEFAULT '0',
    `suppression_details` varchar(255) DEFAULT NULL,
    `url_name`            varchar(128) DEFAULT NULL,
    PRIMARY KEY (`genome_id`),
    UNIQUE KEY `genome_genome_uuid_6b62d0ad_uniq` (`genome_uuid`),
    KEY                   `genome_assembly_id_0a748388_fk_assembly_assembly_id` (`assembly_id`),
    KEY                   `genome_organism_id_99ad7f35_fk_organism_organism_id` (`organism_id`),
    CONSTRAINT `genome_assembly_id_0a748388_fk_assembly_assembly_id` FOREIGN KEY (`assembly_id`) REFERENCES `assembly` (`assembly_id`) ON DELETE CASCADE,
    CONSTRAINT `genome_organism_id_99ad7f35_fk_organism_organism_id` FOREIGN KEY (`organism_id`) REFERENCES `organism` (`organism_id`) ON DELETE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=205 DEFAULT CHARSET=latin1;

CREATE TABLE `genome_dataset`
(
    `genome_dataset_id` int(11) NOT NULL AUTO_INCREMENT,
    `is_current`        tinyint(1) NOT NULL,
    `dataset_id`        int(11) NOT NULL,
    `genome_id`         int(11) NOT NULL,
    `release_id`        int(11) DEFAULT NULL,
    PRIMARY KEY (`genome_dataset_id`),
    UNIQUE KEY `uk_genome_dataset` (`dataset_id`,`genome_id`),
    KEY                 `genome_dataset_genome_id_21d55a50_fk_genome_genome_id` (`genome_id`),
    KEY                 `genome_dataset_release_id_1903f87c_fk_ensembl_release_release_id` (`release_id`),
    CONSTRAINT `genome_dataset_dataset_id_0e9b7c99_fk_dataset_dataset_id` FOREIGN KEY (`dataset_id`) REFERENCES `dataset` (`dataset_id`) ON DELETE CASCADE,
    CONSTRAINT `genome_dataset_genome_id_21d55a50_fk_genome_genome_id` FOREIGN KEY (`genome_id`) REFERENCES `genome` (`genome_id`) ON DELETE CASCADE,
    CONSTRAINT `genome_dataset_release_id_1903f87c_fk_ensembl_release_release_id` FOREIGN KEY (`release_id`) REFERENCES `ensembl_release` (`release_id`) ON DELETE SET NULL
) ENGINE=InnoDB AUTO_INCREMENT=9020 DEFAULT CHARSET=latin1;

CREATE TABLE `genome_group`
(
    `genome_group_id` int(11) NOT NULL AUTO_INCREMENT,
    `type`            enum('compara_reference','structural_variant','project') NOT NULL,
    `name`            varchar(128) NOT NULL,
    `label`           varchar(128) DEFAULT NULL,
    `searchable`      tinyint(1) NOT NULL DEFAULT '0',
    `description`     varchar(255) DEFAULT NULL,
    PRIMARY KEY (`genome_group_id`),
    UNIQUE KEY `unique_type_name` (`type`,`name`),
    KEY               `idx_type` (`type`),
    KEY               `idx_searchable` (`searchable`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 ROW_FORMAT=COMPACT;

CREATE TABLE `genome_group_member`
(
    `genome_group_member_id` int(11) NOT NULL AUTO_INCREMENT,
    `is_reference`           tinyint(1) NOT NULL DEFAULT '0',
    `genome_id`              int(11) NOT NULL,
    `genome_group_id`        int(11) NOT NULL,
    `release_id`             int(11) DEFAULT NULL,
    `is_current`             tinyint(1) NOT NULL DEFAULT '0',
    PRIMARY KEY (`genome_group_member_id`),
    UNIQUE KEY `unique_genome_group` (`genome_id`,`genome_group_id`),
    KEY                      `idx_genome_id` (`genome_id`),
    KEY                      `idx_genome_group_id` (`genome_group_id`),
    KEY                      `idx_release_id` (`release_id`),
    KEY                      `idx_is_current` (`is_current`),
    CONSTRAINT `fk_ggm_genome` FOREIGN KEY (`genome_id`) REFERENCES `genome` (`genome_id`) ON DELETE CASCADE,
    CONSTRAINT `fk_ggm_group` FOREIGN KEY (`genome_group_id`) REFERENCES `genome_group` (`genome_group_id`) ON DELETE CASCADE,
    CONSTRAINT `fk_ggm_release` FOREIGN KEY (`release_id`) REFERENCES `ensembl_release` (`release_id`) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8 ROW_FORMAT=COMPACT;

CREATE TABLE `genome_release`
(
    `genome_release_id` int(11) NOT NULL AUTO_INCREMENT,
    `is_current`        tinyint(1) NOT NULL,
    `genome_id`         int(11) NOT NULL,
    `release_id`        int(11) NOT NULL,
    PRIMARY KEY (`genome_release_id`),
    UNIQUE KEY `uk_genome_dataset` (`release_id`,`genome_id`),
    KEY                 `genome_release_genome_id_3e45dc04_fk_genome_genome_id` (`genome_id`),
    CONSTRAINT `genome_release_genome_id_3e45dc04_fk_genome_genome_id` FOREIGN KEY (`genome_id`) REFERENCES `genome` (`genome_id`),
    CONSTRAINT `genome_release_release_id_bca7e1e5_fk_ensembl_release_release_id` FOREIGN KEY (`release_id`) REFERENCES `ensembl_release` (`release_id`)
) ENGINE=InnoDB AUTO_INCREMENT=31 DEFAULT CHARSET=latin1;

CREATE TABLE `ncbi_taxa_name` (
  `taxon_id` int(10) unsigned NOT NULL,
  `name` varchar(500) NOT NULL,
  `name_class` varchar(50) NOT NULL,
  KEY `taxon_id` (`taxon_id`),
  KEY `name` (`name`),
  KEY `name_class` (`name_class`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TABLE `ncbi_taxa_node` (
  `taxon_id` int(10) unsigned NOT NULL,
  `parent_id` int(10) unsigned NOT NULL,
  `rank` char(32) NOT NULL DEFAULT '',
  `genbank_hidden_flag` tinyint(1) NOT NULL DEFAULT '0',
  `left_index` int(10) NOT NULL DEFAULT '0',
  `right_index` int(10) NOT NULL DEFAULT '0',
  `root_id` int(10) NOT NULL DEFAULT '1',
  PRIMARY KEY (`taxon_id`),
  KEY `parent_id` (`parent_id`),
  KEY `rank` (`rank`),
  KEY `left_index` (`left_index`),
  KEY `right_index` (`right_index`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TABLE `organism`
(
    `organism_id`              int(11) NOT NULL AUTO_INCREMENT,
    `taxonomy_id`              int(11) NOT NULL,
    `species_taxonomy_id`      int(11) DEFAULT NULL,
    `common_name`              varchar(128) NOT NULL,
    `strain`                   varchar(128) DEFAULT NULL,
    `scientific_name`          varchar(128) DEFAULT NULL,
    `biosample_id`             varchar(128) NOT NULL,
    `scientific_parlance_name` varchar(255) DEFAULT NULL,
    `organism_uuid`            char(36)     NOT NULL,
    `strain_type`              varchar(128) DEFAULT NULL,
    `rank`                     int(11) DEFAULT '0',
    PRIMARY KEY (`organism_id`),
    UNIQUE KEY `ensembl_name` (`biosample_id`),
    UNIQUE KEY `organism_uuid` (`organism_uuid`)
) ENGINE=InnoDB AUTO_INCREMENT=176 DEFAULT CHARSET=latin1;

CREATE TABLE `organism_group`
(
    `organism_group_id` int(11) NOT NULL AUTO_INCREMENT,
    `type`              varchar(32) DEFAULT NULL,
    `name`              varchar(255) NOT NULL,
    `code`              varchar(48) DEFAULT NULL,
    PRIMARY KEY (`organism_group_id`),
    UNIQUE KEY `code` (`code`),
    UNIQUE KEY `organism_group_type_name_170b6dae_uniq` (`type`,`name`)
) ENGINE=InnoDB AUTO_INCREMENT=15 DEFAULT CHARSET=latin1;

CREATE TABLE `organism_group_member`
(
    `organism_group_member_id` int(11) NOT NULL AUTO_INCREMENT,
    `is_reference`             tinyint(1) NOT NULL DEFAULT '0',
    `organism_id`              int(11) NOT NULL,
    `organism_group_id`        int(11) NOT NULL,
    `order`                    int(11) DEFAULT NULL,
    PRIMARY KEY (`organism_group_member_id`),
    UNIQUE KEY `organism_group_member_organism_id_organism_gro_fe8f49ac_uniq` (`organism_id`,`organism_group_id`),
    KEY                        `organism_group_membe_organism_group_id_533ca128_fk_organism_` (`organism_group_id`),
    CONSTRAINT `organism_group_membe_organism_group_id_533ca128_fk_organism_` FOREIGN KEY (`organism_group_id`) REFERENCES `organism_group` (`organism_group_id`) ON DELETE CASCADE,
    CONSTRAINT `organism_group_membe_organism_id_2808252e_fk_organism_` FOREIGN KEY (`organism_id`) REFERENCES `organism` (`organism_id`) ON DELETE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=235 DEFAULT CHARSET=latin1;

CREATE TABLE `sequence_alias`
(
    `sequence_alias_id`    int(11) NOT NULL AUTO_INCREMENT,
    `assembly_sequence_id` int(11) NOT NULL,
    `alias`                varchar(128) NOT NULL,
    `source`               varchar(128) DEFAULT NULL,
    PRIMARY KEY (`sequence_alias_id`),
    UNIQUE KEY `unique_sequence_alias` (`assembly_sequence_id`,`alias`),
    KEY                    `idx_alias` (`alias`),
    KEY                    `idx_assembly_sequence_id` (`assembly_sequence_id`),
    CONSTRAINT `fk_sa_assembly_sequence` FOREIGN KEY (`assembly_sequence_id`) REFERENCES `assembly_sequence` (`assembly_sequence_id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8 ROW_FORMAT=COMPACT;

