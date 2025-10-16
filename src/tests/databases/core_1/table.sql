CREATE TABLE `attrib_type` (
  `attrib_type_id` smallint(5) unsigned NOT NULL AUTO_INCREMENT,
  `code` varchar(20) NOT NULL DEFAULT '',
  `name` varchar(255) NOT NULL DEFAULT '',
  `description` text,
  PRIMARY KEY (`attrib_type_id`),
  UNIQUE KEY `code_idx` (`code`)
) ENGINE=InnoDB AUTO_INCREMENT=548 DEFAULT CHARSET=latin1;

CREATE TABLE `coord_system`
(
    `coord_system_id` int(10) unsigned NOT NULL AUTO_INCREMENT,
    `species_id`      int(10) unsigned NOT NULL DEFAULT '1',
    `name`            varchar(40) NOT NULL,
    `version`         varchar(255) DEFAULT NULL,
    `rank`            int(11) NOT NULL,
    `attrib` set('default_version','sequence_level') DEFAULT NULL,
    PRIMARY KEY (`coord_system_id`),
    UNIQUE KEY `rank_idx` (`rank`,`species_id`),
    UNIQUE KEY `name_idx` (`name`,`version`,`species_id`),
    KEY               `species_idx` (`species_id`)
) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=latin1;

CREATE TABLE `meta`
(
    `meta_id`    int(11) NOT NULL AUTO_INCREMENT,
    `species_id` int(10) unsigned DEFAULT '1',
    `meta_key`   varchar(40)  NOT NULL,
    `meta_value` varchar(255) NOT NULL,
    PRIMARY KEY (`meta_id`),
    UNIQUE KEY `species_key_value_idx` (`species_id`,`meta_key`,`meta_value`),
    KEY          `species_value_idx` (`species_id`,`meta_value`)
) ENGINE=InnoDB AUTO_INCREMENT=29 DEFAULT CHARSET=latin1;

CREATE TABLE `seq_region`
(
    `seq_region_id`   int(10) unsigned NOT NULL AUTO_INCREMENT,
    `name`            varchar(255) NOT NULL,
    `coord_system_id` int(10) unsigned NOT NULL,
    `length`          int(10) unsigned NOT NULL,
    PRIMARY KEY (`seq_region_id`),
    UNIQUE KEY `name_cs_idx` (`name`,`coord_system_id`),
    KEY               `cs_idx` (`coord_system_id`)
) ENGINE=InnoDB AUTO_INCREMENT=4 DEFAULT CHARSET=latin1;

CREATE TABLE `seq_region_attrib`
(
    `seq_region_id`  int(10) unsigned NOT NULL DEFAULT '0',
    `attrib_type_id` smallint(5) unsigned NOT NULL DEFAULT '0',
    `value`          text NOT NULL,
    UNIQUE KEY `region_attribx` (`seq_region_id`,`attrib_type_id`,`value`(500)),
    KEY              `seq_region_idx` (`seq_region_id`),
    KEY              `type_val_idx` (`attrib_type_id`,`value`(40)),
    KEY              `val_only_idx` (`value`(40))
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TABLE `seq_region_synonym`
(
    `seq_region_synonym_id` int(10) unsigned NOT NULL AUTO_INCREMENT,
    `seq_region_id`         int(10) unsigned NOT NULL,
    `synonym`               varchar(250) NOT NULL,
    `external_db_id`        int(10) unsigned DEFAULT NULL,
    PRIMARY KEY (`seq_region_synonym_id`),
    UNIQUE KEY `syn_idx` (`synonym`,`seq_region_id`),
    KEY                     `seq_region_idx` (`seq_region_id`)
) ENGINE=InnoDB AUTO_INCREMENT=4 DEFAULT CHARSET=latin1;

