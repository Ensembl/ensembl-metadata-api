################################################################
############## Section for new organism          ###############
################################################################
create database test_core_1;
use test_core_1;
CREATE TABLE `meta` (
  `meta_id` int(11) NOT NULL AUTO_INCREMENT,
  `species_id` int(10) unsigned DEFAULT 1,
  `meta_key` varchar(40) NOT NULL,
  `meta_value` varchar(255) NOT NULL,
  PRIMARY KEY (`meta_id`),
  UNIQUE KEY `species_key_value_idx` (`species_id`,`meta_key`,`meta_value`),
  KEY `species_value_idx` (`species_id`,`meta_value`)
);
#The main data for the organism table
insert into meta (meta_key,meta_value) values ('species.species_taxonomy_id', 6666666);
insert into meta (meta_key,meta_value) values ('species.taxonomy_id', 66666668);
insert into meta (meta_key,meta_value) values ('species.display_name','jabberwocky');
insert into meta (meta_key,meta_value) values ('species.scientific_name','carol_jabberwocky');
insert into meta (meta_key,meta_value) values ('species.url','Jabbe');
insert into meta (meta_key,meta_value) values ('species.production_name','Jabberwocky');
# insert into meta (meta_key,meta_value) values ('species.strain','wonderland');

#The following are for the groups table
insert into meta (meta_key,meta_value) values ('species.division', 'Ensembl_TEST');
insert into meta (meta_key,meta_value) values ('species.strain', 'reference');
insert into meta (meta_key,meta_value) values ('species.strain_group', 'testing');
insert into meta (meta_key,meta_value) values ('species.type', 'monsters');

#Following for assembly tables
CREATE TABLE `coord_system` (
  `coord_system_id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `species_id` int(10) unsigned NOT NULL DEFAULT 1,
  `name` varchar(40) NOT NULL,
  `version` varchar(255) DEFAULT NULL,
  `rank` int(11) NOT NULL,
  `attrib` set('default_version','sequence_level') DEFAULT NULL,
  PRIMARY KEY (`coord_system_id`),
  UNIQUE KEY `rank_idx` (`rank`,`species_id`),
  UNIQUE KEY `name_idx` (`name`,`version`,`species_id`),
  KEY `species_idx` (`species_id`)
);
insert into coord_system values(1,1,'primary_assembly','test',1,'default_version,sequence_level');
insert into meta (meta_key,meta_value) values ('assembly.ucsc_alias', 'SCARY');
insert into meta (meta_key,meta_value) values ('assembly.accession', 'weird01');
insert into meta (meta_key,meta_value) values ('assembly.name', 'jaber01');
insert into meta (meta_key,meta_value) values ('assembly.default', 'jaber01');


#Add in the whole complicated senquences.
CREATE TABLE `seq_region` (
  `seq_region_id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(255) NOT NULL,
  `coord_system_id` int(10) unsigned NOT NULL,
  `length` int(10) unsigned NOT NULL,
  PRIMARY KEY (`seq_region_id`),
  UNIQUE KEY `name_cs_idx` (`name`,`coord_system_id`),
  KEY `cs_idx` (`coord_system_id`)
);
insert into seq_region values(1,'TEST1_seq',1,666666);
insert into seq_region values(2,'TEST2_seq',1,666);
insert into seq_region values(3,'TEST3_seq',1,1666666);

CREATE TABLE `seq_region_synonym` (
  `seq_region_synonym_id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `seq_region_id` int(10) unsigned NOT NULL,
  `synonym` varchar(250) NOT NULL,
  `external_db_id` int(10) unsigned DEFAULT NULL,
  PRIMARY KEY (`seq_region_synonym_id`),
  UNIQUE KEY `syn_idx` (`synonym`,`seq_region_id`),
  KEY `seq_region_idx` (`seq_region_id`)
);

insert into seq_region_synonym values(1,1,'TEST1_seq',50710);
insert into seq_region_synonym values(2,2,'TEST2_seq',50710);
insert into seq_region_synonym values(3,3,'TEST3_seq',50710);

CREATE TABLE `seq_region_attrib` (
  `seq_region_id` int(10) unsigned NOT NULL DEFAULT 0,
  `attrib_type_id` smallint(5) unsigned NOT NULL DEFAULT 0,
  `value` text NOT NULL,
  UNIQUE KEY `region_attribx` (`seq_region_id`,`attrib_type_id`,`value`(500)),
  KEY `type_val_idx` (`attrib_type_id`,`value`(40)),
  KEY `val_only_idx` (`value`(40)),
  KEY `seq_region_idx` (`seq_region_id`)
);

insert into seq_region_attrib values(1,6,1);
insert into seq_region_attrib values(2,6,1);
insert into seq_region_attrib values(3,6,1);
insert into seq_region_attrib values(1,547,'nuclear_chromosome');
insert into seq_region_attrib values(2,547,'nuclear_chromosome');
insert into seq_region_attrib values(3,547,'mitochondrial_chromosome');

#Genbuild data
insert into meta (meta_key,meta_value) values ('gencode.version', '999');
insert into meta (meta_key,meta_value) values ('genebuild.id', '01');
################################################################
############## Section for organism update       ###############
################################################################
#Values changed:
#insert into meta (meta_key,meta_value) values ('species.scientific_name','carol_jabberwocky');

create database test_core_2;
use test_core_2;
CREATE TABLE `meta` (
  `meta_id` int(11) NOT NULL AUTO_INCREMENT,
  `species_id` int(10) unsigned DEFAULT 1,
  `meta_key` varchar(40) NOT NULL,
  `meta_value` varchar(255) NOT NULL,
  PRIMARY KEY (`meta_id`),
  UNIQUE KEY `species_key_value_idx` (`species_id`,`meta_key`,`meta_value`),
  KEY `species_value_idx` (`species_id`,`meta_value`)
);
#The main data for the organism table
insert into meta (meta_key,meta_value) values ('species.species_taxonomy_id', 6666666);
insert into meta (meta_key,meta_value) values ('species.taxonomy_id', 66666668);
insert into meta (meta_key,meta_value) values ('species.display_name','jabberwocky');
insert into meta (meta_key,meta_value) values ('species.scientific_name','lewis_carol');
insert into meta (meta_key,meta_value) values ('species.url','Jabbe');
insert into meta (meta_key,meta_value) values ('species.production_name','Jabberwocky');
# insert into meta (meta_key,meta_value) values ('species.strain','wonderland');

#The following are for the groups table
insert into meta (meta_key,meta_value) values ('species.division', 'Ensembl_TEST');
insert into meta (meta_key,meta_value) values ('species.strain', 'reference');
insert into meta (meta_key,meta_value) values ('species.strain_group', 'testing');
insert into meta (meta_key,meta_value) values ('species.type', 'monsters');

#Following for assembly tables
CREATE TABLE `coord_system` (
  `coord_system_id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `species_id` int(10) unsigned NOT NULL DEFAULT 1,
  `name` varchar(40) NOT NULL,
  `version` varchar(255) DEFAULT NULL,
  `rank` int(11) NOT NULL,
  `attrib` set('default_version','sequence_level') DEFAULT NULL,
  PRIMARY KEY (`coord_system_id`),
  UNIQUE KEY `rank_idx` (`rank`,`species_id`),
  UNIQUE KEY `name_idx` (`name`,`version`,`species_id`),
  KEY `species_idx` (`species_id`)
);
insert into coord_system values(1,1,'primary_assembly','test',1,'default_version,sequence_level');
insert into meta (meta_key,meta_value) values ('assembly.ucsc_alias', 'SCARY');
insert into meta (meta_key,meta_value) values ('assembly.accession', 'weird01');
insert into meta (meta_key,meta_value) values ('assembly.name', 'jaber01');
insert into meta (meta_key,meta_value) values ('assembly.default', 'jaber01');


#Add in the whole complicated senquences.
CREATE TABLE `seq_region` (
  `seq_region_id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(255) NOT NULL,
  `coord_system_id` int(10) unsigned NOT NULL,
  `length` int(10) unsigned NOT NULL,
  PRIMARY KEY (`seq_region_id`),
  UNIQUE KEY `name_cs_idx` (`name`,`coord_system_id`),
  KEY `cs_idx` (`coord_system_id`)
);
insert into seq_region values(1,'TEST1_seq',1,666666);
insert into seq_region values(2,'TEST2_seq',1,666);
insert into seq_region values(3,'TEST3_seq',1,1666666);

CREATE TABLE `seq_region_synonym` (
  `seq_region_synonym_id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `seq_region_id` int(10) unsigned NOT NULL,
  `synonym` varchar(250) NOT NULL,
  `external_db_id` int(10) unsigned DEFAULT NULL,
  PRIMARY KEY (`seq_region_synonym_id`),
  UNIQUE KEY `syn_idx` (`synonym`,`seq_region_id`),
  KEY `seq_region_idx` (`seq_region_id`)
);

insert into seq_region_synonym values(1,1,'TEST1_seq',50710);
insert into seq_region_synonym values(2,2,'TEST2_seq',50710);
insert into seq_region_synonym values(3,3,'TEST3_seq',50710);

CREATE TABLE `seq_region_attrib` (
  `seq_region_id` int(10) unsigned NOT NULL DEFAULT 0,
  `attrib_type_id` smallint(5) unsigned NOT NULL DEFAULT 0,
  `value` text NOT NULL,
  UNIQUE KEY `region_attribx` (`seq_region_id`,`attrib_type_id`,`value`(500)),
  KEY `type_val_idx` (`attrib_type_id`,`value`(40)),
  KEY `val_only_idx` (`value`(40)),
  KEY `seq_region_idx` (`seq_region_id`)
);

insert into seq_region_attrib values(1,6,1);
insert into seq_region_attrib values(2,6,1);
insert into seq_region_attrib values(3,6,1);
insert into seq_region_attrib values(1,547,'nuclear_chromosome');
insert into seq_region_attrib values(2,547,'nuclear_chromosome');
insert into seq_region_attrib values(3,547,'mitochondrial_chromosome');

#Genbuild data
insert into meta (meta_key,meta_value) values ('gencode.version', '999');
insert into meta (meta_key,meta_value) values ('genebuild.id', '01');
################################################################
############## Section for assembly update       ###############
################################################################
#Values changed:
# insert into meta (meta_key,meta_value) values ('assembly.ucsc_alias', 'SCARY');
# insert into meta (meta_key,meta_value) values ('assembly.accession', 'weird01');
# insert into meta (meta_key,meta_value) values ('assembly.name', 'jaber01');
# insert into meta (meta_key,meta_value) values ('assembly.default', 'jaber01');
create database test_core_3;
use test_core_3;
CREATE TABLE `meta` (
  `meta_id` int(11) NOT NULL AUTO_INCREMENT,
  `species_id` int(10) unsigned DEFAULT 1,
  `meta_key` varchar(40) NOT NULL,
  `meta_value` varchar(255) NOT NULL,
  PRIMARY KEY (`meta_id`),
  UNIQUE KEY `species_key_value_idx` (`species_id`,`meta_key`,`meta_value`),
  KEY `species_value_idx` (`species_id`,`meta_value`)
);
#The main data for the organism table
insert into meta (meta_key,meta_value) values ('species.species_taxonomy_id', 6666666);
insert into meta (meta_key,meta_value) values ('species.taxonomy_id', 66666668);
insert into meta (meta_key,meta_value) values ('species.display_name','jabberwocky');
insert into meta (meta_key,meta_value) values ('species.scientific_name','lewis_carol');
insert into meta (meta_key,meta_value) values ('species.url','Jabbe');
insert into meta (meta_key,meta_value) values ('species.production_name','Jabberwocky');
# insert into meta (meta_key,meta_value) values ('species.strain','wonderland');

#The following are for the groups table
insert into meta (meta_key,meta_value) values ('species.division', 'Ensembl_TEST');
insert into meta (meta_key,meta_value) values ('species.strain', 'reference');
insert into meta (meta_key,meta_value) values ('species.strain_group', 'testing');
insert into meta (meta_key,meta_value) values ('species.type', 'monsters');

#Following for assembly tables
CREATE TABLE `coord_system` (
  `coord_system_id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `species_id` int(10) unsigned NOT NULL DEFAULT 1,
  `name` varchar(40) NOT NULL,
  `version` varchar(255) DEFAULT NULL,
  `rank` int(11) NOT NULL,
  `attrib` set('default_version','sequence_level') DEFAULT NULL,
  PRIMARY KEY (`coord_system_id`),
  UNIQUE KEY `rank_idx` (`rank`,`species_id`),
  UNIQUE KEY `name_idx` (`name`,`version`,`species_id`),
  KEY `species_idx` (`species_id`)
);
insert into coord_system values(1,1,'primary_assembly','test',1,'default_version,sequence_level');
insert into meta (meta_key,meta_value) values ('assembly.ucsc_alias', 'SCARYIER');
insert into meta (meta_key,meta_value) values ('assembly.accession', 'weird02');
insert into meta (meta_key,meta_value) values ('assembly.name', 'jaber02');
# insert into meta (meta_key,meta_value) values ('assembly.default', 'jaber02');


#Add in the whole complicated senquences.
CREATE TABLE `seq_region` (
  `seq_region_id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(255) NOT NULL,
  `coord_system_id` int(10) unsigned NOT NULL,
  `length` int(10) unsigned NOT NULL,
  PRIMARY KEY (`seq_region_id`),
  UNIQUE KEY `name_cs_idx` (`name`,`coord_system_id`),
  KEY `cs_idx` (`coord_system_id`)
);
insert into seq_region values(1,'TEST1_seq',1,666666);
insert into seq_region values(2,'TEST2_seq',1,666);
insert into seq_region values(3,'TEST3_seq',1,1666666);

CREATE TABLE `seq_region_synonym` (
  `seq_region_synonym_id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `seq_region_id` int(10) unsigned NOT NULL,
  `synonym` varchar(250) NOT NULL,
  `external_db_id` int(10) unsigned DEFAULT NULL,
  PRIMARY KEY (`seq_region_synonym_id`),
  UNIQUE KEY `syn_idx` (`synonym`,`seq_region_id`),
  KEY `seq_region_idx` (`seq_region_id`)
);

insert into seq_region_synonym values(1,1,'TEST1_seq',50710);
insert into seq_region_synonym values(2,2,'TEST2_seq',50710);
insert into seq_region_synonym values(3,3,'TEST3_seq',50710);

CREATE TABLE `seq_region_attrib` (
  `seq_region_id` int(10) unsigned NOT NULL DEFAULT 0,
  `attrib_type_id` smallint(5) unsigned NOT NULL DEFAULT 0,
  `value` text NOT NULL,
  UNIQUE KEY `region_attribx` (`seq_region_id`,`attrib_type_id`,`value`(500)),
  KEY `type_val_idx` (`attrib_type_id`,`value`(40)),
  KEY `val_only_idx` (`value`(40)),
  KEY `seq_region_idx` (`seq_region_id`)
);

insert into seq_region_attrib values(1,6,1);
insert into seq_region_attrib values(2,6,1);
insert into seq_region_attrib values(3,6,1);
insert into seq_region_attrib values(1,547,'nuclear_chromosome');
insert into seq_region_attrib values(2,547,'nuclear_chromosome');
insert into seq_region_attrib values(3,547,'mitochondrial_chromosome');

#Genbuild data
insert into meta (meta_key,meta_value) values ('gencode.version', '999');
insert into meta (meta_key,meta_value) values ('genebuild.id', '01');

################################################################
############## Section for gencode update       ###############
################################################################
#Values changed:
# insert into meta (meta_key,meta_value) values ('gencode.version', '999');
# insert into meta (meta_key,meta_value) values ('genebuild.id', '01');
create database test_core_4;
use test_core_4;
CREATE TABLE `meta` (
  `meta_id` int(11) NOT NULL AUTO_INCREMENT,
  `species_id` int(10) unsigned DEFAULT 1,
  `meta_key` varchar(40) NOT NULL,
  `meta_value` varchar(255) NOT NULL,
  PRIMARY KEY (`meta_id`),
  UNIQUE KEY `species_key_value_idx` (`species_id`,`meta_key`,`meta_value`),
  KEY `species_value_idx` (`species_id`,`meta_value`)
);
#The main data for the organism table
insert into meta (meta_key,meta_value) values ('species.species_taxonomy_id', 6666666);
insert into meta (meta_key,meta_value) values ('species.taxonomy_id', 66666668);
insert into meta (meta_key,meta_value) values ('species.display_name','jabberwocky');
insert into meta (meta_key,meta_value) values ('species.scientific_name','lewis_carol');
insert into meta (meta_key,meta_value) values ('species.url','Jabbe');
insert into meta (meta_key,meta_value) values ('species.production_name','Jabberwocky');
# insert into meta (meta_key,meta_value) values ('species.strain','wonderland');

#The following are for the groups table
insert into meta (meta_key,meta_value) values ('species.division', 'Ensembl_TEST');
insert into meta (meta_key,meta_value) values ('species.strain', 'reference');
insert into meta (meta_key,meta_value) values ('species.strain_group', 'testing');
insert into meta (meta_key,meta_value) values ('species.type', 'monsters');

#Following for assembly tables
CREATE TABLE `coord_system` (
  `coord_system_id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `species_id` int(10) unsigned NOT NULL DEFAULT 1,
  `name` varchar(40) NOT NULL,
  `version` varchar(255) DEFAULT NULL,
  `rank` int(11) NOT NULL,
  `attrib` set('default_version','sequence_level') DEFAULT NULL,
  PRIMARY KEY (`coord_system_id`),
  UNIQUE KEY `rank_idx` (`rank`,`species_id`),
  UNIQUE KEY `name_idx` (`name`,`version`,`species_id`),
  KEY `species_idx` (`species_id`)
);
insert into coord_system values(1,1,'primary_assembly','test',1,'default_version,sequence_level');
insert into meta (meta_key,meta_value) values ('assembly.ucsc_alias', 'SCARYIER');
insert into meta (meta_key,meta_value) values ('assembly.accession', 'weird02');
insert into meta (meta_key,meta_value) values ('assembly.name', 'jaber01');
insert into meta (meta_key,meta_value) values ('assembly.default', 'jaber01');


#Add in the whole complicated senquences.
CREATE TABLE `seq_region` (
  `seq_region_id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(255) NOT NULL,
  `coord_system_id` int(10) unsigned NOT NULL,
  `length` int(10) unsigned NOT NULL,
  PRIMARY KEY (`seq_region_id`),
  UNIQUE KEY `name_cs_idx` (`name`,`coord_system_id`),
  KEY `cs_idx` (`coord_system_id`)
);
insert into seq_region values(1,'TEST1_seq',1,666666);
insert into seq_region values(2,'TEST2_seq',1,666);
insert into seq_region values(3,'TEST3_seq',1,1666666);

CREATE TABLE `seq_region_synonym` (
  `seq_region_synonym_id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `seq_region_id` int(10) unsigned NOT NULL,
  `synonym` varchar(250) NOT NULL,
  `external_db_id` int(10) unsigned DEFAULT NULL,
  PRIMARY KEY (`seq_region_synonym_id`),
  UNIQUE KEY `syn_idx` (`synonym`,`seq_region_id`),
  KEY `seq_region_idx` (`seq_region_id`)
);

insert into seq_region_synonym values(1,1,'TEST1_seq',50710);
insert into seq_region_synonym values(2,2,'TEST2_seq',50710);
insert into seq_region_synonym values(3,3,'TEST3_seq',50710);

CREATE TABLE `seq_region_attrib` (
  `seq_region_id` int(10) unsigned NOT NULL DEFAULT 0,
  `attrib_type_id` smallint(5) unsigned NOT NULL DEFAULT 0,
  `value` text NOT NULL,
  UNIQUE KEY `region_attribx` (`seq_region_id`,`attrib_type_id`,`value`(500)),
  KEY `type_val_idx` (`attrib_type_id`,`value`(40)),
  KEY `val_only_idx` (`value`(40)),
  KEY `seq_region_idx` (`seq_region_id`)
);

insert into seq_region_attrib values(1,6,1);
insert into seq_region_attrib values(2,6,1);
insert into seq_region_attrib values(3,6,1);
insert into seq_region_attrib values(1,547,'nuclear_chromosome');
insert into seq_region_attrib values(2,547,'nuclear_chromosome');
insert into seq_region_attrib values(3,547,'mitochondrial_chromosome');

#Genbuild data
insert into meta (meta_key,meta_value) values ('gencode.version', '999');
insert into meta (meta_key,meta_value) values ('genebuild.id', '02');