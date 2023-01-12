create database test_core;
use test_core;
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
insert into meta (meta_key,meta_value) values ('species.strain','wonderland');
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
insert into coord_system values(1,1,'primary_assembly','test',1,'default_version,sequence_level')
insert into meta (meta_key,meta_value) values ('assembly.ucsc_alias', 'SCARY');
insert into meta (meta_key,meta_value) values ('assembly.accession', 'weird01');
insert into meta (meta_key,meta_value) values ('assembly.name', 'jaber01');
insert into meta (meta_key,meta_value) values ('assembly.default', 'jaber01');


#Add in the whole complicated senquences.
