CREATE TABLE genome_db (
  genome_db_id                INT unsigned NOT NULL AUTO_INCREMENT, # unique internal id
  taxon_id                    INT unsigned DEFAULT NULL, # KF taxon.taxon_id
  name                        varchar(128) DEFAULT '' NOT NULL,
  assembly                    varchar(100) DEFAULT '' NOT NULL,
  genebuild                   varchar(255) DEFAULT '' NOT NULL,
  has_karyotype			tinyint(1) NOT NULL DEFAULT 0,
  is_good_for_alignment       TINYINT(1) NOT NULL DEFAULT 0,
  genome_component            varchar(5) DEFAULT NULL,
  strain_name                 varchar(100) DEFAULT NULL,
  display_name                varchar(255) DEFAULT NULL,
  locator                     varchar(400),
  first_release               smallint,
  last_release                smallint,

  PRIMARY KEY (genome_db_id),
  UNIQUE KEY name (name,assembly,genome_component)

) COLLATE=latin1_swedish_ci ENGINE=MyISAM;