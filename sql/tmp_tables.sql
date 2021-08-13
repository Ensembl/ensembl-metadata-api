create table tmp_dataset (
  type varchar(255),
  dbname varchar(255),
  ensembl_release int,
  ensembl_name varchar(255),
  name varchar(255),
  label varchar(255),
  version varchar(255)
);

create table tmp_attribute (
  dbname varchar(255),
  ensembl_release int,
  ensembl_name varchar(255),
  dataset_type varchar(255),
  dataset_name varchar(255),
  type varchar(255),
  name varchar(255),
  label varchar(255),
  value varchar(255)
);
