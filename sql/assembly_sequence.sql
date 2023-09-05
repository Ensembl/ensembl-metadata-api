-- Because the attrib_type.txt and external_db tables are identical
-- across all dbs, and in sync with the production master copy,
-- we can use IDs directly, and avoid some complicated outer
-- join statements...
-- external_db.external_db_id 50710 = INSDC
-- attrib_type.txt.attrib_type_id 6 = toplevel
-- attrib_type.txt.attrib_type_id 367 = karyotype_rank
-- attrib_type.txt.attrib_type_id 547 = sequence_location

-- Unfortunately, the sequence_location attribute in the core dbs
-- isn't set with the values you might expect; it has '*_chromosome'
-- SO terms, rather than '*_location' ones, but we do not want to
-- only record this information for chromosomes... There is only
-- data for three different terms in the current set of dbs, so
-- mapping is fairly easy; if the attribute is not set, we assume
-- nuclear_sequence.
-- chloroplast_chromosome (SO:0000820) => chloroplast_sequence (SO:0000745)
-- mitochondrial_chromosome (SO:0000819) => mitochondrial_sequence (SO:0000737)
-- nuclear_chromosome (SO:0000828) => nuclear_sequence (SO:0000738)

select
  m.meta_value as assembly_accession,
  if(srs.synonym REGEXP '[[:alpha:]]+[[:digit:]]+[.][[:digit:]]+',
    srs.synonym,
    if(sr.name REGEXP '[[:alpha:]]+[[:digit:]]+[.][[:digit:]]+',
      sr.name,
      NULL
    )
  ) as accession,
  if(sr.name NOT REGEXP '[[:alpha:]]+[[:digit:]]+[.][[:digit:]]+',
    sr.name,
    NULL
  ) as name,
  sr.length,
  if(isnull(sra2.value),
    if(isnull(sra3.value),
      0,
      if(sra3.value <> 'nuclear_chromosome',
          1,
          0
      )
    ),
    1
  ) as chromosomal,
  if(isnull(sra3.value),
    'SO:0000738',
    if(sra3.value = 'chloroplast_chromosome',
      'SO:0000745',
      if(sra3.value = 'mitochondrial_chromosome',
        'SO:0000737',
        if(sra3.value = 'nuclear_chromosome',
          'SO:0000738',
          NULL
        )
      )
    )
  ) as sequence_location,
  md5(sequence) as sequence_checksum,
  NULL as ga4gh_identifier
from
  meta m inner join
  coord_system cs using (species_id) inner join
  seq_region sr using (coord_system_id) inner join
  seq_region_attrib sra1 on sr.seq_region_id = sra1.seq_region_id left outer join
  seq_region_synonym srs on (
    sr.seq_region_id = srs.seq_region_id and
    srs.external_db_id = 50710
    ) left outer join
  seq_region_attrib sra2 on (
    sr.seq_region_id = sra2.seq_region_id and
    sra2.attrib_type_id = 367
    ) left outer join
  seq_region_attrib sra3 on (
    sr.seq_region_id = sra3.seq_region_id and
    sra3.attrib_type_id = 547
    ) left outer join
  dna on sr.seq_region_id = dna.seq_region_id
where
  m.meta_key = 'assembly.accession' and
  sra1.attrib_type_id = 6;
