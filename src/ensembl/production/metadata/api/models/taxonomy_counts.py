# See the NOTICE file distributed with this work for additional information
# regarding copyright ownership.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

"""Persistent release taxonomy group counts."""

from sqlalchemy import Column, Integer, String

from ensembl.production.metadata.api.models.base import Base, LoadAble

__all__ = ["GenomeTaxonomyCount"]


class GenomeTaxonomyCount(LoadAble, Base):
    """Number of genomes in an Ensembl top-level taxonomy group.

    ``release_id == 0`` holds the all-current-releases total.  Other rows
    hold the count for one released Ensembl release, matching the historical
    ``genome_taxonomy_counts`` release script output.
    """

    __tablename__ = "genome_taxonomy_counts"

    release_id = Column(Integer, primary_key=True)
    taxon_id = Column(Integer, primary_key=True)
    label = Column(String(64), nullable=False, default="")
    ensembl_taxon_name = Column(String(32), nullable=False)
    count = Column(Integer, nullable=False)
    ord = Column(Integer, nullable=False)
