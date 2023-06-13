import re

from sqlalchemy.engine import make_url


def meta_factory(db_uri, metadata_uri=None):
    db_url = make_url(db_uri)
    if '_compara_' in db_url.database:
        raise Exception("compara not implemented yet")
    # !DP!#
    # NOT DONE#######################Worry about this after the core et al are done. Don't delete it yet.
    #     elif '_collection_' in db_url.database:
    #        self.db_type = "collection"
    ################################################################
    elif '_variation_' in db_url.database:
        raise Exception("variation not implemented yet")
    elif '_funcgen_' in db_url.database:
        raise Exception("funcgen not implemented yet")
    elif '_core_' in db_url.database:
        return CoreMetaUpdater(db_uri, metadata_uri)
    elif '_otherfeatures_' in db_url.database:
        raise Exception("otherfeatures not implemented yet")
    elif '_rnaseq_' in db_url.database:
        raise Exception("rnaseq not implemented yet")
    elif '_cdna_' in db_url.database:
        raise Exception("cdna not implemented yet")
    # Dealing with other versionned databases like mart, ontology,...
    elif re.match(r'^\w+_?\d*_\d+$', db_url.database):
        raise Exception("other not implemented yet")
    elif re.match(r'^ensembl_accounts|ensembl_archive|ensembl_autocomplete|ensembl_metadata|ensembl_production|'
                  r'ensembl_stable_ids|ncbi_taxonomy|ontology|website',
            db_url.database):
        raise Exception("other not implemented yet")
    else:
        raise "Can't find data_type for database " + db_url.database
