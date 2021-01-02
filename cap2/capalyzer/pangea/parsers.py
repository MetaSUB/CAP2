
import pandas as pd
import ..parsers as local_parsers
from pangea_api import (
    Knex,
    User,
    Organization,
    SampleAnalysisResultField,
)


def parse_taxa_report(report: SampleAnalysisResultField) -> dict:
    """Return a dict of taxa_name to relative abundance."""
    local_path = report.download_file()
    tbl = local_parsers.parse_taxa_report(local_path)
    return tbl


def parse_pileup(pileup: SampleAnalysisResultField, sparse=1):
    """Return a pandas dataframe with info from a pileup file.
    
    `sparse` is an int >= 1 if `sparse` is > 1 values will be averaged
    making the table more smaller.
    """
    local_path = pileup.download_file()
    tbl = local_parsers.parse_pileup(local_path, sparse=sparse)
    return tbl
