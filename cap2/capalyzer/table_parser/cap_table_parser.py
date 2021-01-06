import os
import logging
import pandas as pd
from .parsers import (
    parse_taxa_report,
    parse_pileup,
)

logger = logging.getLogger(__name__)  # Same name as calling module
logger.addHandler(logging.NullHandler())  # No output unless configured by calling program


class CAPTableParser:
    """This class builds summary tables for a set of samples."""

    def __init__(self, name, file_source):
        self.name = name
        self.file_source = file_source

    def metadata(self):
        """Return a metadata table for these samples."""
        return self.file_source.metadata()

    def sample_names(self):
        """Return a list of sample names (strings)."""
        return self.file_source.sample_names()

    def covid_fast_detect_read_counts(self):
        """Return a table of read counts by taxa from covid fast detect."""
        local_path = self.file_source.group_module_files('cap2::capalyzer::kraken2_covid_fast_detect', 'read_counts')
        taxa = pd.read_csv(local_path, index_col=0)
        logger.info(f'[CovidFastReadCounts] Found cached read count table')
        return taxa

    def taxa_read_counts(self):
        """Return a table of read counts by taxa."""
        local_path = self.file_source.group_module_files('cap2::capalyzer::kraken2_taxa', 'read_counts')
        taxa = pd.read_csv(local_path, index_col=0)
        logger.info(f'[TaxaReadCounts] Found cached read count table')
        return taxa
