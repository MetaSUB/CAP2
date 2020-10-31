import os
import logging
import pandas as pd
from .parsers import (
    parse_taxa_report,
    parse_pileup,
)

logger = logging.getLogger(__name__)  # Same name as calling module
logger.addHandler(logging.NullHandler())  # No output unless configured by calling program


class CAPFileSource:
    """This abstract class provides an interface to get
    filepaths and other raw data.
    """

    def metadata(self):
        """Return a DataFrame containing metadata for the samples."""
        raise NotImplementedError()

    def sample_names(self):
        """Return a list of sample names (strings)."""
        raise NotImplementedError()

    def module_files(self, module_name, field_name):
        """Return an iterable 2-ples of (sample_name, local_path) for modules of specified type."""
        raise NotImplementedError()

    def __call__(self, module_name, field_name):
        return self.module_files(module_name, field_name)


class CAPTableBuilder:
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

    def taxa_read_counts(self):
        """Return a table of read counts by taxa."""
        taxa = {}
        for i, (sample_name, report_path) in enumerate(self.file_source('cap2::kraken2', 'report')):
            taxa[sample_name] = parse_taxa_report(report_path)
            logger.info(f'[TaxaReadCounts] Parsed {sample_name} ({i + 1})')
        taxa = pd.DataFrame.from_dict(taxa, orient='index')
        return taxa

    def strain_pileup(self, organism, sparse=1):
        """Return a table of pileups for a strain."""
        organism = organism.replace(' ', '_')
        tbls = []
        for i, (sample_name, path) in enumerate(self.file_source('cap2::experimental::make_pileup', f'pileup__{organism}')):
            tbl = parse_pileup(path, sparse=sparse)
            tbl['sample_name'] = sample_name
            tbls.append(tbl)
            logger.info(f'[StrainPileup] Parsed {sample_name} for {organism} ({i + 1})')
        tbl = pd.concat(tbls)
        return tbl
