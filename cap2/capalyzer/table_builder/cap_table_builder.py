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

    def module_files(self, module_name, field_name):
        """Return an iterable 2-ples of (sample_name, local_path) for modules of specified type."""
        raise NotImplementedError()

    def __call__(self, module_name, field_name):
        return self.module_files(module_name, field_name)


class CAPTableBuilder:

    def __init__(self, name, file_source):
        self.name = name
        self.file_source = file_source

    def metadata(self):
        return self.file_source.metadata()

    def sample_names(self):
        return self.file_source.sample_names()

    def taxa_read_counts(self):
        taxa = {}
        for i, (sample_name, report_path) in enumerate(self.file_source('cap2::kraken2', 'report')):
            taxa[sample_name] = parse_taxa_report(report_path)
            logger.info(f'[TaxaReadCounts] Parsed {sample_name} ({i + 1})')
        taxa = pd.DataFrame.from_dict(taxa, orient='index')
        return taxa

    def strain_pileup(self, organism, sparse=1):
        organism = organism.replace(' ', '_')
        tbls = []
        for i, (sample_name, path) in enumerate(self.file_source('cap2::experimental::make_pileup', f'pileup__{organism}')):
            tbl = parse_pileup(path, sparse=sparse)
            tbl['sample_name'] = sample_name
            tbls.append(tbl)
            logger.info(f'[StrainPileup] Parsed {sample_name} for {organism} ({i + 1})')
        tbl = pd.concat(tbls)
        return tbl
