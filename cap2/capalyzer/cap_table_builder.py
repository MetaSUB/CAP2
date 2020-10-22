import os
import pandas as pd
from .parsers import (
    parse_taxa_report,
    parse_pileup,
)


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

    def pandas_cache(self, name, func, *args, **kwargs):
        if os.path.isfile(name):
            return pd.read_csv(name, index_col=0)
        tbl = func(self, *args, **kwargs)
        tbl.to_csv(name)
        return tbl

    def taxa_read_counts(self, *args, **kwargs):
        return self.pandas_cache(
            f'{self.name}__taxa_read_counts.csv',
            self._taxa_read_counts,
            *args,
            **kwargs,
        )

    def _taxa_read_counts(self, *args, **kwargs):
        logger = kwargs.get('logger', lambda i, x: None)
        taxa = {}
        for i, (sample_name, report_path) in enumerate(self.file_source('cap2::kraken2', 'report')):
            taxa[sample_name] = parse_taxa_report(report_path)
            logger(i + 1, sample_name)
        taxa = pd.DataFrame.from_dict(taxa, orient='index')
        return taxa
    
    def pileup(self, *args, **kwargs):
        organism = kwargs.get('organism')
        sparse = kwargs.get('sparse', 1)
        return self.pandas_cache(
            f'{self.name}__pileup_{organism}_sparse-{sparse}.csv',
            self._pileup,
            *args,
            **kwargs,
        )

    def _pileup(self, *args, **kwargs):
        logger = kwargs.get('logger', lambda i, x: None)
        organism = kwargs['organism'].replace(' ', '_')
        tbls = []
        for i, (sample_name, path) in enumerate(self.file_source('cap2::experimental::make_pileup', f'pileup__{organism}')):
            tbl = parse_pileup(path, sparse=kwargs.get('sparse', 1))
            tbl['sample_name'] = sample_name
            tbls.append(tbl)
            logger(i + 1, sample_name)
        tbl = pd.concat(tbls)
        return tbl
            