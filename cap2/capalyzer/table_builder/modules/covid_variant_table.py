
import pandas as pd
import logging
from ..base_module import BaseModule
from ..parsers import parse_taxa_report

logger = logging.getLogger(__name__)  # Same name as calling module


class CovidVariantTableModule(BaseModule):
    result_name = 'ivar_covid_variants_table'
    field_name = 'table'
    source_result_name = 'cap2::experimental::call_covid_variants'

    @staticmethod
    def build_result(file_source):
        tables = {}
        fs = file_source('cap2::experimental::call_covid_variants', 'tsv')
        for i, (sample_name, report_path) in enumerate(fs):
            try:
                tables[sample_name] = pd.read_csv(report_path, sep='\t')
                logger.debug(f'[CovidVariantTable] Parsed {sample_name} ({i + 1})')
            except Exception as e:
                logger.debug(f'[CovidVariantTable] failed to parse "{sample_name}". Exception: {e}')
        variant_tbl = []
        for sample_name, vtbl in tables.items():
            vtbl['sample_name'] = sample_name
            variant_tbl.append(vtbl)
        variant_tbl = pd.concat(variant_tbl)
        return variant_tbl, variant_tbl['sample_name'].nunique()
