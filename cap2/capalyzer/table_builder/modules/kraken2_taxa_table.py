
import pandas as pd
import logging
from ..base_module import BaseModule

logger = logging.getLogger(__name__)  # Same name as calling module


class Kraken2TaxaTableModule(BaseModule):
    result_name = 'kraken2_taxa'
    field_name = 'read_counts'
    source_result_name = 'cap2::kraken2'

    @staticmethod
    def build_result(file_source):
        taxa = {}
        for i, (sample_name, report_path) in enumerate(file_source('cap2::kraken2', 'report')):
            try:
                taxa[sample_name] = parse_taxa_report(report_path)
                logger.debug(f'[TaxaReadCounts] Parsed {sample_name} ({i + 1})')
            except Exception as e:
                logger.debug(f'[TaxaReadCounts] failed to parse "{sample_name}". Exception: {e}')
        taxa = pd.DataFrame.from_dict(taxa, orient='index')
        return taxa, taxa.shape[0]
