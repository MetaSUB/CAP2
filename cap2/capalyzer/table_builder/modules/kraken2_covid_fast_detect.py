
import pandas as pd
import logging
from ..base_module import BaseModule
from ..parsers import parse_taxa_report

logger = logging.getLogger(__name__)  # Same name as calling module


class Kraken2CovidFastDetectModule(BaseModule):
    result_name = 'kraken2_covid_fast_detect'
    field_name = 'read_counts'
    source_result_name = 'cap2::experimental::covid19_fast_detect'

    @staticmethod
    def build_result(file_source):
        taxa = {}
        fs = file_source('cap2::experimental::covid19_fast_detect', 'report')
        for i, (sample_name, report_path) in enumerate(fs):
            try:
                taxa[sample_name] = parse_taxa_report(report_path)
                logger.debug(f'[CovidFastReadCounts] Parsed {sample_name} ({i + 1})')
            except Exception as e:
                logger.debug(f'[CovidFastReadCounts] failed to parse "{sample_name}". Exception: {e}')
        taxa = pd.DataFrame.from_dict(taxa, orient='index')
        return taxa, taxa.shape[0]
