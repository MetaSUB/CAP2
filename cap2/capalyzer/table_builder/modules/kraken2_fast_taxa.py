
import pandas as pd
import logging
from ..base_module import BaseModule
from ..parsers import parse_taxa_report

logger = logging.getLogger(__name__)  # Same name as calling module


class FastKraken2TableModule(BaseModule):
    result_name = 'fast_kraken2_taxa'
    field_name = 'report'
    source_result_name = 'cap2::fast_kraken2'

    @classmethod
    def build_result(cls, file_source):
        taxa = {}
        fs = file_source(cls.source_result_name, cls.field_name)
        for i, (sample_name, report_path) in enumerate(fs):
            try:
                taxa[sample_name] = parse_taxa_report(
                    report_path,
                    normalize=True,
                    minimum_abundance=0.001,
                    species_only=True,
                )
                logger.debug(f'[FastTaxa] Parsed {sample_name} ({i + 1})')
            except Exception as e:
                logger.debug(f'[FastTaxa] failed to parse "{sample_name}". Exception: {e}')
        taxa = pd.DataFrame.from_dict(taxa, orient='index')
        return taxa, taxa.shape[0]
