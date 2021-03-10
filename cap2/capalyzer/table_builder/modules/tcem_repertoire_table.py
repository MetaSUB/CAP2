
import pandas as pd
import logging
from ..base_module import BaseModule
from ..parsers import parse_taxa_report

logger = logging.getLogger(__name__)  # Same name as calling module


class TcemRepertoireTableModule(BaseModule):
    result_name = 'tcem_repertoire'
    field_name = 'tcem_counts'
    source_result_name = 'cap2::tcems::tcem_repertoire'

    @classmethod
    def build_result(cls, file_source):
        tbls = []
        for i, (sample_name, report_path) in enumerate(file_source(cls.source_result_name, cls.field_name)):
            try:
                tbl = pd.read_csv(report_path, index_col=(0, 1))
                tbl = pd.concat({sample_name: tbl}, names=['sample_name'])
                tbls.append(tbl)
                logger.debug(f'[TCEMRepertoire] Parsed {sample_name} ({i + 1})')
            except Exception as e:
                logger.debug(f'[TCEMRepertoire] failed to parse "{sample_name}". Exception: {e}')
        tbl = pd.concat(tbls)
        return tbl, len(tbls)
