
import pandas as pd
import json
import logging
from ..base_module import BaseModule
from ..parsers import parse_taxa_report

logger = logging.getLogger(__name__)  # Same name as calling module


def flatten_dict(d):
    for k, v in d.items():
        if isinstance(v, dict):
            for sub_k, sub_v in flatten_dict(v):
                yield [k] + sub_k, sub_v
        else:
            yield [k], v


class BasicStatsTableModule(BaseModule):
    result_name = 'basic_stats_table'
    field_name = 'report'
    source_result_name = 'cap2::basic_sample_stats'

    @classmethod
    def build_result(cls, file_source):
        tbl = {}
        fs = file_source(cls.source_result_name, cls.field_name)
        for i, (sample_name, report_path) in enumerate(fs):
            try:
                report = json.loads(open(report_path).read())
                tbl[sample_name] = {tuple(k): v for k, v in flatten_dict(report)}
                logger.debug(f'[BasicStats] Parsed {sample_name} ({i + 1})')
            except Exception as e:
                logger.debug(f'[BasicStats] failed to parse "{sample_name}". Exception: {e}')
        tbl = pd.DataFrame.from_dict(tbl, orient='index')
        return tbl, tbl.shape[0]
