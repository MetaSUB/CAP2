
import luigi
import pandas as pd
from random import random
from os.path import join, dirname, basename
from Bio import SeqIO

from ..utils.cap_task import CapTask
from ..utils.utils import stats_one_fastq
from ..config import PipelineConfig
from ..preprocessing.clean_reads import CleanReads


class ReadStats(CapTask):
    module_description = """
    This module calculates various read statistics from samples.

    Motivation: Read statistics can help uncover basic patterns.
    """
    MODULE_VERSION = 'v1.0.1'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.config = PipelineConfig(self.config_filename)
        self.out_dir = self.config.out_dir
        self.reads = CleanReads.from_cap_task(self)
        self.dropout = 1 / 1000

    @classmethod
    def _module_name(cls):
        return 'read_stats'

    def requires(self):
        return self.reads

    @classmethod
    def dependencies(cls):
        return [CleanReads]

    def output(self):
        return {
            'report': self.get_target('report', 'csv'),
        }

    def _run(self):
        tbl = pd.DataFrame.from_dict({
            (self.sample_name, 'read_1'): stats_one_fastq(self.reads.output()['clean_reads_1'].path, self.dropout),
            (self.sample_name, 'read_2'): stats_one_fastq(self.reads.output()['clean_reads_2'].path, self.dropout),
        }, orient='index')
        tbl.to_csv(self.output()['report'].path)
