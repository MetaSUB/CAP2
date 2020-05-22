
import luigi
import pandas as pd
from random import random
from os.path import join, dirname, basename
from Bio import SeqIO
from gzip import open as gopen

from ..utils.cap_task import CapTask
from ..config import PipelineConfig
from ..preprocessing.clean_reads import CleanReads


def stats_one_fastq(fastq, dropout):
    read_count = 0
    gc_count, n_count, base_count = 0, 0, 0
    seq_length = 0
    with gopen(fastq) as f:
        for i, line in enumerate(f):
            if i % 4 != 1:
                continue
            seq = line.strip()
            read_count += 1
            seq_length += len(seq)
            if random() > dropout:
                continue
            for base in seq:
                if base in b'GCgc':
                    gc_count += 1
                elif base not in b'ATUatu':
                    n_count += 1
                base_count += 1
    gc_frac = gc_count / base_count
    n_frac = n_count / base_count
    seq_length /= read_count
    return {
        'read_count': read_count,
        'gc_fraction': gc_frac,
        'n_fraction': n_frac,
        'mean_seq_length': seq_length,
    }


class ReadStats(CapTask):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.config = PipelineConfig(self.config_filename)
        self.out_dir = self.config.out_dir
        self.reads = CleanReads(
            sample_name=self.sample_name,
            pe1=self.pe1,
            pe2=self.pe2,
            config_filename=self.config_filename,
        )
        self.dropout = 1 / 1000

    @classmethod
    def _module_name(cls):
        return 'read_stats'

    def requires(self):
        return self.reads

    @classmethod
    def version(cls):
        return 'v1.0.0'

    @classmethod
    def dependencies(cls):
        return [CleanReads]

    def output(self):
        return {
            'report': self.get_target('report', 'csv'),
        }

    def _run(self):
        tbl = pd.DataFrame.from_dict({
            (self.sample_name, 'read_1'): stats_one_fastq(self.reads.reads[0], self.dropout),
            (self.sample_name, 'read_2'): stats_one_fastq(self.reads.reads[1], self.dropout),
        }, orient='index')
        tbl.to_csv(self.output()['report'].path)
