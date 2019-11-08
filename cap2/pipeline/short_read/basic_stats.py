import luigi
import subprocess
from os.path import join, dirname, basename

from ..config import PipelineConfig
from ..utils.conda import CondaPackage
from ..preprocessing.clean_reads import CleanReads


class BasicReadStats(luigi.Task):
    sample_name = luigi.Parameter()
    pe1 = luigi.Parameter()
    pe2 = luigi.Parameter()
    config_filename = luigi.Parameter()
    cores = luigi.IntParameter(default=1)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.config = PipelineConfig(self.config_filename)
        self.reads = CleanReads(
            sample_name=sample_name, pe1=pe1, pe2=pe2, config_filename=config_filename
        )

    def requires(self):
        return self.reads

    def output(self):
        stats = luigi.LocalTarget(
            join(self.out_dir, f'{self.sample_name}.basic_stats.stats.yaml')
        )
        stats.makedirs()
        return {'stats': stats}

    def run(self):
        pass
