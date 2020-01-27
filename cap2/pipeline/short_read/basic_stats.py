import luigi
import subprocess
from os.path import join, dirname, basename

from ..utils.cap_task import CapTask
from ..config import PipelineConfig
from ..utils.conda import CondaPackage
from ..preprocessing.clean_reads import CleanReads


class BasicReadStats(CapTask):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.config = PipelineConfig(self.config_filename)
        self.reads = CleanReads(
            sample_name=self.sample_name,
            pe1=self.pe1,
            pe2=self.pe2,
            config_filename=self.config_filename
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
