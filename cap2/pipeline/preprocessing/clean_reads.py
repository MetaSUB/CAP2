
import luigi
import subprocess
from os.path import join, dirname, basename

from ..config import PipelineConfig
from ..utils.conda import CondaPackage
from .error_correct_reads import ErrorCorrectReads


class CleanReads(luigi.Task):
    """This class represents the culmination of the
    preprocessing pipeline.
    """ 
    sample_name = luigi.Parameter()
    pe1 = luigi.Parameter()
    pe2 = luigi.Parameter()
    config_filename = luigi.Parameter()
    cores = luigi.IntParameter(default=1)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.config = PipelineConfig(self.config_filename)
        self.out_dir = self.config.out_dir
        self.ec_reads = ErrorCorrectReads(
            pe1=self.pe1,
            pe2=self.pe2,
            sample_name=self.sample_name,
            config_filename=self.config_filename,
        )

    @property
    def reads(self):
        return self.ec_reads

    def requires(self):
        return self.ec_reads

    def output(self):
        return {
            'clean_reads': self.ec_reads.output()['ec_reads'],
        }

    def run(self):
        pass
