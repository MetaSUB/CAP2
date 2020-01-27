
import luigi
import subprocess
from os.path import join, dirname, basename

from ..utils.cap_task import CapTask
from ..config import PipelineConfig
from ..utils.conda import CondaPackage
from .error_correct_reads import ErrorCorrectReads


class CleanReads(CapTask):
    """This class represents the culmination of the
    preprocessing pipeline.
    """

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
            'clean_reads': self.ec_reads.output()['error_corrected_reads'],
        }

    def run(self):
        pass
