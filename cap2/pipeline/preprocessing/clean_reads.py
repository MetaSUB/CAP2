
import luigi
import subprocess
from os.path import join, dirname, basename

from ..utils.cap_task import CapTask

from ..utils.conda import CondaPackage
from .error_correct_reads import ErrorCorrectReads


class CleanReads(CapTask):
    """This class represents the culmination of the
    preprocessing pipeline.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.ec_reads = ErrorCorrectReads(
            pe1=self.pe1,
            pe2=self.pe2,
            sample_name=self.sample_name,
            config_filename=self.config_filename,
        )

    @property
    def reads(self):
        return self.ec_reads

    @classmethod
    def version(cls):
        return 'v1.0.0'

    @classmethod
    def dependencies(cls):
        return [ErrorCorrectReads]

    @classmethod
    def _module_name(cls):
        return 'clean_reads'

    def requires(self):
        return self.ec_reads

    def output(self):
        return {
            'clean_reads': self.ec_reads.output()['error_corrected_reads'],
        }

    def _run(self):
        pass
