
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
    module_description = """
    This module contains cleaned paired end short reads.

    It is the end of the preprocessing stage of the pipeline.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.ec_reads = ErrorCorrectReads.from_cap_task(self)

    @property
    def reads(self):
        return self.ec_reads

    @classmethod
    def version(cls):
        return 'v0.2.1'

    @classmethod
    def dependencies(cls):
        return [ErrorCorrectReads]

    @classmethod
    def _module_name(cls):
        return 'clean_reads'

    def requires(self):
        return self.ec_reads

    def output(self):
        out = {
            'clean_reads_1': self.ec_reads.output()['error_corrected_reads_1'],
        }
        if self.paired:
            out['clean_reads_2'] = self.ec_reads.output()['error_corrected_reads_2']
        return out

    def _run(self):
        pass
