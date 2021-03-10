
import luigi

from ..utils.cap_task import CapTask


class BaseReads(CapTask):
    """This class represents the start of the pipeline.
    """
    MODULE_VERSION = 'v1.0.0'
    module_description = """
    This module contains paired end short reads.

    It is the start of the pipeline.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @classmethod
    def dependencies(cls):
        return []

    @classmethod
    def _module_name(cls):
        return 'base_reads'

    def requires(self):
        return []

    @property
    def read_1(self):
        return self.output()["base_reads_1"].path

    @property
    def read_2(self):
        return self.output()["base_reads_2"].path

    def output(self):
        out = {'base_reads_1': luigi.LocalTarget(self.pe1)}
        if self.paired:
            out['base_reads_2'] = luigi.LocalTarget(self.pe2)
        return out

    def _run(self):
        pass
