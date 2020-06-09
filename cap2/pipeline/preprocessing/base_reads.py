
import luigi

from ..utils.cap_task import CapTask


class BaseReads(CapTask):
    """This class represents the start of the pipeline.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @classmethod
    def version(cls):
        return 'v1.0.0'

    @classmethod
    def dependencies(cls):
        return []

    @classmethod
    def _module_name(cls):
        return 'base_reads'

    def requires(self):
        return []

    def output(self):
        return {
            'base_reads_1': luigi.LocalTarget(self.pe1),
            'base_reads_2': luigi.LocalTarget(self.pe2),
        }

    def _run(self):
        pass