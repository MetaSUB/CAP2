
from ..utils.cap_task import CapTask

from .amrs import GrootAMR
from .hmp_comparison import HmpComparison
from .humann2 import Humann2
from .kraken2 import Kraken2
from .mash import Mash
from .read_stats import ReadStats


class ProcessedReads(CapTask):
    """This class represents the culmination of the
    shortread pipeline.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @property
    def reads(self):
        return self.ec_reads

    @classmethod
    def version(cls):
        return 'v0.1.0'

    @classmethod
    def dependencies(cls):
        return [GrootAMR, HmpComparison, Humann2, Kraken2, Mash, ReadStats]

    @classmethod
    def _module_name(cls):
        return 'processed_reads'

    def requires(self):
        return self.ec_reads

    def output(self):
        return {}

    def _run(self):
        pass
