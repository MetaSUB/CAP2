
from ..utils.cap_task import CapTask

from .amrs import GrootAMR
from .hmp_comparison import HmpComparison
from .humann2 import Humann2
from .kraken2 import BrakenKraken2, Kraken2
from .mash import Mash
from .read_stats import ReadStats
from .jellyfish import Jellyfish


class ProcessedReads(CapTask):
    """This class represents the culmination of the
    shortread pipeline.
    """
    module_description = """
    This module is a proxy for the end of the short read stage.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.hmp = HmpComparison.from_cap_task(self)
        self.humann2 = Humann2.from_cap_task(self)
        self.kraken2 = BrakenKraken2.from_cap_task(self)
        self.mash = Mash.from_cap_task(self)
        self.jellyfish = Jellyfish.from_cap_task(self)
        self.read_stats = ReadStats.from_cap_task(self)

    @classmethod
    def version(cls):
        return 'v0.2.1'

    @classmethod
    def dependencies(cls):
        return [HmpComparison, Humann2, BrakenKraken2, Mash, ReadStats, Jellyfish]

    @classmethod
    def _module_name(cls):
        return 'processed_reads'

    def requires(self):
        return self.hmp, self.humann2, self.kraken2, self.mash, self.read_stats, self.jellyfish

    def output(self):
        return {}

    def complete(self):
        for depends in self.requires():
            if not depends.complete():
                return False
        return True

    def _run(self):
        pass
