
from ..utils.cap_task import CapTask

from .amrs import GrootAMR
from .hmp_comparison import HmpComparison
from .humann2 import Humann2
from .kraken2 import BrakenKraken2
from .mash import Mash
from .read_stats import ReadStats


class ProcessedReads(CapTask):
    """This class represents the culmination of the
    shortread pipeline.
    """
    module_description = """
    This module is a proxy for the end of the short read stage.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.hmp = HmpComparison(
            sample_name=self.sample_name,
            pe1=self.pe1,
            pe2=self.pe2,
            config_filename=self.config_filename,
            cores=self.cores,
        )
        self.humann2 = Humann2(
            sample_name=self.sample_name,
            pe1=self.pe1,
            pe2=self.pe2,
            config_filename=self.config_filename,
            cores=self.cores,
        )
        self.kraken2 = BrakenKraken2(
            sample_name=self.sample_name,
            pe1=self.pe1,
            pe2=self.pe2,
            config_filename=self.config_filename,
            cores=self.cores,
        )
        self.mash = Mash(
            sample_name=self.sample_name,
            pe1=self.pe1,
            pe2=self.pe2,
            config_filename=self.config_filename,
            cores=self.cores,
        )
        self.read_stats = ReadStats(
            sample_name=self.sample_name,
            pe1=self.pe1,
            pe2=self.pe2,
            config_filename=self.config_filename,
            cores=self.cores,
        )

    @classmethod
    def version(cls):
        return 'v0.2.0'

    @classmethod
    def dependencies(cls):
        return [HmpComparison, Humann2, Kraken2, Mash, ReadStats]

    @classmethod
    def _module_name(cls):
        return 'processed_reads'

    def requires(self):
        return self.hmp, self.humann2, self.kraken2, self.mash, self.read_stats

    def output(self):
        return {}

    def complete(self):
        for depends in self.requires():
            if not depends.complete():
                return False
        return True

    def _run(self):
        pass
