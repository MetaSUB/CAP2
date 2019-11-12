
import luigi
from os.path import join, dirname, basename

from ..config import PipelineConfig


class CleanContigs(luigi.Task):
    """This class represents the culmination of the
    assembly pipeline.
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
        self.contigs = None

    @property
    def fasta(self):
        return None

    def requires(self):
        return None

    def output(self):
        return {
            'fasta': self.fasta,
        }

    def run(self):
        pass
