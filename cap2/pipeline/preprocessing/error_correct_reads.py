
import luigi
import subprocess
from os.path import join, dirname, basename

from ..config import PipelineConfig
from ..utils.conda import CondaPackage
from .map_to_human import RemoveHumanReads


class ErrorCorrectReads(luigi.Task):
    sample_name = luigi.Parameter()
    pe1 = luigi.Parameter()
    pe2 = luigi.Parameter()
    config_filename = luigi.Parameter()
    cores = luigi.IntParameter(default=1)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pkg = CondaPackage(
            package="bayeshammer",
            executable="fastqc",
            channel="bioconda"
        )
        self.config = PipelineConfig(self.config_filename)
        self.out_dir = self.config.out_dir
        self.nonhuman_reads = RemoveHumanReads(
            pe1=self.pe1,
            pe2=self.pe2,
            sample_name=self.sample_name,
            config_filename=self.config_filename,
        )

    def requires(self):
        return self.pkg, self.nonhuman_reads

    def output(self):
        ec_1 = luigi.LocalTarget(join(
            self.out_dir, f'{self.sample_name}.error_corrected_reads.R1.fastq.gz'
        ))
        ec_1.makedirs()
        ec_2 = luigi.LocalTarget(join(
            self.out_dir, f'{self.sample_name}.error_corrected_reads.R2.fastq.gz'
        ))
        return {
            'error_corrected_reads': [ec_1, ec_2],
        }

    def run(self):
        pass
