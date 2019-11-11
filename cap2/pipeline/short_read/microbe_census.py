
import luigi
import subprocess
from os.path import join, dirname, basename

from ..config import PipelineConfig
from ..utils.conda import CondaPackage
from ..preprocessing.clean_reads import CleanReads


class MicrobeCensus(luigi.Task):
    sample_name = luigi.Parameter()
    pe1 = luigi.Parameter()
    pe2 = luigi.Parameter()
    config_filename = luigi.Parameter()
    cores = luigi.IntParameter(default=1)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pkg = CondaPackage(
            package="microbecensus",
            executable="microbe_census",
            channel="bioconda"
        )
        self.config = PipelineConfig(self.config_filename)
        self.out_dir = self.config.out_dir
        self.reads = CleanReads(
            sample_name=self.sample_name,
            pe1=self.pe1,
            pe2=self.pe2,
            config_filename=self.config_filename
        )

    def requires(self):
        return self.pkg, self.reads

    def output(self):
        report = luigi.LocalTarget(
            join(self.out_dir, f'{self.sample_name}.microbe_census.report.tsv')
        )
        report.makedirs()
        return {
            'report': report,
        }

    def run(self):
        cmd = (
            f'{self.pkg.bin} '
            f'-t {self.cores} '
            f'{self.reads.output()["clean_reads"][0].path},'
            f'{self.reads.output()["clean_reads"][1].path} '
            f'{self.output()["report"].path}'
        )
        subprocess.call(cmd, shell=True)
