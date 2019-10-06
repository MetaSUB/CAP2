
import luigi
import subprocess
from os.path import join, dirname, basename

from ..config import PipelineConfig
from ..utils.conda import CondaPackage
from ..preprocessing.map_to_human import RemoveHumanReads


class MicrobeCensus(luigi.Task):
    sample_name = luigi.Parameter()
    pe1 = luigi.Parameter()
    pe2 = luigi.Parameter()
    config_filename = luigi.Parameter()
    cores = luigi.IntParameter(default=1)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pkg = CondaPackage(
            package="microbe_census",
            executable="microbe_census",
            channel="bioconda"
        )
        self.config = PipelineConfig(self.config_filename)
        self.out_dir = self.config.out_dir
        self.reads = RemoveHumanReads(
            sample_name=sample_name, pe1=pe1, pe2=pe2, config_filename=config_filename
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
            f'{self.reads.output()["nonhuman_reads"][0].path},'
            f'{self.reads.output()["nonhuman_reads"][1].path} '
            f'{self.output()["report"].path}'
        )
        subprocess.call(cmd, shell=True)
