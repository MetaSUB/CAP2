
import luigi
import subprocess
from os.path import join, dirname, basename

from ..config import PipelineConfig
from ..utils.conda import CondaPackage
from .prodigal import Prodigal


class CardRGI(luigi.Task):
    sample_name = luigi.Parameter()
    pe1 = luigi.Parameter()
    pe2 = luigi.Parameter()
    config_filename = luigi.Parameter()
    cores = luigi.IntParameter(default=1)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pkg = CondaPackage(
            package="rgi",
            executable="rgi",
            channel="bioconda"
        )
        self.config = PipelineConfig(self.config_filename)
        self.out_dir = self.config.out_dir
        self.prots = Prodigal(
            sample_name=self.sample_name,
            pe1=self.pe1,
            pe2=self.pe2,
            config_filename=self.config_filename
        )

    def requires(self):
        return self.pkg, self.prots

    def output(self):
        amrs = luigi.LocalTarget(
            join(self.out_dir, f'{self.sample_name}.rgi.amrs.tsv')
        )
        amrs.makedirs()
        return {
            'amrs': amrs
        }

    def run(self):
        cmd = (
            f'{self.pkg.bin} main '
            f' -i {self.prots.proteins} '
            f' -o {self.output()["amrs"].path} '
            f' -t protein '
            f' -a diamond '
            f' -n {self.cores} '
            f' --include_loose '
            f' --low_quality '
            f' --clean '
            f' -d wgs '
        )
        subprocess.check_call(cmd, shell=True)
