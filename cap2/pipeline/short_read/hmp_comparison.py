
import luigi
import subprocess
from os.path import join, dirname, basename

from ..config import PipelineConfig
from ..utils.conda import CondaPackage
from ..databases.hmp_db import HmpDB
from .mash import Mash


class HmpComparison(luigi.Task):
    sample_name = luigi.Parameter()
    pe1 = luigi.Parameter()
    pe2 = luigi.Parameter()
    config_filename = luigi.Parameter()
    cores = luigi.IntParameter(default=1)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pkg = CondaPackage(
            package="krakenuniq",
            executable="krakenuniq",
            channel="bioconda"
        )
        self.config = PipelineConfig(self.config_filename)
        self.out_dir = self.config.out_dir
        self.db = HmpDB()
        self.mash = Mash(
            sample_name=sample_name, pe1=pe1, pe2=pe2, config_filename=config_filename
        )

    def requires(self):
        return self.pkg, self.db, self.mash

    def output(self):
        dists = luigi.LocalTarget(
            join(self.out_dir, f'{self.sample_name}.mash.hmp_dists.tsv')
        )
        dists.makedirs()
        return dists

    def run(self):
        cmd = (
            f'{self.pkg.bin} '
            f'{self.db.mash_sketch} '
            f'{self.mash.output().path} '
            f'> {self.output().path}'
        )
        subprocess.call(cmd, shell=True)
