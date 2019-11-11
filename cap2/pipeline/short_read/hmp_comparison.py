
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
            package="mash",
            executable="mash",
            channel="bioconda"
        )
        self.config = PipelineConfig(self.config_filename)
        self.out_dir = self.config.out_dir
        self.db = HmpDB(config_filename=self.config_filename)
        self.mash = Mash(
            sample_name=self.sample_name,
            pe1=self.pe1,
            pe2=self.pe2,
            config_filename=self.config_filename
        )

    def requires(self):
        return self.pkg, self.db, self.mash

    def output(self):
        dists = luigi.LocalTarget(
            join(self.out_dir, f'{self.sample_name}.mash.hmp_dists.tsv')
        )
        dists.makedirs()
        return {'hmp_dists': dists}

    def run(self):
        cmd = (
            f'{self.pkg.bin} '
            f'{self.db.mash_sketch} '
            f'{self.mash.output()["10M_mash_sketch"].path} '
            f'> {self.output()["hmp_dists"].path}'
        )
        subprocess.call(cmd, shell=True)
