
import luigi
import subprocess
from os.path import join, dirname, basename

from ..utils.cap_task import CapTask
from ..config import PipelineConfig
from ..utils.conda import CondaPackage
from ..databases.hmp_db import HmpDB
from .mash import Mash


class HmpComparison(CapTask):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pkg = CondaPackage(
            package="mash",
            executable="mash",
            channel="bioconda",
            config_filename=self.config_filename,
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

    @classmethod
    def _module_name(cls):
        return 'hmp_comparison'

    def requires(self):
        return self.pkg, self.db, self.mash

    @classmethod
    def version(cls):
        return 'v1.0.0'

    @classmethod
    def dependencies(cls):
        return ['mash==2.2.2', HmpDB, Mash]

    def output(self):
        return {'mash': self.get_target('mash', 'tsv')}

    def _run(self):
        cmd = (
            f'{self.pkg.bin} '
            f'{self.db.mash_sketch} '
            f'{self.mash.output()["10M_mash_sketch"].path} '
            f'> {self.output()["mash"].path}'
        )
        self.run_cmd(cmd)
