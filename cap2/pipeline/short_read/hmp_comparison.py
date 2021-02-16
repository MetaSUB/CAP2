
import luigi
import subprocess
from os.path import join, dirname, basename

from ..utils.cap_task import CapTask
from ..config import PipelineConfig
from ..utils.conda import CondaPackage
from ..databases.hmp_db import HmpDB
from .mash import Mash


class HmpComparison(CapTask):
    module_description = """
    This module compares samples to reference samples from the HMP.

    Motivation: Similarity to human commensal microbiomes helps to
    contextualize samples. 

    Negatives: HMP samples are a useful reference but do not provide
    the full gamut of human microbiome diversity.
    """
    MODULE_VERSION = 'v0.3.0'

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
        self.mash = Mash.from_cap_task(self)

    @classmethod
    def _module_name(cls):
        return 'hmp_comparison'

    def requires(self):
        return self.pkg, self.db, self.mash

    @classmethod
    def dependencies(cls):
        return ['mash==2.2.2', HmpDB, Mash]

    def output(self):
        return {'mash': self.get_target('mash', 'tsv')}

    def _run(self):
        cmd = (
            f'{self.pkg.bin} dist '
            f'{self.db.mash_sketch} '
            f'{self.mash.output()["10M_mash_sketch"].path} '
            f'> {self.output()["mash"].path}'
        )
        self.run_cmd(cmd)
