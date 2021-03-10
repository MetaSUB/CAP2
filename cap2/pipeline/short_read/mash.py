
import luigi
import subprocess
from os.path import join, dirname, basename

from ..utils.cap_task import CapTask
from ..constants import MASH_SKETCH_SIZE
from ..config import PipelineConfig
from ..utils.conda import CondaPackage
from ..preprocessing.clean_reads import CleanReads


class Mash(CapTask):
    module_description = """
    This module provides small sketches of samples.

    Motivation: MASH sketches provide an efficient way to compute the
    distance between microbiome samples. Since MASH sketches are not
    based on any database they aren't biased towards certain sample
    types.

    Negatives: Small MASH sketch sizes can obscure differences between
    samples. As such this module produces two different sketch sizes.
    """
    MODULE_VERSION = 'v0.2.0'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pkg = CondaPackage(
            package="mash==2.2.2",
            executable="mash",
            channel="bioconda",
            config_filename=self.config_filename,
        )
        self.config = PipelineConfig(self.config_filename)
        self.out_dir = self.config.out_dir
        self.reads = CleanReads.from_cap_task(self)

    def tool_version(self):
        return self.run_cmd(f'{self.pkg.bin} --version').stderr.decode('utf-8')

    @classmethod
    def _module_name(cls):
        return 'mash'

    def requires(self):
        return self.pkg, self.reads

    @classmethod
    def dependencies(cls):
        return ['mash==2.2.2', CleanReads]

    def output(self):
        return {
            '10M_mash_sketch': self.get_target('10M_sketch', 'msh'),
            '10K_mash_sketch': self.get_target('10K_sketch', 'msh'),
        }

    def _cmd(self, mash_sketch_size, out_key):
        cmd = (
            f'{self.pkg.bin} '
            f'sketch -s {mash_sketch_size} '
            f'-o {self.output()[out_key].path[:-4]} '
            f'{self.reads.output()["clean_reads_1"].path}'
        )
        return cmd

    def _run(self):
        self.run_cmd(self._cmd(10 * 1000, '10K_mash_sketch'))
        self.run_cmd(self._cmd(10 * 1000 * 1000, '10M_mash_sketch'))
