
import luigi
import subprocess
from os.path import join, dirname, basename

from ..utils.cap_task import CapTask
from ..constants import MASH_SKETCH_SIZE
from ..config import PipelineConfig
from ..utils.conda import CondaPackage
from ..preprocessing.clean_reads import CleanReads


class Mash(CapTask):

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
        self.reads = CleanReads(
            sample_name=self.sample_name,
            pe1=self.pe1,
            pe2=self.pe2,
            config_filename=self.config_filename
        )

    @classmethod
    def _module_name(cls):
        return 'mash'

    def requires(self):
        return self.pkg, self.reads

    @classmethod
    def version(cls):
        return 'v1.0.0'

    @classmethod
    def dependencies(cls):
        return ['mash==2.2.2', CleanReads]

    def output(self):
        return {'10M_mash_sketch': self.get_target('sketch', 'msh')}

    def _run(self):
        cmd = (
            f'{self.pkg.bin} '
            f'sketch -s {MASH_SKETCH_SIZE} '
            f'-o {self.output()["10M_mash_sketch"].path[:-4]} '
            f'{self.reads.output()["clean_reads"][0].path}'
        )
        self.run_cmd(cmd)
