
import luigi
import subprocess
from os.path import join, dirname, basename

from ..utils.cap_task import CapTask
from ..config import PipelineConfig
from ..utils.conda import CondaPackage
from ..preprocessing.clean_reads import CleanReads


class Sakeima(CapTask):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pkg = CondaPackage(
            package="sakeima",
            executable="sakeima",
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

    def module_name(self):
        return 'sakeima'

    def requires(self):
        return self.pkg, self.reads

    def version(self):
        return 'v1.0.0'

    def dependencies(self):
        return [CleanReads]

    def output(self):
        return {'sketch': self.get_target('sketch', 'jf')}

    def _run(self):
        cmd = (
            f'{self.pkg.bin} '
        )
        self.run_cmd(cmd)
