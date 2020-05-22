
import luigi
from os.path import join
import subprocess

from ..config import PipelineConfig
from ..utils.conda import CondaPackage
from ..utils.cap_task import CapTask


class Uniref90(CapTask):
    config_filename = luigi.Parameter()
    cores = luigi.IntParameter(default=1)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pkg = CondaPackage(
            package="diamond==0.9.32",
            executable="diamond",
            channel="bioconda",
            config_filename=self.config_filename,
        )
        self.config = PipelineConfig(self.config_filename)
        self.db_dir = self.config.db_dir
        self.fasta = join(self.db_dir, 'uniref90', 'uniref90.fasta.gz')

    def requires(self):
        return self.pkg

    @classmethod
    def _module_name(cls):
        return 'diamond_uniref_db'

    @classmethod
    def version(cls):
        return 'v1.0.0'

    @classmethod
    def dependencies(cls):
        return ['diamond==0.9.32', '2020-06-01']

    @property
    def diamond_index(self):
        return join(self.db_dir, 'uniref90', 'uniref90.dmnd')

    def output(self):
        diamond_index = luigi.LocalTarget(self.diamond_index)
        diamond_index.makedirs()
        return {
            'diamond_index': diamond_index,
        }

    def run(self):
        cmd = self.pkg.bin + ' makedb'
        cmd += f' --in {self.fasta} -d {self.diamond_index[:-5]}'
        print(cmd)
        subprocess.check_call(cmd, shell=True)
