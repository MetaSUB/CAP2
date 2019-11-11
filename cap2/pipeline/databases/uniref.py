
import luigi
from os.path import join
import subprocess

from ..config import PipelineConfig
from ..utils.conda import CondaPackage


class Uniref90(luigi.Task):
    config_filename = luigi.Parameter()
    cores = luigi.IntParameter(default=1)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pkg = CondaPackage(
            package="diamond",
            executable="diamond",
            channel="bioconda"
        )
        self.config = PipelineConfig(self.config_filename)
        self.db_dir = self.config.db_dir
        self.fasta = join(self.db_dir, 'uniref90', 'uniref90.faa.gz')

    def requires(self):
        return self.pkg

    @property
    def diamond_index(self):
        return join(self.db_dir, 'uniref90.dmnd')

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
        subprocess.call(cmd, shell=True)
