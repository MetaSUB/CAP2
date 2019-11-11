
import luigi
import subprocess
from os.path import join, dirname, basename

from ..constants import MASH_SKETCH_SIZE
from ..config import PipelineConfig
from ..utils.conda import CondaPackage
from ..preprocessing.clean_reads import CleanReads


class Mash(luigi.Task):
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
        self.reads = CleanReads(
            sample_name=self.sample_name,
            pe1=self.pe1,
            pe2=self.pe2,
            config_filename=self.config_filename
        )

    def requires(self):
        return self.pkg, self.reads

    def output(self):
        sketch = luigi.LocalTarget(
            join(self.out_dir, f'{self.sample_name}.mash.sketch.msh')
        )
        sketch.makedirs()
        return {'10M_mash_sketch': sketch}

    def run(self):
        cmd = (
            f'{self.pkg.bin} '
            f'sketch -s {MASH_SKETCH_SIZE} '
            f'-o {self.output().path} '
            f'{self.reads.output()["clean_reads"][0].path}'
        )
        subprocess.call(cmd, shell=True)
