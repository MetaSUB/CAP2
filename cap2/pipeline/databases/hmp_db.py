
import luigi
from os.path import join
from glob import glob
import subprocess

from cap2.pipeline.constants import MASH_SKETCH_SIZE
from ..config import PipelineConfig
from ..utils.conda import CondaPackage
from ..utils.cap_task import CapTask


class HmpDB(CapTask):

    config_filename = luigi.Parameter()
    cores = luigi.IntParameter(default=1)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pkg = CondaPackage(
            package="mash==2.2.2",
            executable="mash",
            channel="bioconda",
            config_filename=self.config_filename,
        )
        self.config = PipelineConfig(self.config_filename)
        self.db_dir = self.config.db_dir
        self.fastqs = list(glob(join(self.db_dir, 'hmp') + '/**.fastq.gz'))
        self.sketch_size = MASH_SKETCH_SIZE

    def requires(self):
        return self.pkg

    @classmethod
    def _module_name(cls):
        return 'mash_hmp_db'

    @classmethod
    def version(cls):
        return 'v1.0.0'

    @classmethod
    def dependencies(cls):
        return ['mash==2.2.2', '2020-06-01']

    @property
    def mash_sketch(self):
        return join(self.db_dir, 'hmp', 'hmp_mash_sketch.msh')

    def output(self):
        sketch = luigi.LocalTarget(self.mash_sketch)
        sketch.makedirs()
        return {
            'hmp_sketch': sketch,
        }

    def run(self):
        self.build_mash_index_sketch()

    def build_mash_index_sketch(self):
        cmd = self.pkg.bin + ' sketch'
        cmd += f' -s {self.sketch_size} -o {self.mash_sketch[:-4]} '
        cmd += ' '.join(self.fastqs)
        subprocess.check_call(cmd, shell=True)
