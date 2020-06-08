
import luigi
from os.path import join, dirname
from glob import glob
import subprocess

from cap2.pipeline.constants import MASH_SKETCH_SIZE
from ..config import PipelineConfig
from ..utils.conda import CondaPackage
from ..utils.cap_task import CapDbTask


class HmpDB(CapDbTask):

    config_filename = luigi.Parameter()
    cores = luigi.IntParameter(default=1)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pkg = CondaPackage(
            package="mash",
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
        if self.config.db_mode == PipelineConfig.DB_MODE_BUILD:
            self.build_mash_index_sketch()
        else:
            self.download_hmp_sketch_from_s3()

    def build_mash_index_sketch(self):
        cmd = self.pkg.bin + ' sketch'
        cmd += f' -s {self.sketch_size} -o {self.mash_sketch[:-4]} '
        cmd += ' '.join(self.fastqs)
        subprocess.check_call(cmd, shell=True)

    def download_hmp_sketch_from_s3(self):
        paths = [
            'cap2/databases/2020-06-08/hmp/hmp_mash_sketch.msh',
        ]
        for path in paths:
            cmd = (
                'wget '
                f'--directory-prefix={dirname(self.output()["hmp_sketch"].path)} '
                f'https://s3.wasabisys.com/metasub-microbiome/{path} '

            )
            self.run_cmd(cmd)
