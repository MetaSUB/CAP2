
import luigi

from cap2.pipeline.constants import MASH_SKETCH_SIZE
from ..config import PipelineConfig
from ..utils.conda import CondaPackage


class HmpDB(luigi.Task):

    config_filename = luigi.Parameter()
    cores = luigi.IntParameter(default=1)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pkg = CondaPackage(
            package="mash",
            executable="mash sketch",
            channel="bioconda"
        )
        self.config = PipelineConfig(self.config_filename)
        self.db_dir = self.config.db_dir
        self.fastqs = []

    @property
    def mash_sketch(self):
        return 'hmp_mash_sketch.msh'

    def output(self):
        sketch = luigi.LocalTarget(join(self.db_dir, self.mash_sketch))
        sketch.makedirs()
        return {
            'hmp_sketch': sketch,
        }

    def run(self):
        self.build_mash_index_sketch()

    def build_mash_index_sketch(self):
        cmd = self.pkg.bin
        cmd += f' -s {MASH_SKETCH_SIZE} -o {hmp_mash_sketch[:-3]} '
        cmd += ' '.join(fastqs)
        subprocess.call(cmd, shell=True)
