
import luigi

from ..config import PipelineConfig
from ..utils.conda import CondaPackage


class MegaResDB(luigi.Task):

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
    def bowtie2_index(self):
        return 'hmp_mash_sketch.msh'

    @property
    def fasta(self):
        return self._fasta

    @property
    def annotations(self):
        return self._annotations

    

    def output(self):
        sketch = luigi.LocalTarget(join(self.db_dir, self.mash_sketch))
        sketch.makedirs()
        return {
            'hmp_sketch': sketch,
        }

    def run(self):
        pass


class CardDB(luigi.Task):

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
        pass
