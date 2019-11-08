
import luigi

from ..config import PipelineConfig
from ..utils.conda import CondaPackage


class TaxonomicDB(luigi.Task):
    config_filename = luigi.Parameter()
    cores = luigi.IntParameter(default=1)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pkg = CondaPackage(
            package="krakenuniq",
            executable="mash sketch",
            channel="bioconda"
        )
        self.config = PipelineConfig(self.config_filename)
        self.db_dir = self.config.db_dir
        self.kraken_db_dir = ''

    @property
    def krakenuniq_db(self):
        pass

    def output(self):
        pass

    def run(self):
        self.build_krakenuniq_db()

    def build_krakenuniq_db(self):
        cmd = self.pkg.bin
        cmd += f' --db {self.kraken_db_dir} --threads {self.cores} '
        cmd += ' --taxids-for-genomes --taxids-for-sequences --kmer-len 31 '
        subprocess.call(cmd, shell=True)
