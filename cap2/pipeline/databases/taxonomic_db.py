
import luigi

from os.path import join
from glob import glob
import subprocess

from ..config import PipelineConfig
from ..utils.conda import CondaPackage


class TaxonomicDB(luigi.Task):
    config_filename = luigi.Parameter()
    cores = luigi.IntParameter(default=1)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pkg = CondaPackage(
            package="krakenuniq",
            executable="krakenuniq-build",
            channel="bioconda"
        )
        self.config = PipelineConfig(self.config_filename)
        self.db_dir = self.config.db_dir
        self.kraken_db_dir = ''

    def requires(self):
        return self.pkg

    @property
    def krakenuniq_db(self):
        return join(self.db_dir, 'taxa_krakenuniq')

    def output(self):
        db_taxa = luigi.LocalTarget(join(self.krakenuniq_db, 'taxDB'))
        db_taxa.makedirs()
        return {'krakenuniq_db_taxa': db_taxa}

    def run(self):
        self.build_krakenuniq_db()

    def build_krakenuniq_db(self):
        cmd = self.pkg.bin
        cmd += f' --db {self.kraken_db_dir} --threads {self.cores} '
        cmd += ' --taxids-for-genomes --taxids-for-sequences --kmer-len 31 '
        print(cmd)
        subprocess.check_call(cmd, shell=True)
