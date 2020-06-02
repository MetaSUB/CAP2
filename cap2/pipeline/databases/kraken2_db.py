
import luigi

from os.path import join
from glob import glob
import subprocess

from ..config import PipelineConfig
from ..utils.conda import CondaPackage
from ..utils.cap_task import CapDbTask


class Kraken2DB(CapDbTask):
    config_filename = luigi.Parameter()
    cores = luigi.IntParameter(default=1)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pkg = CondaPackage(
            package="kraken2=2.0.9beta",
            executable="kraken2",
            channel="bioconda",
            env="CAP_v2_kraken2",
            config_filename=self.config_filename,
        )
        self.config = PipelineConfig(self.config_filename)
        self.db_dir = self.config.db_dir
        self.libraries = ['archaea', 'bacteria', 'plasmid', 'viral', 'fungi', 'protozoa']
        self.kraken_db_dir = ''

    def requires(self):
        return self.pkg

    @classmethod
    def _module_name(cls):
        return 'kraken2_taxa_db'

    @classmethod
    def version(cls):
        return 'v0.1.0'

    @classmethod
    def dependencies(cls):
        return ['kraken2=2.0.9beta', '2020-06-01']

    @property
    def kraken2_db(self):
        return join(self.db_dir, 'taxa_kraken2')

    def output(self):
        db_taxa = luigi.LocalTarget(join(self.kraken2_db, 'taxDB'))
        db_taxa.makedirs()
        return {'kraken2_db_taxa': db_taxa}

    def run(self):
        self.download_kraken2_db()
        self.build_kraken2_db()

    def download_kraken2_db(self):
        for library in []:
            cmd = (
                f'{self.pkg.bin}-build '
                f'--download-library {library} '
                f'--db {self.kraken_db_dir}'
            )
            self.run_cmd(cmd)

    def build_kraken2_db(self):
        cmd = (
            f'{self.pkg.bin}-build '
            '--build '
            f'--db {self.kraken_db_dir}'
        )
        self.run_cmd(cmd)
