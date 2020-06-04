
import luigi

from os.path import join, abspath, dirname
from glob import glob
import subprocess

from ..config import PipelineConfig
from ..utils.conda import CondaPackage
from ..utils.cap_task import CapDbTask


class Kraken2DBDataDown(CapDbTask):
    config_filename = luigi.Parameter()
    cores = luigi.IntParameter(default=1)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pkg = CondaPackage(
            package="kraken2",
            executable="kraken2",
            channel="bioconda",
            env="CAP_v2_kraken2",
            config_filename=self.config_filename,
        )
        self.config = PipelineConfig(self.config_filename)
        self.libraries = [
            'archaea', 'bacteria', 'plasmid', 'viral', 'fungi', 'protozoa', 'human'
        ]
        self.kraken_db_dir = 'taxa_kraken2'
        self.download_libs = True

    def requires(self):
        return [self.pkg]

    @classmethod
    def _module_name(cls):
        return 'kraken2_taxa_db_down'

    @classmethod
    def version(cls):
        return 'v0.1.0'

    @classmethod
    def dependencies(cls):
        return ['kraken2', '2020-06-01']

    @property
    def kraken2_db(self):
        return join(self.config.db_dir, self.kraken_db_dir)

    def output(self):
        download_flag = luigi.LocalTarget(join(self.kraken2_db, 'db_download_flag'))
        download_flag.makedirs()
        return {'flag': download_flag}

    def run(self):
        if self.download_libs:
            self.download_kraken2_db()
            open(self.output()['flag'].path, 'w').close()

    def download_kraken2_db(self):
        cmd = f'{self.pkg.bin}-build --use-ftp --download-taxonomy --db {self.kraken_db_dir}'
        self.run_cmd(cmd)
        for library in self.libraries:
            cmd = (
                f'PATH=$PATH:{dirname(abspath(self.pkg.bin))} '
                f'{self.pkg.bin}-build '
                f'--use-ftp '
                f'--download-library {library} '
                f'--db {self.kraken2_db}'
            )
            self.run_cmd(cmd)


class Kraken2DB(CapDbTask):
    config_filename = luigi.Parameter()
    cores = luigi.IntParameter(default=1)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pkg = CondaPackage(
            package="kraken2",
            executable="kraken2",
            channel="bioconda",
            env="CAP_v2_kraken2",
            config_filename=self.config_filename,
        )
        self.download_task = Kraken2DBDataDown(
            config_filename=self.config_filename,
        )
        self.config = PipelineConfig(self.config_filename)
        self.kraken_db_dir = 'taxa_kraken2'
        self.db_size = 120 * (1000 * 3)  # 120 GB

    def requires(self):
        return self.pkg, self.download_task

    @classmethod
    def _module_name(cls):
        return 'kraken2_taxa_db'

    @classmethod
    def version(cls):
        return 'v0.1.0'

    @classmethod
    def dependencies(cls):
        return ['kraken2', '2020-06-01', self.download_task]

    @property
    def kraken2_db(self):
        return join(self.config.db_dir, self.kraken_db_dir)

    def output(self):
        db_taxa = luigi.LocalTarget(join(self.kraken2_db, 'hash.k2d'))
        db_taxa.makedirs()
        return {'kraken2_db_taxa': db_taxa}

    def run(self):
        self.build_kraken2_db()

    def build_kraken2_db(self):
        cmd = (
            f'{self.pkg.bin}-build '
            '--build '
            f'--max-db-size {self.db_size} '
            f'--db {self.kraken2_db}'
        )
        self.run_cmd(cmd)
