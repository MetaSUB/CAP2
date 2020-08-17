
import luigi
import os

from os.path import join, abspath, dirname
from glob import glob
import subprocess

from ....pipeline.config import PipelineConfig
from ....pipeline.utils.conda import CondaPackage
from ....pipeline.utils.cap_task import CapDbTask

DB_DATE = '2020-08-17'


'cap2/databases/2020-06-08/taxa_kraken2/db_download_flag',

KRAKEN2_SILVA = 'ftp://ftp.ccb.jhu.edu/pub/data/kraken2_dbs/16S_Silva132_20200326.tgz'
KRAKEN2_MINIKRAKEN = 'ftp://ftp.ccb.jhu.edu/pub/data/kraken2_dbs/minikraken_8GB_202003.tgz'


class PreclassifyKraken2DBDataDown(CapDbTask):
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
        self._kraken_db_dir = 'preclassify_taxa_kraken2'

    def requires(self):
        return [self.pkg]

    @classmethod
    def _module_name(cls):
        return 'preclassify_kraken2_taxa_db_down'

    @classmethod
    def version(cls):
        return 'v0.1.0'

    @classmethod
    def dependencies(cls):
        return ['kraken2', DB_DATE]

    @property
    def kraken_db_dir(self):
        return join(self.config.db_dir, self._kraken_db_dir)

    @property
    def kraken2_metagenome_db(self):
        return join(self.kraken_db_dir, 'minikraken_8GB_20200312')

    @property
    def kraken2_16s_db(self):
        return join(self.kraken_db_dir, '16S_SILVA132_k2db')

    def output(self):
        download_flag = luigi.LocalTarget(join(self.kraken_db_dir, 'db_download_flag'))
        download_flag.makedirs()
        return {'flag': download_flag}

    def run(self):
        os.makedirs(self.kraken_db_dir, exist_ok=True)
        self.download_kraken2_db()
        open(self.output()['flag'].path, 'w').close()

    def download_kraken2_db(self):
        for db in [KRAKEN2_SILVA, KRAKEN2_MINIKRAKEN]:
            cmd = f'wget -P {self.kraken_db_dir} {db}'
            self.run_cmd(cmd)
            base = db.split('/')[-1]
            cmd = f'tar -C {self.kraken_db_dir} -xzf {self.kraken_db_dir}/{base}'
            self.run_cmd(cmd)
