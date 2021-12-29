
import luigi

from os.path import join, abspath, dirname, isfile
from glob import glob
import subprocess

from ..config import PipelineConfig
from ..utils.conda import CondaPackage
from ..utils.cap_task import CapDbTask

MINIKRAKEN_V2_8GB_URL = 'ftp://ftp.ccb.jhu.edu/pub/data/kraken2_dbs/old/minikraken2_v2_8GB_201904.tgz'


class FastKraken2DB(CapDbTask):
    config_filename = luigi.Parameter()
    cores = luigi.IntParameter(default=1)
    MODULE_VERSION = 'v0.1.0'

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

    def requires(self):
        return self.pkg

    @classmethod
    def _module_name(cls):
        return 'fast_kraken2_taxa_db'

    @classmethod
    def dependencies(cls):
        return ['kraken2', MINIKRAKEN_V2_8GB_URL]

    @property
    def kraken2_db(self):
        return join(self.config.db_dir, 'minikraken2_v2_8GB_201904_UPDATE')

    def output(self):
        db_taxa = luigi.LocalTarget(join(self.kraken2_db, 'hash.k2d'))
        return {'kraken2_db_taxa': db_taxa}

    def run(self):
        self.output()['kraken2_db_taxa'].makedirs()
        cmd = (
            'wget '
            f'--directory-prefix={self.config.db_dir} '
            f'{MINIKRAKEN_V2_8GB_URL} '
            ' && '
            f'tar -xzf {join(self.config.db_dir, "minikraken2_v2_8GB_201904.tgz")}'
            ' && '
            f'mv minikraken2_v2_8GB_201904_UPDATE {self.config.db_dir}'
        )
        self.run_cmd(cmd)
