
import luigi

from os.path import join
from glob import glob
import subprocess

from ..config import PipelineConfig
from ..utils.conda import CondaPackage
from ..utils.cap_task import CapDbTask


class TaxonomicDB(CapDbTask):
    config_filename = luigi.Parameter()
    cores = luigi.IntParameter(default=1)
    MODULE_VERSION = 'v1.0.0'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pkg = CondaPackage(
            package="krakenuniq==0.5.8",
            executable="krakenuniq-build",
            channel="bioconda",
            config_filename=self.config_filename,
        )
        self.config = PipelineConfig(self.config_filename)
        self.db_dir = self.config.db_dir
        self.kraken_db_dir = ''

    def tool_version(self):
        return self.run_cmd(f'{self.pkg.bin} --version').stderr.decode('utf-8')

    def requires(self):
        return self.pkg

    @classmethod
    def _module_name(cls):
        return 'krakenuniq_taxa_db'MODULE_VERSION =

    @classmethod
    def dependencies(cls):
        return ['krakenuniq==0.5.8', '2020-06-01']

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
