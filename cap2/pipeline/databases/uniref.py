
import luigi
from os.path import join
import subprocess

from ..config import PipelineConfig
from ..utils.conda import CondaPackage
from ..utils.cap_task import CapDbTask


class Uniref90(CapDbTask):
    config_filename = luigi.Parameter()
    cores = luigi.IntParameter(default=1)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.boost_pkg = CondaPackage(  # the diamond package on conda has a broken link
            package="boost-cpp=1.70",
            executable="",
            channel="conda-forge",
            config_filename=self.config_filename,
        )
        self.pkg = CondaPackage(
            package="diamond",
            executable="diamond",
            channel="bioconda",
            config_filename=self.config_filename,
        )
        self.config = PipelineConfig(self.config_filename)
        self.db_dir = self.config.db_dir
        self.fasta = join(self.db_dir, 'uniref90', 'uniref90.fasta.gz')

    def requires(self):
        return [self.pkg]

    @classmethod
    def _module_name(cls):
        return 'diamond_uniref_db'

    @classmethod
    def version(cls):
        return 'v1.0.0'

    @classmethod
    def dependencies(cls):
        return ['diamond==0.9.32', '2020-06-01']

    @property
    def diamond_index(self):
        return join(self.db_dir, 'uniref90', 'uniref90.dmnd')

    def output(self):
        diamond_index = luigi.LocalTarget(self.diamond_index)
        diamond_index.makedirs()
        return {
            'diamond_index': diamond_index,
        }

    def run(self):
        if self.config.db_mode == PipelineConfig.DB_MODE_BUILD:
            cmd = self.pkg.bin + ' makedb'
            cmd += f' --in {self.fasta} -d {self.diamond_index[:-5]}'
            self.run_cmd(cmd)
        else:
            self.download_uniref90_index_from_s3()

    def download_uniref90_index_from_s3(self):
        paths = [
            'cap2/databases/2020-06-08/uniref90/uniref90.dmnd',
            'cap2/databases/2020-06-08/uniref90/uniref90.fasta.gz',
        ]
        for path in paths:
            cmd = (
                'wget '
                f'--directory-prefix={dirname(self.output()["diamond_index"].path)} '
                f'https://s3.wasabisys.com/metasub-microbiome/{path} '

            )
            self.run_cmd(cmd)
