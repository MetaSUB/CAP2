
import luigi
from os.path import join, dirname
from glob import glob
import subprocess

from ..config import PipelineConfig
from ..utils.conda import CondaPackage
from ..utils.cap_task import CapDbTask


class HumanRemovalDB(CapDbTask):
    """This class is responsible for building and/or retriveing
    validating the database which will be used to remove human
    reads from the sample.
    """
    config_filename = luigi.Parameter()
    cores = luigi.IntParameter(default=1)
    MODULE_VERSION = 'v1.0.0'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pkg = CondaPackage(
            package="bowtie2==2.4.1",
            executable="bowtie2-build",
            channel="bioconda",
            config_filename=self.config_filename,
        )
        self.config = PipelineConfig(self.config_filename)
        self.db_dir = self.config.db_dir
        self.fastas = list(glob(join(self.db_dir, 'hg38') + '/*.fa.gz'))

    def tool_version(self):
        return self.run_cmd(f'{self.pkg.bin} --version').stderr.decode('utf-8')

    def requires(self):
        return self.pkg

    @classmethod
    def _module_name(cls):
        return 'bowtie_human_removal_db'

    @classmethod
    def version(cls):
        return 'v1.0.0'

    @classmethod
    def dependencies(cls):
        return ['bowtie2==2.4.1', 'hg38_alt_contigs', '2020-06-01']

    @property
    def bowtie2_index(self):
        return join(self.db_dir, 'hg38', 'human_removal.bt2')

    def output(self):
        index = luigi.LocalTarget(self.bowtie2_index + '.1.bt2')
        index.makedirs()
        return {
            'bt2_index_1': index,
        }

    def build_bowtie2_index_from_fasta(self):
        cmd = ''.join((
            self.pkg.bin,
            f' --threads {self.cores} ',
            ','.join(self.fastas),
            ' ',
            self.bowtie2_index
        ))
        subprocess.check_call(cmd, shell=True)

    def download_bowtie2_index_from_s3(self):
        paths = [
            'cap2/databases/2020-06-08/hg38/hg38.fa.gz',
            'cap2/databases/2020-06-08/hg38/human_removal.bt2.1.bt2',
            'cap2/databases/2020-06-08/hg38/human_removal.bt2.2.bt2',
            'cap2/databases/2020-06-08/hg38/human_removal.bt2.3.bt2',
            'cap2/databases/2020-06-08/hg38/human_removal.bt2.4.bt2',
            'cap2/databases/2020-06-08/hg38/human_removal.bt2.rev.1.bt2',
            'cap2/databases/2020-06-08/hg38/human_removal.bt2.rev.2.bt2',
        ]
        for path in paths:
            cmd = (
                'wget '
                f'--directory-prefix={dirname(self.output()["bt2_index_1"].path)} '
                f'https://s3.wasabisys.com/metasub-microbiome/{path} '

            )
            self.run_cmd(cmd)

    def run(self):
        if self.config.db_mode == PipelineConfig.DB_MODE_BUILD:
            self.build_bowtie2_index_from_fasta()
        else:
            self.download_bowtie2_index_from_s3()
