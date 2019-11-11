
import luigi
from os.path import join
from glob import glob
import subprocess

from ..config import PipelineConfig
from ..utils.conda import CondaPackage


class HumanRemovalDB(luigi.Task):
    """This class is responsible for building and/or retriveing
    validating the database which will be used to remove human
    reads from the sample.
    """
    config_filename = luigi.Parameter()
    cores = luigi.IntParameter(default=1)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pkg = CondaPackage(
            package="bowtie2",
            executable="bowtie2-build",
            channel="bioconda"
        )
        self.config = PipelineConfig(self.config_filename)
        self.db_dir = self.config.db_dir
        self.fastas = list(glob(join(self.db_dir, 'hg38') + '/*.fa.gz'))

    def requires(self):
        return self.pkg

    @property
    def bowtie2_index(self):
        return join(self.db_dir, 'human_removal.bt2')

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

    def run(self):
        self.build_bowtie2_index_from_fasta()
