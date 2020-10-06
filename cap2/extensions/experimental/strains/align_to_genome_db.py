
import luigi
from os.path import join, dirname
from glob import glob
import subprocess

from ....pipeline.utils.cap_task import CapDbTask
from ....pipeline.config import PipelineConfig
from ....pipeline.utils.conda import CondaPackage


class AlignReadsToGenomeDb(CapDbTask):
    """
    """
    genome_name = luigi.Parameter()
    genome_path = luigi.Parameter(significant=False)

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
        self.fastas = []
        for ext in ['.fa', '.fa.gz', '.fna', '.fna.gz']:
            self.fastas += list(glob(self.genome_path + f'/*{ext}'))

    def tool_version(self):
        return self.run_cmd(f'{self.pkg.bin} --version').stderr.decode('utf-8')

    def requires(self):
        return self.pkg

    @classmethod
    def _module_name(cls):
        return 'experimental::align_to_genome_db'

    @classmethod
    def version(cls):
        return 'v0.1.0'

    @classmethod
    def dependencies(cls):
        return ['bowtie2==2.4.1']

    @property
    def bowtie2_index(self):
        return join(self.db_dir, 'align_to_genomes', self.genome_name, f'{self.genome_name}.bt2')

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
