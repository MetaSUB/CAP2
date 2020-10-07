
import luigi
from os.path import join, dirname
from glob import glob
import subprocess

from .tasks import StrainCapDbTask
from .get_microbial_genome import get_microbial_genome
from ....pipeline.config import PipelineConfig
from ....pipeline.utils.conda import CondaPackage


class GenomeFastaGetterDb(StrainCapDbTask):
    """
    """
    MAX_N_GENOMES = 100
    DATABASE = 'refseq'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.config = PipelineConfig(self.config_filename)
        self.db_dir = self.config.db_dir
        self.fastas = glob(f'{self.genome_dir}/*genomic.fna.gz')

    def requires(self):
        return []

    @classmethod
    def _module_name(cls):
        return 'experimental::genome_fasta_getter'

    @classmethod
    def version(cls):
        return 'v0.1.0'

    @classmethod
    def dependencies(cls):
        return []

    @property
    def genome_dir(self):
        return join(self.db_dir, 'download_genomes', self.genome_name)

    def output(self):
        flag = luigi.LocalTarget(self.genome_dir + f'/{self.genome_name}.downloaded.flag')
        flag.makedirs()
        return {'download_flag': flag}

    def run(self):
        self.fastas = get_microbial_genome(
            self.genome_name,
            outdir=self.genome_dir,
            max_n_genomes=self.MAX_N_GENOMES,
            database=self.DATABASE,
        )
        open(self.output()['download_flag'].path, 'w').close()
