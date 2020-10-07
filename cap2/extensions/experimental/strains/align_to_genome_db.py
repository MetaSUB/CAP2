
import luigi
from os.path import join, dirname
from glob import glob
import subprocess

from .bacterial_genome_getter import GenomeFastaGetterDb
from .tasks import StrainCapDbTask
from ....pipeline.config import PipelineConfig
from ....pipeline.utils.conda import CondaPackage


class AlignReadsToGenomeDb(StrainCapDbTask):
    """
    """

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
        self._fastas = []
        if self.genome_path:
            for ext in ['.fa', '.fa.gz', '.fna', '.fna.gz']:
                self._fastas += list(glob(self.genome_path + f'/*{ext}'))
        else:
            self._genome_getter = GenomeFastaGetterDb(
                genome_name=self.genome_name,
                genome_path=self.genome_path,
                config_filename=self.config_filename,
                cores=self.cores,
            )

    def tool_version(self):
        return self.run_cmd(f'{self.pkg.bin} --version').stderr.decode('utf-8')

    def requires(self):
        reqs = [self.pkg]
        if self._genome_getter:
            reqs.append(self._genome_getter)
        return reqs

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
    def fastas(self):
        if self._fastas:
            return self._fastas
        # if we get here it implies `genome_path` was null
        self._fastas = self._genome_getter.fastas

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
        print(self.fastas)
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
