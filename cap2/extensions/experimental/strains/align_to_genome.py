
import luigi
import subprocess
import logging
from os.path import join, dirname, basename

from .align_to_genome_db import AlignReadsToGenomeDb

from .tasks import StrainCapTask
from ....pipeline.config import PipelineConfig
from ....pipeline.utils.conda import CondaPackage
from ....pipeline.preprocessing.map_to_human import RemoveHumanReads

logger = logging.getLogger('experimental::strains')


class AlignReadsToGenome(StrainCapTask):
    genome_name = luigi.Parameter()  # A genome name with only lowercase characters and underscores
    genome_path = luigi.Parameter(significant=False)  # A filepath to a folder containing fastas
    module_description = """
    This module 

    Motivation: 

    Negatives: 
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pkg = CondaPackage(
            package="bowtie2",
            executable="bowtie2",
            channel="bioconda",
            config_filename=self.config_filename,
        )
        self.samtools = CondaPackage(
            package="samtools=1.09",
            executable="samtools",
            channel="bioconda",
            config_filename=self.config_filename,
        )
        self.config = PipelineConfig(self.config_filename)
        self.out_dir = self.config.out_dir
        self.db = AlignReadsToGenomeDb(
            genome_name=self.genome_name,
            genome_path=self.genome_path,
            config_filename=self.config_filename,
            cores=self.cores,
        )
        self.reads = RemoveHumanReads(
            pe1=self.pe1,
            pe2=self.pe2,
            sample_name=self.sample_name,
            config_filename=self.config_filename,
            cores=self.cores,
            data_type=self.data_type,
        )

    def requires(self):
        return self.samtools, self.pkg, self.db, self.reads

    @classmethod
    def version(cls):
        return 'v0.1.0'

    def tool_version(self):
        version = '[BOWTIE2]\n'
        version += self.run_cmd(f'{self.pkg.bin} --version').stderr.decode('utf-8')
        version += '\n[SAMTOOLS]\n'
        version += self.run_cmd(f'{self.samtools.bin} --version').stderr.decode('utf-8')
        return version

    @classmethod
    def dependencies(cls):
        return ["samtools", "bowtie2", AlignReadsToGenomeDb, RemoveHumanReads]

    @classmethod
    def _module_name(cls):
        return 'experimental::align_to_genome'

    def output(self):
        out = {
            f'bam__{self.genome_name}': self.get_target(f'genome_alignment__{self.genome_name}', 'bam'),
        }
        return out

    @property
    def temp_bam_path(self):
        return join(dirname(self.bam_path), 'temp_unsorted_' + basename(self.bam_path))

    @property
    def bam_path(self):
        return self.output()[f'bam__{self.genome_name}'].path

    def _run(self):
        logger.info(f'running {self.module_name()} for {self.genome_name}')
        if self.paired:
            return self._run_paired()
        return self._run_single()

    def _sort_bam(self):
        cmd = (
            f'{self.samtools.bin} '
            'sort '
            f'{self.temp_bam_path} '
            f'-o {self.bam_path} '
            f'&& rm {self.temp_bam_path}'
        )
        self.run_cmd(cmd)

    def _run_single(self):
        cmd = ''.join((
            self.pkg.bin,
            ' -x ', self.db.bowtie2_index,
            ' -U ', self.reads.output()['nonhuman_reads_1'].path,
            ' --threads ', str(self.cores),
            ' --very-sensitive ',
            f' | {self.samtools.bin} view -F 4 -b > ',
            f'{self.temp_bam_path}'
        ))
        self.run_cmd(cmd)
        return self._sort_bam()

    def _run_paired(self):
        cmd = ''.join((
            self.pkg.bin,
            ' -x ', self.db.bowtie2_index,
            ' -1 ', self.reads.output()['nonhuman_reads_1'].path,
            ' -2 ', self.reads.output()['nonhuman_reads_2'].path,
            ' --threads ', str(self.cores),
            ' --very-sensitive ',
            f' | {self.samtools.bin} view -F 4 -b > ',
            f'{self.temp_bam_path}'

        ))
        self.run_cmd(cmd)
        return self._sort_bam()
