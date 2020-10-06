
import luigi
import subprocess
from os.path import join, dirname, basename

from .align_to_genome import AlignReadsToGenome

from ....pipeline.utils.cap_task import CapTask
from ....pipeline.config import PipelineConfig
from ....pipeline.utils.conda import CondaPackage
from ....pipeline.preprocessing.map_to_human import RemoveHumanReads


class MakePileup(CapTask):
    genome_name = luigi.Parameter()  # A genome name with only lowercase characters and underscores
    genome_path = luigi.Parameter(significant=False)  # A filepath to a folder containing fastas
    module_description = """
    This module 

    Motivation: 

    Negatives: 
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.samtools = CondaPackage(
            package="samtools=1.09",
            executable="samtools",
            channel="bioconda",
            config_filename=self.config_filename,
        )
        self.config = PipelineConfig(self.config_filename)
        self.out_dir = self.config.out_dir
        self.bam = AlignReadsToGenome(
            genome_name=self.genome_name,
            genome_path=self.genome_path,
            pe1=self.pe1,
            pe2=self.pe2,
            sample_name=self.sample_name,
            config_filename=self.config_filename,
            cores=self.cores,
            data_type=self.data_type,
        )

    def requires(self):
        return self.samtools, self.bam

    @classmethod
    def version(cls):
        return 'v0.1.0'

    def tool_version(self):
        version = self.run_cmd(f'{self.samtools.bin} --version').stderr.decode('utf-8')
        return version

    @classmethod
    def dependencies(cls):
        return ["samtools", AlignReadsToGenome]

    @classmethod
    def _module_name(cls):
        return 'experimental::make_pileup'

    def output(self):
        out = {
            f'pileup__{self.genome_name}': self.get_target(f'make_pileup__{self.genome_name}', 'pileup.gz'),
        }
        return out

    @property
    def pileup_path(self):
        return self.output()[f'pileup__{self.genome_name}'].path

    def _run(self):
        cmd = (
            f'{self.samtools.bin} '
            'mpileup '
            f'{self.bam.bam_path} '
            f'| gzip > {self.pileup_path}'
        )
        self.run_cmd(cmd)
