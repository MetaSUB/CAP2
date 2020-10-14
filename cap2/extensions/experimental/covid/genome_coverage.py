
import luigi
import logging
import subprocess
import logging
from os.path import join, dirname, basename

from .align_to_covid_genome import AlignReadsToCovidGenome

from ....pipeline.utils.cap_task import CapTask
from ....pipeline.config import PipelineConfig
from ....pipeline.utils.conda import CondaPackage
from ....pipeline.preprocessing.map_to_human import RemoveHumanReads

logger = logging.getLogger('experimental::covid')


class CovidGenomeCoverage(CapTask):
    module_description = """
    This module 

    Motivation: 

    Negatives: 
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pkg = CondaPackage(
            package="bedtools",
            executable="bedtools",
            channel="bioconda",
            config_filename=self.config_filename,
        )
        self.config = PipelineConfig(self.config_filename)
        self.out_dir = self.config.out_dir
        self.bam = AlignReadsToCovidGenome(
            pe1=self.pe1,
            pe2=self.pe2,
            sample_name=self.sample_name,
            config_filename=self.config_filename,
            cores=self.cores,
            data_type=self.data_type,
        )

    def requires(self):
        return self.samtools, self.pkg, self.bam

    @classmethod
    def version(cls):
        return 'v0.1.0'

    def tool_version(self):
        version = '[BEDTOOLS]\n'
        version += self.run_cmd(f'{self.pkg.bin} --version').stdout.decode('utf-8')
        return version

    @classmethod
    def dependencies(cls):
        return ["bedtools", "bowtie2", AlignReadsToCovidGenome]

    @classmethod
    def _module_name(cls):
        return 'experimental::covid_genome_coverage'

    def output(self):
        out = {
            'genomecov': self.get_target(f'genome_coverage', 'genomecov'),
        }
        return out

    @property
    def genomecov_path(self):
        return self.output()[f'genomecov'].path

    def _run(self):
        cmd = (
            f'{self.pkg.bin} '
            'genomecov '
            '-d '
            f'-ibam {self.bam.bam_path} '
            f'> {self.genomecov_path} ' 
        )
        self.run_cmd(cmd)
