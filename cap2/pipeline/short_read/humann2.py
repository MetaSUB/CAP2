
import luigi
import subprocess
from os.path import join, dirname, basename

from ..utils.cap_task import CapTask
from ..config import PipelineConfig
from ..utils.conda import CondaPackage
from ..databases.uniref import Uniref90
from ..preprocessing.clean_reads import CleanReads


class MicaUniref90(CapTask):
    module_description = """
    This module aligns reads to UniRef90 in
    preparation for functional profiling.

    Motivation: UniRef90 is a large database of functional genes. 

    Negatives: Fairly slow. Many of the genes in UniRef90 are
    merely predicted functions.

    Note: this module currently uses Diamond not MiCA
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pkg = CondaPackage(
            package="diamond",
            executable="diamond",
            channel="bioconda",
            config_filename=self.config_filename,
        )
        self.config = PipelineConfig(self.config_filename)
        self.out_dir = self.config.out_dir
        self.db = Uniref90(config_filename=self.config_filename)
        self.reads = CleanReads(
            sample_name=self.sample_name,
            pe1=self.pe1,
            pe2=self.pe2,
            config_filename=self.config_filename
        )

    def tool_version(self):
        return self.run_cmd(f'{self.pkg.bin} --version').stderr.decode('utf-8')

    def requires(self):
        return self.pkg, self.db, self.reads

    @classmethod
    def _module_name(cls):
        return 'diamond'

    @classmethod
    def version(cls):
        return 'v0.2.0'

    @classmethod
    def dependencies(cls):
        return ['diamond==0.9.32', Uniref90, CleanReads]

    def output(self):
        return {
            'm8': self.get_target('uniref90', 'm8.gz'),
        }

    def _run(self):
        cmd = (
            f'{self.pkg.bin} blastx '
            f'--threads {self.cores} '
            f'-d {self.db.diamond_index} '
            f'-q {self.reads.output()["clean_reads_1"].path} '
            '--block-size 6 '
            f'| gzip > {self.output()["m8"].path} '
        )
        self.run_cmd(cmd)


class Humann2(CapTask):
    module_description = """
    This module provides functional profiles of microbiomes.

    Motivation: Functional profiles are sometimes more stable than
    taxonomic profiles and can provided metabolic and adaptive
    insights. 

    Negatives: Functional profiling is somewhat less well benchmarked
    than taxonomic profiling and metabolic pathways are often based on
    model organisms.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.config = PipelineConfig(self.config_filename)
        self.out_dir = self.config.out_dir
        self.alignment = MicaUniref90(
            sample_name=self.sample_name,
            pe1=self.pe1,
            pe2=self.pe2,
            config_filename=self.config_filename
        )

    def tool_version(self):
        return self.run_cmd(f'{self.pkg.bin} --version').stderr.decode('utf-8')

    def requires(self):
        return self.alignment

    @classmethod
    def _module_name(cls):
        return 'humann2'

    @classmethod
    def version(cls):
        return 'v0.2.0'

    @classmethod
    def dependencies(cls):
        return ['humann2', MicaUniref90]

    def output(self):
        return {
            'genes': self.get_target('genes', 'tsv'),
            'path_abunds': self.get_target('path_abunds', 'tsv'),
            'path_covs': self.get_target('path_covs', 'tsv'),
        }

    def _run(self):
        odir = self.sample_name + '_humann2'
        genes = odir + '/*genefamilies.tsv'
        abunds = odir + '/*pathabundance.tsv'
        covs = odir + '/*pathcoverage.tsv'
        cmd = (
            f'humann2 '
            f'--input {self.alignment.output()["m8"].path} '
            f'--output {self.sample_name}_humann2 ; '
            'mv ' + genes + ' ' + self.output()['genes'].path + '; '
            'mv ' + abunds + ' ' + self.output()['path_abunds'].path + '; '
            'mv ' + covs + ' ' + self.output()['path_covs'].path + '; '
            f'rm -r {self.sample_name}_humann2;'
        )
        self.run_cmd(cmd)
