
import luigi
import subprocess
from os.path import join, dirname, basename

from ..utils.cap_task import CapTask
from ..config import PipelineConfig
from ..utils.conda import CondaPackage, PyPiPackage
from ..databases.uniref import Uniref90, HumannIdTable
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
    MODULE_VERSION = 'v0.2.0'

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
        self.reads = CleanReads.from_cap_task(self)

    def tool_version(self):
        return self.run_cmd(f'{self.pkg.bin} --version').stderr.decode('utf-8')

    def requires(self):
        return self.pkg, self.db, self.reads

    @classmethod
    def _module_name(cls):
        return 'diamond'

    @classmethod
    def dependencies(cls):
        return ['humann==3.0.0a4', 'diamond==0.9.32', Uniref90, CleanReads]

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

    Notes: This class is named Humann2 for historical reasons. It uses
    humann3
    """
    MODULE_VERSION = 'v0.3.0'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.config = PipelineConfig(self.config_filename)
        self.humann = PyPiPackage(
            package="humann==3.0.0a4",
            executable="humann",
            config_filename=self.config_filename,
        )
        self.db = HumannIdTable.from_cap_task(self)
        self.out_dir = self.config.out_dir
        self.alignment = MicaUniref90.from_cap_task(self)

    def tool_version(self):
        return self.run_cmd(f'{self.pkg.bin} --version').stderr.decode('utf-8')

    def requires(self):
        return self.humann, self.alignment, self.db

    @classmethod
    def _module_name(cls):
        return 'humann'

    @classmethod
    def dependencies(cls):
        return ['humann==3.0.0a4', MicaUniref90, HumannIdTable]

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
            f'{self.humann.bin} '
            f'--id-mapping {self.db.humann_id_table} '
            f'--input {self.alignment.output()["m8"].path} '
            f'--output {self.sample_name}_humann2 ; '
            'mv ' + genes + ' ' + self.output()['genes'].path + '; '
            'mv ' + abunds + ' ' + self.output()['path_abunds'].path + '; '
            'mv ' + covs + ' ' + self.output()['path_covs'].path + '; '
            f'rm -r {self.sample_name}_humann2;'
        )
        self.run_cmd(cmd)
