
import luigi
import subprocess
from os.path import join, dirname, basename

from ..utils.cap_task import CapTask
from ..config import PipelineConfig
from ..utils.conda import CondaPackage
from ..databases.uniref import Uniref90
from ..preprocessing.clean_reads import CleanReads


class MicaUniref90(CapTask):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pkg = CondaPackage(
            package="diamond",
            executable="diamond",
            channel="bioconda"
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

    def module_name(self):
        return 'diamond'

    def requires(self):
        return self.pkg, self.db, self.reads

    def output(self):
        return {
            'm8': self.get_target('uniref90', 'm8.gz'),
        }

    def _run(self):
        cmd = (
            f'{self.pkg.bin} blastx '
            f'--threads {self.cores} '
            f'-d {self.db.diamond_index} '
            f'-q {self.reads.output()["clean_reads"][0]} '
            '--block-size 6 '
            f'| gzip > {self.output()["m8"].path} '
        )
        self.run_cmd(cmd)


class Humann2(CapTask):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pkg = CondaPackage(
            package="humann2",
            executable="humann2",
            channel="bioconda"
        )
        self.config = PipelineConfig(self.config_filename)
        self.out_dir = self.config.out_dir
        self.alignment = MicaUniref90(
            sample_name=self.sample_name,
            pe1=self.pe1,
            pe2=self.pe2,
            config_filename=self.config_filename
        )

    def module_name(self):
        return 'humann2'

    def requires(self):
        return self.pkg, self.alignment

    def output(self):
        return {
            'genes': self.get_target('genes', 'tsv'),
            'path_abunds': self.get_target('path_abunds', 'tsv'),
            'path_covs': self.get_target('path_covs', 'tsv'),
        }

    def _run(self):
        odir = params.sample_name + '_humann2'
        genes = odir + '/*genefamilies.tsv'
        abunds = odir + '/*pathabundance.tsv'
        covs = odir + '/*pathcoverage.tsv'
        cmd = (
            f'{self.pkg.bin} '
            f'--input {self.alignment.output()["m8"].path} '
            f'--output {self.sample_name}_humann2 ; '
            'mv ' + genes + ' ' + self.output()['genes'].path + '; '
            'mv ' + abunds + ' ' + self.output()['path_abunds'].path + '; '
            'mv ' + covs + ' ' + self.output()['path_covs'].path + '; '
            f'rm -r {self.sample_name}_humann2;'
        )
        self.run_cmd(cmd)
