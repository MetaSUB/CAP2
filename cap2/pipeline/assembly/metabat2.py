
import luigi
import shutil
from os.path import join, dirname, basename, isdir
from shutil import rmtree
from glob import glob

from ..utils.cap_task import CapTask
from ..config import PipelineConfig
from ..utils.conda import CondaPackage
from .metaspades import MetaspadesAssembly


class MetaBat2Binning(CapTask):
    module_description = """
    This module groups contigs into bins that (hopefully) equate to one species.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pkg = CondaPackage(
            package="metabat2",
            executable="metabat2",
            channel="bioconda",
            config_filename=self.config_filename
        )
        self.contigs = MetaspadesAssembly(
            sample_name=self.sample_name,
            pe1=self.pe1,
            pe2=self.pe2,
            config_filename=self.config_filename
        )
        self.config = PipelineConfig(self.config_filename)

    def tool_version(self):
        lines = self.run_cmd(f'{self.pkg.bin} --help').stderr.decode('utf-8').split('\n')
        return lines[1]

    @classmethod
    def _module_name(cls):
        return 'metabat2'

    def requires(self):
        return self.pkg, self.contigs

    @classmethod
    def version(cls):
        return 'v0.1.0'

    @classmethod
    def dependencies(cls):
        return ['metabat2', MetaspadesAssembly]

    def output(self):
        return {
            'scaffold_bins': self.get_target('scaffold_bins', 'tar.gz'),
            'bin_assignments': self.get_target('bin_assignments', 'tsv'),
        }

    @property
    def bin_name(self):
        return f'{self.sample_name}_metabat2'

    def untar_then_list_bin_files(self):
        self.run_cmd(f'tar -xzf {self.output()["scaffold_bins"].path}')
        return sorted(list(glob(f'{self.bin_name}.*.fa')))

    def clean_up_bin_files(self):
        scaffold_bin_fastas = ' '.join(glob(f'{self.bin_name}.*.fa'))
        self.run_cmd(f'rm {scaffold_bin_fastas}')

    def _run(self):
        scaffolds = self.contigs.output()['scaffolds_fasta'].path
        cmd = ' '.join((
            self.pkg.bin,
            f'-i {scaffolds} ',
            f'-o {self.bin_name}',
            f'-t {self.cores}',
            '-m 1500 --maxP 95 --minS 60 --maxEdges 200 --pTNF 0 --seed 10 ',
            '--saveCls',
        ))
        print('CMD!!!!', cmd)
        self.run_cmd(cmd)
        scaffold_bin_fastas = ' '.join(glob(f'{self.bin_name}.*.fa'))
        self.run_cmd(f'tar -czf {self.output()["scaffold_bins"].path} {scaffold_bin_fastas}')
        self.clean_up_bin_files()
        self.run_cmd(f'mv {self.bin_name} {self.output()["bin_assignments"].path}')
