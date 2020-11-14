
import luigi
import subprocess
import os
from os.path import join, dirname, basename, isfile

from ..utils.cap_task import CapTask
from ..constants import MASH_SKETCH_SIZE
from ..config import PipelineConfig
from ..utils.conda import CondaPackage
from ..preprocessing.clean_reads import CleanReads


class Jellyfish(CapTask):
    Ks = [31, 15]
    module_description = """
    This module coutns kmer abundances.

    Motivation: 

    Negatives: 
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pkg = CondaPackage(
            package="jellyfish",
            executable="jellyfish",
            channel="bioconda",
            config_filename=self.config_filename,
        )
        self.config = PipelineConfig(self.config_filename)
        self.out_dir = self.config.out_dir
        self.reads = CleanReads(
            sample_name=self.sample_name,
            pe1=self.pe1,
            pe2=self.pe2,
            config_filename=self.config_filename
        )

    def tool_version(self):
        return self.run_cmd(f'{self.pkg.bin} --version').stderr.decode('utf-8')

    @classmethod
    def _module_name(cls):
        return 'jellyfish'

    def requires(self):
        return self.pkg, self.reads

    @classmethod
    def version(cls):
        return 'v0.2.0'

    @classmethod
    def dependencies(cls):
        return ['jellyfish', CleanReads]

    def output(self):
        return {
            f'k{K}': self.get_target(f'k{K}', 'jf')
            for K in self.Ks
        }

    def _cmd(self, k):
        outfile = self.output()[f'k{k}'].path
        r1 = self.reads.output()['clean_reads_1'].path
        r2 = self.reads.output()['clean_reads_2'].path
        cmd = (
            '/bin/bash '
            f'{self.pkg.bin} count '
            f'-m {k} '
            '-s 1G '
            '-t 8 '
            '-C '
            f'-o  {outfile} '
            f'<(gunzip -c {r1}) '
            f'<(gunzip -c {r2}) '
        )
        return cmd

    def _run(self):
        for K in self.Ks:
            self.run_cmd(self._cmd(K))
            path = self.output()[f'k{K}'].path
            if isfile(path + '_0'):
                os.rename(path + '_0', path)
