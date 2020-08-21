
import luigi
import shutil
from os.path import join, dirname, basename, isdir
from shutil import rmtree

from ..utils.cap_task import CapTask
from ..config import PipelineConfig
from ..utils.conda import CondaPackage
from ..preprocessing import CleanReads


class MetaspadesAssembly(CapTask):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pkg = CondaPackage(
            package="spades",
            executable="metaspades.py",
            channel="bioconda",
            config_filename=self.config_filename
        )
        self.reads = CleanReads(
            sample_name=self.sample_name,
            pe1=self.pe1,
            pe2=self.pe2,
            config_filename=self.config_filename
        )
        self.config = PipelineConfig(self.config_filename)
        self.exc = self.pkg.bin
        if self.config.exc_metaspades is not None:
            self.exc = self.config.exc_metaspades

    @classmethod
    def _module_name(cls):
        return 'metaspades'

    def requires(self):
        return self.pkg, self.reads

    @classmethod
    def version(cls):
        return 'v0.3.0'

    @classmethod
    def dependencies(cls):
        return ['spades', CleanReads]

    def tool_version(self):
        return self.run_cmd(f'{self.exc} --version').stderr.decode('utf-8')

    def output(self):
        return {
            'contigs': self.get_target('contigs', 'fasta'),
            'contig_paths': self.get_target('contigs', 'paths'),
            'scaffolds_fasta': self.get_target('scaffolds', 'fasta'),
            'scaffolds_paths': self.get_target('scaffolds', 'paths'),
            'fastg': self.get_target('graph', 'fastg'),
        }

    def _run(self):
        out_dir = f'{self.out_dir}/tmp_metaspades_out.{self.sample_name}'
        if isdir(out_dir):
            shutil.rmtree(out_dir)
        cmd = ''.join((
            self.exc,
            ' --only-assembler ',  # we start from error corrected reads
            ' -1 ', self.reads.output()["clean_reads_1"].path,
            ' -2 ', self.reads.output()["clean_reads_2"].path,
            f' -t {self.cores} ',
            ' -m 200 ',
            f' -o {out_dir}'
        ))
        self.run_cmd(cmd)
        pairs_to_move = [
            ('contigs.fasta', 'contigs'),
            ('scaffolds.fasta', 'scaffolds_fasta'),
            ('scaffolds.paths', 'scaffolds_paths'),
            ('assembly_graph.fastg', 'fastg'),
            ('contigs.paths', 'contig_paths'),
        ]
        out = self.output()
        for cur, new in pairs_to_move:
            cur = f'{out_dir}/{cur}'
            new = out[new].path
            self.run_cmd(f'mv {cur} {new}')
        rmtree(out_dir)
