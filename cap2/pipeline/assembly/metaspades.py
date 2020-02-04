
import luigi
from os.path import join, dirname, basename
from shutil import rmtree

from ..utils.cap_task import CapTask
from ..config import PipelineConfig
from ..utils.conda import CondaPackage
from ..databases.human_removal_db import HumanRemovalDB


class MetaspadesAssembly(CapTask):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pkg = CondaPackage(
            package="spades",
            executable="metaspades.py",
            channel="bioconda"
        )
        self.config = PipelineConfig(self.config_filename)
        self.out_dir = self.config.out_dir

    def requires(self):
        return self.pkg

    def output(self):
        trgt = lambda a, b: self.get_target(self.sample_name, 'metaspades', a, b)
        return {
            'contigs': trgt('contigs', 'fasta'),
            'scaffolds_fasta': trgt('scaffolds', 'fasta'),
            'scaffolds_paths': trgt('scaffolds', 'paths'),
            'fastg': trgt('graph', 'fastg'),
            'gfa': trgt('graph', 'gfa'),
        }

    def run(self):
        out_dir = f'{self.out_dir}/tmp_metaspades_out.{self.sample_name}'
        cmd = ''.join((
            self.pkg.bin,
            ' -1 ', self.pe1,
            ' -2 ', self.pe2,
            f' -t {self.cores} ',
            ' -m 200 ',
            f' -o {out_dir}'
        ))
        self.run_cmd(cmd)
        pairs_to_move = [
            ('contigs.fasta', 'contigs'),
            ('scaffolds.fasta', 'scaffolds'),
            ('scaffolds.paths', 'scaffolds'),
            ('assembly_graph.fastg', 'fastq'),
            ('assembly_graph_with_scaffolds.gfa', 'gfa'),

        ]
        out = self.output()
        for cur, new in pairs_to_move:
            cur = f'{out_dir}/{cur}'
            new = out[new].path
            self.run_cmd(f'mv {cur} {new}')
        rmtree(out_dir)
