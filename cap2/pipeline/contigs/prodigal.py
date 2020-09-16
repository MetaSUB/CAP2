
import luigi
import subprocess
from os.path import join, dirname, basename

from ..config import PipelineConfig
from ..utils.conda import CondaPackage
from ..utils.cap_task import CapTask
from ..assembly.metabat2 import MetaBat2Binning


class Prodigal(CapTask):
    """Predict ORFs from assembled contigs."""
    module_description = """
    Prodigal finds predicted ORFs in microbiome data.
    """
    sample_name = luigi.Parameter()
    pe1 = luigi.Parameter()
    pe2 = luigi.Parameter()
    config_filename = luigi.Parameter()
    cores = luigi.IntParameter(default=1)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pkg = CondaPackage(
            package="prodigal",
            executable="prodigal",
            channel="bioconda",
            config_filename=self.config_filename
        )
        self.config = PipelineConfig(self.config_filename)
        self.out_dir = self.config.out_dir
        self.contigs = MetaBat2Binning(
            sample_name=self.sample_name,
            pe1=self.pe1,
            pe2=self.pe2,
            config_filename=self.config_filename
        )

    @property
    def proteins(self):
        return self.output()['protein_fasta'].path

    def requires(self):
        return self.pkg, self.contigs

    def tool_version(self):
        return self.run_cmd(f'{self.pkg.bin} --version').stderr.decode('utf-8')

    @classmethod
    def _module_name(cls):
        return 'prodigal'

    @classmethod
    def version(cls):
        return 'v0.1.0'

    @classmethod
    def dependencies(cls):
        return ['prodigal', MetaBat2Binning]

    def output(self):
        return {
            'gene_table': self.get_target('genes', 'tar.gz'),
            'protein_fasta': self.get_target('proteins', 'tar.gz'),
        }

    def run(self):

        def run_on_one_bin(bin_fasta):
            genes = bin_fasta.replace('.fa', '.genes.gbk')
            prots = bin_fasta.replace('.fa', '.aa.fa')
            cmd = (
                f'{self.pkg.bin} '
                f'-i {bin_fasta} '
                f'-f gbk '
                f'-o {genes} '
                f'-a {prots} '
                '-p meta'
            )
            self.run_cmd(cmd)
            return genes, prots

        all_genes, all_prots = [], []
        for bin_fasta in self.contigs.untar_then_list_bin_files():
            genes, prots = run_on_one_bin(bin_fasta)
            all_genes.append(genes)
            all_prots.append(prots)
        self.contigs.clean_up_bin_files()
        all_genes = " ".join(all_genes)
        all_prots = " ".join(all_prots)
        self.run_cmd(f'tar -czf {self.output()["gene_table"].path} {all_genes}')
        self.run_cmd(f'tar -czf {self.output()["protein_fasta"].path} {all_prots}')
        self.run_cmd(f'rm {all_genes}')
        self.run_cmd(f'rm {all_prots}')
