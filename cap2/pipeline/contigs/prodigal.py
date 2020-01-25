
import luigi
import subprocess
from os.path import join, dirname, basename

from ..config import PipelineConfig
from ..utils.conda import CondaPackage
from ..utils.cap_task import CapTask
from ..assembly.clean_contigs import CleanContigs


class Prodigal(CapTask):
    """Predict ORFs from assembled contigs."""
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
            channel="bioconda"
        )
        self.config = PipelineConfig(self.config_filename)
        self.out_dir = self.config.out_dir
        self.contigs = CleanContigs(
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

    def output(self):
        return {
            'gene_table': self.get_target(self.sample_name, 'prodigal', 'genes', 'tsv'),
            'protein_fasta': self.get_target(self.sample_name, 'prodigal', 'proteins', 'faa'),
        }

    def run(self):
        cmd = (
            f'{self.pkg.bin} '
            f'-i {self.contigs.fasta} '
            f'-o {self.output()["gene_table"].path} '
            f'-a {self.output()["protein_fasta"].path} '
            '-p meta'
        )
        subprocess.check_call(cmd, shell=True)
