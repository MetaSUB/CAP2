
import luigi
import subprocess
from os.path import join, dirname, basename

from ..utils.cap_task import CapTask
from ..config import PipelineConfig
from ..utils.conda import CondaPackage
from ..databases.kraken2_db import Kraken2DB
from ..preprocessing.clean_reads import CleanReads


class Kraken2(CapTask):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pkg = CondaPackage(
            package="kraken2=2.0.9beta",
            executable="kraken2",
            channel="bioconda",
            config_filename=self.config_filename,
        )
        self.config = PipelineConfig(self.config_filename)
        self.out_dir = self.config.out_dir
        self.db = Kraken2DB(config_filename=self.config_filename)
        self.reads = CleanReads(
            sample_name=self.sample_name,
            pe1=self.pe1,
            pe2=self.pe2,
            config_filename=self.config_filename
        )

    @classmethod
    def _module_name(cls):
        return 'kraken2'

    def requires(self):
        return self.pkg, self.db, self.reads

    @classmethod
    def version(cls):
        return 'v0.1.0'

    @classmethod
    def dependencies(cls):
        return ['kraken2=2.0.9beta', Kraken2DB, CleanReads]

    def output(self):
        return {
            'report': self.get_target('report', 'tsv'),
            'read_assignments': self.get_target('read_assignments', 'tsv'),
        }

    def _run(self):
        cmd = (
            f'{self.pkg.bin} '
            f'--paired '
            f'--threads {self.cores} '
            '--fastq-input '
            '--use-mpa-style '
            '--gzip-compressed '
            f'--report {self.output()["report"].path} '
            f'{self.reads.output()["clean_reads"][0].path} '
            f'{self.reads.output()["clean_reads"][1].path} '
            f'> {self.output()["read_assignments"].path}'
        )
        self.run_cmd(cmd)
