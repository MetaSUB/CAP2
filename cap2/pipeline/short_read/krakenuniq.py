
import luigi
import subprocess
from os.path import join, dirname, basename

from ..utils.cap_task import CapTask
from ..config import PipelineConfig
from ..utils.conda import CondaPackage
from ..databases.taxonomic_db import TaxonomicDB
from ..preprocessing.clean_reads import CleanReads


class KrakenUniq(CapTask):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pkg = CondaPackage(
            package="krakenuniq==0.5.8",
            executable="krakenuniq",
            channel="bioconda",
            config_filename=self.config_filename,
        )
        self.config = PipelineConfig(self.config_filename)
        self.out_dir = self.config.out_dir
        self.db = TaxonomicDB(config_filename=self.config_filename)
        self.reads = CleanReads(
            sample_name=self.sample_name,
            pe1=self.pe1,
            pe2=self.pe2,
            config_filename=self.config_filename
        )

    @classmethod
    def _module_name(cls):
        return 'krakenuniq'

    def requires(self):
        return self.pkg, self.db, self.reads

    @classmethod
    def version(cls):
        return 'v0.1.0'

    @classmethod
    def dependencies(cls):
        return ['krakenuniq==0.5.8', TaxonomicDB, CleanReads]

    def output(self):
        return {
            'report': self.get_target('report', 'tsv'),
            'read_assignments': self.get_target('read_assignments', 'tsv'),
        }

    def _run(self):
        report_path = self.output()['report'].path
        read_assignments = self.output['read_assignments'].path
        cmd = (
            f'{self.pkg.bin} '
            f'--report-file {report_path} '
            '--gzip-compressed '
            '--fastq-input '
            f'--threads {self.cores} '
            '--paired '
            '--preload '
            f'--db {self.db.krakenuniq_db} '
            f'{self.reads.output()["clean_reads"][0].path} '
            f'{self.reads.output()["clean_reads"][1].path} '
            f'> {read_assignments}'
        )
        self.run_cmd(cmd)
