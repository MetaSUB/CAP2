
import luigi
import subprocess
from os.path import join, dirname, basename

from ..config import PipelineConfig
from ..utils.conda import CondaPackage
from ..databases.taxonomic_db import TaxonomicDB
from ..preprocessing.clean_reads import CleanReads


class KrakenUniq(luigi.Task):
    sample_name = luigi.Parameter()
    pe1 = luigi.Parameter()
    pe2 = luigi.Parameter()
    config_filename = luigi.Parameter()
    cores = luigi.IntParameter(default=1)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pkg = CondaPackage(
            package="krakenuniq",
            executable="krakenuniq",
            channel="bioconda"
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

    def requires(self):
        return self.pkg, self.db, self.reads

    def output(self):
        report = luigi.LocalTarget(
            join(self.out_dir, f'{self.sample_name}.report.tsv')
        )
        read_assignments = luigi.LocalTarget(
            join(self.out_dir, f'{self.sample_name}.read_assignments.tsv')
        )
        return {
            'report': report,
            'read_assignments': read_assignments,
        }

    def run(self):
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
        subprocess.call(cmd, shell=True)
