
import luigi
import subprocess
from os.path import join, dirname, basename

from ..config import PipelineConfig
from ..utils.conda import CondaPackage
from ..databases.taxonomic_db import TaxonomicDB


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
        self.db = TaxonomicDB()

    def requires(self):
        return self.pkg, self.db

    def output(self):
        report = luigi.LocalTarget(join(self.out_dir, f'{self.sample_name}.report.tsv'))
        read_assignments = luigi.LocalTarget(join(self.out_dir, f'{self.sample_name}.read_assignments.tsv'))
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
            f'{self.pe1} '
            f'{self.pe2} '
            f'> {read_assignments}'
        )
        subprocess.call(cmd, shell=True)
