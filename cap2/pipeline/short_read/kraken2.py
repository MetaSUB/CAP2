
import luigi
import subprocess
from os.path import join, dirname, basename

from ..utils.cap_task import CapTask
from ..utils.utils import estimate_read_length
from ..config import PipelineConfig
from ..utils.conda import CondaPackage
from ..databases.kraken2_db import Kraken2DB, BrakenKraken2DB
from ..preprocessing.clean_reads import CleanReads


class Kraken2(CapTask):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.rsync_pkg = CondaPackage(
            package="rsync",
            executable="rsync",
            channel="conda-forge",
            env="CAP_v2_kraken2",
            config_filename=self.config_filename,
        )
        self.pkg = CondaPackage(
            package="kraken2",
            executable="kraken2",
            channel="bioconda",
            env="CAP_v2_kraken2",
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
        return self.rsync_pkg, self.pkg, self.db, self.reads

    @classmethod
    def version(cls):
        return 'v0.2.0'

    @classmethod
    def dependencies(cls):
        return ['kraken2', Kraken2DB, CleanReads]

    def output(self):
        return {
            'report': self.get_target('report', 'tsv'),
            'read_assignments': self.get_target('read_assignments', 'tsv'),
        }

    def _run(self):
        cmd = (
            f'{self.pkg.bin} '
            f'--db {self.db.kraken2_db} '
            f'--paired '
            f'--threads {self.cores} '
            '--use-mpa-style '
            '--gzip-compressed '
            f'--report {self.output()["report"].path} '
            f'{self.reads.output()["clean_reads_1"].path} '
            f'{self.reads.output()["clean_reads_2"].path} '
            f'> {self.output()["read_assignments"].path}'
        )
        self.run_cmd(cmd)


class BrakenKraken2(CapTask):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pkg = CondaPackage(
            package="bracken==2.6.0",
            executable="bracken",
            channel="bioconda",
            env="CAP_v2_bracken_kraken2",
            config_filename=self.config_filename,
        )
        self.config = PipelineConfig(self.config_filename)
        self.report = Kraken2(
            sample_name=self.sample_name,
            pe1=self.pe1,
            pe2=self.pe2,
            config_filename=self.config_filename
        )
        self.db = BrakenKraken2DB(config_filename=self.config_filename)
        self.reads = CleanReads(
            sample_name=self.sample_name,
            pe1=self.pe1,
            pe2=self.pe2,
            config_filename=self.config_filename
        )

    @classmethod
    def _module_name(cls):
        return 'bracken_kraken2'

    def requires(self):
        return self.pkg, self.report, self.db, self.reads

    @classmethod
    def version(cls):
        return 'v0.1.0'

    @classmethod
    def dependencies(cls):
        return ['bracken==2.6.0', Kraken2, BrakenKraken2DB, CleanReads]

    def output(self):
        return {
            'report': self.get_target('report', 'tsv'),
        }

    def _run(self):
        rlen = estimate_read_length(self.reads.output()["clean_reads_1"].path)
        index_len, _ = self.db.get_index(rlen)
        cmd = (
            f'{self.pkg.bin} '
            f'-d {self.db.kraken2_db} '
            f'-i {self.report.output()["report"]} '
            f'-o {self.output()["report"]} '
            f'-r {index_len} '
            '-l S '  # Species
            '-t 10 '  # Min reads
        )
        self.run_cmd(cmd)
