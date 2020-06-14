
import luigi
import subprocess
from os.path import join, dirname, basename

from ....pipeline.utils.cap_task import CapTask, CapDbTask
from ....pipeline.config import PipelineConfig
from ....pipeline.utils.conda import CondaPackage
from ....pipeline.databases.human_removal_db import HumanRemovalDB
from ....pipeline.preprocessing.base_reads import BaseReads


KRAKEN2_COVID_DB_URL = 'https://s3.wasabisys.com/metasub/covid/kraken2_covid_2020_03_13.tar.gz'


class Kraken2FastDetectCovidDB(CapDbTask):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.config = PipelineConfig(self.config_filename)

    def requires(self):
        return []

    @classmethod
    def version(cls):
        return 'v0.1.0'

    @classmethod
    def dependencies(cls):
        return []

    @classmethod
    def _module_name(cls):
        return 'experimental::covid19_fast_detect_db'

    @property
    def kraken2_covid_db(self):
        return join(
            self.config.db_dir, KRAKEN2_COVID_DB_URL.split("/")[-1].split('.tar.gz')[0]
        )

    def output(self):
        db_taxa = luigi.LocalTarget(self.kraken2_covid_db)
        db_taxa.makedirs()
        return {'kraken2_db_covid': db_taxa}

    def _run(self):
        cmd = (
            f'cd {self.config.db_dir} && '
            f'wget {KRAKEN2_COVID_DB_URL} && '
            f'tar -xzf {self.kraken2_covid_db} && '
            f'mv home/cem2009/Projects/SARS-CoV-2/database/covid_2020.03.13 {self.kraken2_covid_db}'
        )
        self.run_cmd(cmd)


class Kraken2FastDetectCovid(CapTask):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # self.rsync_pkg = CondaPackage(
        #     package="rsync",
        #     executable="rsync",
        #     channel="conda-forge",
        #     env="CAP_v2_kraken2",
        #     config_filename=self.config_filename,
        # )
        self.pkg = CondaPackage(
            package="kraken2",
            executable="kraken2",
            channel="bioconda",
            env="CAP_v2_kraken2",
            config_filename=self.config_filename,
        )
        self.reads = BaseReads(
            pe1=self.pe1,
            pe2=self.pe2,
            sample_name=self.sample_name,
            config_filename=self.config_filename,
            cores=self.cores,
        )
        self.db = Kraken2FastDetectCovidDB(config_filename=self.config_filename, cores=self.cores)
        self.config = PipelineConfig(self.config_filename)

    def requires(self):
        return self.pkg, self.reads, self.db

    @classmethod
    def version(cls):
        return 'v0.1.0'

    @classmethod
    def dependencies(cls):
        return ["kraken2", BaseReads, Kraken2FastDetectCovidDB]

    @classmethod
    def _module_name(cls):
        return 'experimental::covid19_fast_detect'

    def output(self):
        return {
            'report': self.get_target('report', 'tsv'),
            'read_assignments': self.get_target('read_assignments', 'tsv'),
        }

    def _run(self):
        cmd = (
            f'{self.pkg.bin} '
            f'--db {self.db.kraken2_covid_db} '
            f'--paired '
            f'--threads {self.cores} '
            '--use-mpa-style '
            '--gzip-compressed '
            f'--report {self.output()["report"].path} '
            f'{self.reads.output()["base_reads_1"].path} '
            f'{self.reads.output()["base_reads_2"].path} '
            f'> {self.output()["read_assignments"].path}'
        )
        self.run_cmd(cmd)
