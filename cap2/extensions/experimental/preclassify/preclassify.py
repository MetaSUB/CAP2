
import luigi
import subprocess
from os.path import join, dirname, basename

from ....pipeline.utils.cap_task import CapTask
from ....pipeline.config import PipelineConfig
from ....pipeline.utils.conda import CondaPackage
from .preclassify_db import PreclassifyKraken2DBDataDown
from ....pipeline.preprocessing.base_reads import BaseReads


class PreclassifyKraken2(CapTask):

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
        self.db = PreclassifyKraken2DBDataDown(config_filename=self.config_filename)
        self.reads = BaseReads(
            sample_name=self.sample_name,
            pe1=self.pe1,
            pe2=self.pe2,
            config_filename=self.config_filename
        )

    @classmethod
    def _module_name(cls):
        return 'preclassify_kraken2'

    def requires(self):
        return self.rsync_pkg, self.pkg, self.db, self.reads

    @classmethod
    def version(cls):
        return 'v0.1.0'

    @classmethod
    def dependencies(cls):
        return ['kraken2', PreclassifyKraken2DBDataDown, BaseReads]

    def output(self):
        return {
            'report': self.get_target('report', 'tsv'),
            'read_assignments': self.get_target('read_assignments', 'tsv'),
        }

    def _run(self):
        cmd = (
            f'{self.pkg.bin} '
            f'--db {self.db.kraken2_16s_db} '
            f'--db {self.db.kraken2_metagenome_db} '
            f'--threads {self.cores} '
            '--use-mpa-style '
            '--gzip-compressed '
            f'--report {self.output()["report"].path} '
            f'{self.reads.output()["base_reads_1"].path} '
            f'> {self.output()["read_assignments"].path}'
        )
        self.run_cmd(cmd)


class PreclassifySample(CapTask):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.config = PipelineConfig(self.config_filename)
        self.out_dir = self.config.out_dir
        self.kraken2 = PreclassifyKraken2(
            sample_name=self.sample_name,
            pe1=self.pe1,
            pe2=self.pe2,
            config_filename=self.config_filename
        )

    @classmethod
    def _module_name(cls):
        return 'preclassify_sample'

    def requires(self):
        return self.kraken2

    @classmethod
    def version(cls):
        return 'v0.1.0'

    @classmethod
    def dependencies(cls):
        return [PreclassifyKraken2]

    def output(self):
        return {
            'report': self.get_target('report', 'json'),
        }

    def _run(self):
        kraken2_report = self.kraken2.output()["report"].path

