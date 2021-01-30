
import luigi
import subprocess
from os.path import join, dirname, basename

from ..utils.cap_task import CapTask
from ..utils.utils import estimate_read_length
from ..config import PipelineConfig
from ..utils.conda import CondaPackage
from ..databases.fast_kraken2_db import FastKraken2DB
from .base_reads import BaseReads


class FastKraken2(CapTask):
    module_description = """
    This module provides quick initial taxonomic assignments for short reads.

    Motivation: Taxonomic identification is critical for understanding
    microbiomes. Kraken2 is a well benchmarked tool that is computationally
    efficient. 

    Negatives: Kraken2 uses pseudo-alignment which is somewhat less sensitive
    and specific than true alignment. This module uses a small database on
    uncleaned reads. It is fast but not terribly accurate.
    """

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
        self.db = FastKraken2DB(config_filename=self.config_filename)
        self.reads = BaseReads.from_cap_task(self)

    @classmethod
    def _module_name(cls):
        return 'fast_kraken2'

    def tool_version(self):
        return self.run_cmd(f'{self.pkg.bin} --version').stderr.decode('utf-8')

    def requires(self):
        return self.rsync_pkg, self.pkg, self.db, self.reads

    @classmethod
    def version(cls):
        return 'v0.1.0'

    @classmethod
    def dependencies(cls):
        return ['kraken2', FastKraken2DB, BaseReads]

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
            '--gzip-compressed '
            f'--report {self.output()["report"].path} '
            f'{self.reads.read_1} '
            f'{self.reads.read_2} '
            f'> {self.output()["read_assignments"].path}'
        )
        self.run_cmd(cmd)
