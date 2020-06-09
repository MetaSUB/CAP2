
import luigi
import subprocess
from os.path import join, dirname, basename

from ..utils.cap_task import CapTask
from ..config import PipelineConfig
from ..utils.conda import CondaPackage
from ..databases.human_removal_db import HumanRemovalDB
from .base_reads import BaseReads


class AdapterRemoval(CapTask):
    ILLUMINA_SHARED_PREFIX = 'AGATCGGAAGAGC'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pkg = CondaPackage(
            package="adapterremoval",
            executable="AdapterRemoval",
            channel="bioconda",
            config_filename=self.config_filename,
        )
        self.reads = BaseReads(
            pe1=self.pe1,
            pe2=self.pe2,
            sample_name=self.sample_name,
            config_filename=self.config_filename,
        )
        self.config = PipelineConfig(self.config_filename)
        self.out_dir = self.config.out_dir
        self.adapter1 = self.ILLUMINA_SHARED_PREFIX
        self.adapter2 = self.ILLUMINA_SHARED_PREFIX

    def requires(self):
        return self.pkg, self.reads

    @classmethod
    def version(cls):
        return 'v0.2.0'

    @classmethod
    def dependencies(cls):
        return ["adapterremoval", BaseReads]

    @classmethod
    def _module_name(cls):
        return 'adapter_removal'

    def output(self):
        return {
            'adapter_removed_reads_1': self.get_target('adapter_removed', 'R1.fastq.gz'),
            'adapter_removed_reads_2': self.get_target('adapter_removed', 'R2.fastq.gz'),
            'settings': self.get_target('settings', 'txt'),
        }

    def _run(self):
        basename = f'ar_temp_{self.sample_name}'
        cmd = (
            f'{self.pkg.bin} '
            f'--file1 {self.reads.output()["base_reads_1"].path} '
            f'--file2 {self.reads.output()["base_reads_2"].path} '
            '--trimns '
            '--trimqualities '
            '--gzip '
            f'--adapter1 {self.adapter1} '
            f'--adapter1 {self.adapter2} '
            f'--output1 {self.output()["adapter_removed_reads_1"].path} '
            f'--output2 {self.output()["adapter_removed_reads_2"].path} '
            f'--settings {self.output()["settings"].path} '
            f'--basename {basename} '
            '--minquality 2 '
            f'--threads {self.cores} '
            '; '
            f'rm {basename}*'
        )
        self.run_cmd(cmd)
