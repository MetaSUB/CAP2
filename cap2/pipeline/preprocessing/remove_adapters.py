
import luigi
import subprocess
from os.path import join, dirname, basename

from ..utils.cap_task import CapTask
from ..config import PipelineConfig
from ..utils.conda import CondaPackage
from ..databases.human_removal_db import HumanRemovalDB
from .base_reads import BaseReads


class AdapterRemoval(CapTask):
    module_description = """
    This module removes adapter sequences and low wuality sequences.

    Motivation: adapter sequences can be misidentified or lead to
    issues with assembly or k-mer profiles. Removing adapters is
    fast and reduces this issue.

    Negatives: adapter sequences may not always be properly 
    identified.
    """
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
            cores=self.cores,
            data_type=self.data_type,
        )
        self.config = PipelineConfig(self.config_filename)
        self.out_dir = self.config.out_dir
        self.adapter1 = self.ILLUMINA_SHARED_PREFIX
        self.adapter2 = self.ILLUMINA_SHARED_PREFIX

    def requires(self):
        return self.pkg, self.reads

    @classmethod
    def version(cls):
        return 'v0.2.1'

    def tool_version(self):
        return self.run_cmd(f'{self.pkg.bin} --version').stderr.decode('utf-8')

    @classmethod
    def dependencies(cls):
        return ["adapterremoval", BaseReads]

    @classmethod
    def _module_name(cls):
        return 'adapter_removal'

    def output(self):
        out = {
            'adapter_removed_reads_1': self.get_target('adapter_removed', 'R1.fastq.gz'),
            'settings': self.get_target('settings', 'txt'),
        }
        if self.paired:
            out['adapter_removed_reads_2'] = self.get_target('adapter_removed', 'R2.fastq.gz')
        return out

    def _run(self):
        if self.paired:
            return self._run_paired()
        return self._run_single()

    def _run_single(self):
        basename = f'ar_temp_{self.sample_name}'
        cmd = (
            f'{self.pkg.bin} '
            f'--file1 {self.reads.output()["base_reads_1"].path} '
            '--trimns '
            '--trimqualities '
            '--gzip '
            f'--adapter1 {self.adapter1} '
            f'--output1 {self.output()["adapter_removed_reads_1"].path} '
            f'--settings {self.output()["settings"].path} '
            f'--basename {basename} '
            '--minquality 2 '
            f'--threads {self.cores} '
            '; '
            f'rm {basename}*'
        )
        self.run_cmd(cmd)

    def _run_paired(self):
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
