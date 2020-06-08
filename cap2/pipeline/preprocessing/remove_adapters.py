
import luigi
import subprocess
from os.path import join, dirname, basename

from ..utils.cap_task import CapTask
from ..config import PipelineConfig
from ..utils.conda import CondaPackage
from ..databases.human_removal_db import HumanRemovalDB


class AdapterRemoval(CapTask):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pkg = CondaPackage(
            package="adapterremoval",
            executable="AdapterRemoval",
            channel="bioconda",
            config_filename=self.config_filename,
        )
        self.config = PipelineConfig(self.config_filename)
        self.out_dir = self.config.out_dir

    def requires(self):
        return self.pkg

    @classmethod
    def version(cls):
        return 'v0.1.0'

    @classmethod
    def dependencies(cls):
        return ["adapterremoval"]

    @classmethod
    def _module_name(cls):
        return 'adapter_removal'

    def output(self):
        return {
            'adapter_removed_reads_1': self.get_target('adapter_removed', 'R1.fastq.gz'),
            'adapter_removed_reads_2': self.get_target('adapter_removed', 'R2.fastq.gz'),
        }

    def _run(self):
        basename = f'ar_temp_{self.sample_name}'
        cmd = (
            f'{self.pkg.bin} '
            f'--file1 {self.pe1} '
            f'--file2 {self.pe2} '
            '--trimns '
            '--trimqualities '
            '--gzip '
            f'--output1 {self.output()["adapter_removed_reads_1"].path} '
            f'--output2 {self.output()["adapter_removed_reads_2"].path} '
            f'--basename {basename} '
            '--minquality 2 '
            f'--threads {self.cores} '
            '; '
            f'rm {basename}*'
        )
        self.run_cmd(cmd)
