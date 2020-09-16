
import luigi
import subprocess
from os.path import join, dirname, basename

from ..utils.cap_task import CapTask
from ..config import PipelineConfig
from ..utils.conda import CondaPackage
from .base_reads import BaseReads


class FastQC(CapTask):
    module_description = """
    FastQC computes a number of quality control metrics.

    Motivation: Quality control is important to identify
    systematic errors and sequencing issues. FastQC
    includes a broad suite of checks.

    Negatives: FastQC only runs on a subset of reads though
    this is usually sufficient.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pkg = CondaPackage(
            package="fastqc=0.11.9",
            executable="fastqc",
            channel="bioconda",
            config_filename=self.config_filename,
        )
        self.reads = BaseReads(
            pe1=self.pe1,
            pe2=self.pe2,
            sample_name=self.sample_name,
            config_filename=self.config_filename,
            cores=self.cores,
        )
        self.config = PipelineConfig(self.config_filename)
        self.out_dir = self.config.out_dir

    @classmethod
    def _module_name(cls):
        return 'fastqc'

    def tool_version(self):
        return self.run_cmd(f'{self.pkg.bin} --version').stderr.decode('utf-8')

    @classmethod
    def version(cls):
        return 'v0.2.1'

    @classmethod
    def dependencies(cls):
        return ["fastqc==0.11.9", BaseReads]

    @property
    def _report(self):
        return basename(self.pe1).split('.f')[0] + '_fastqc.html'

    @property
    def _zip_output(self):
        return basename(self.pe1).split('.f')[0] + '_fastqc.zip'

    def requires(self):
        return self.pkg

    def output(self):
        return {
            'report': self.get_target('report', 'html'),
            'zip_output': self.get_target('zip_out', 'zip'),
        }

    def _run(self):
        # fixme: redirect output to loggers
        outdir = dirname(self.output()['report'].path)
        cmd = ' '.join([
            self.pkg._env.bin + '/perl',  # fastqc uses system perl which we do not assume access to
            self.pkg.bin,
            '-t', str(self.cores),
            self.reads.output()["base_reads_1"].path,
            f'-o {outdir}',
            '&& ',
            f'mv {outdir}/{self._report} {self.output()["report"].path}; ',
            f'mv {outdir}/{self._zip_output} {self.output()["zip_output"].path}; ',
        ])
        self.run_cmd(cmd)
