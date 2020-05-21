
import luigi
import subprocess
from os.path import join, dirname, basename

from ..utils.cap_task import CapTask
from ..config import PipelineConfig
from ..utils.conda import CondaPackage


class FastQC(CapTask):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pkg = CondaPackage(
            package="fastqc",
            executable="fastqc",
            channel="bioconda",
            config_filename=self.config_filename,
        )
        self.config = PipelineConfig(self.config_filename)
        self.out_dir = self.config.out_dir

    def _module_name(self):
        return 'fastqc'

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
            self.pe1,
            f'-o {outdir}',
            '&& ',
            f'mv {outdir}/{self._report} {self.output()["report"].path}; ',
            f'mv {outdir}/{self._zip_output} {self.output()["zip_output"].path}; ',
        ])
        self.run_cmd(cmd)
