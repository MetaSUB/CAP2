
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
            channel="bioconda"
        )
        self.config = PipelineConfig(self.config_filename)
        self.out_dir = self.config.out_dir

    @property
    def _report(self):
        return basename(self.pe1).split('.f')[0] + '_fastqc.html'

    @property
    def _zip_output(self):
        return basename(self.pe1).split('.f')[0] + '_fastqc.zip'

    def requires(self):
        return self.pkg

    def output(self):
        report = luigi.LocalTarget(join(self.out_dir, self._report))
        zip_out = luigi.LocalTarget(join(self.out_dir, self._zip_output))
        report.makedirs()
        zip_out.makedirs()
        return {
            'report': report,
            'zip_output': zip_out,
        }

    def run(self):
        # fixme: redirect output to loggers
        cmd = ' '.join([
            self.pkg.bin,
            '-t', str(self.cores),
            self.pe1,
            '-o',
            dirname(self.output()['report'].path)
        ])
        print(cmd)
        subprocess.check_call(cmd, shell=True)
        self._done = True
