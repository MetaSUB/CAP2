
import luigi
import subprocess

from sys import stderr
from os.path import join


class CapTask(luigi.Task):
    """Base class for CAP2 tasks.

    Currently implements some basic shared logic.
    """
    sample_name = luigi.Parameter()
    pe1 = luigi.Parameter()
    pe2 = luigi.Parameter()
    config_filename = luigi.Parameter()
    cores = luigi.IntParameter(default=1)

    def get_target(self, obj_name, module_name, field_name, ext):
        filename = f'{obj_name}.{module_name}.{field_name}.{ext}'
        filepath = join(self.out_dir, filename)
        target = luigi.LocalTarget(filepath)
        target.makedirs()
        return target

    @classmethod
    def from_sample(cls, sample, config_path, cores=1):
        return cls(
            pe1=sample.r1,
            pe2=sample.r2,
            sample_name=sample.name,
            config_filename=config_path,
            cores=cores
        )

    def run_cmd(self, cmd):
        try:
            subprocess.check_call(cmd, shell=True)
        except subprocess.CalledProcessError:
            msg = f'''
            cmd_failed: "{cmd}"
            return_code: {cmd.returncode}
            stdout: "{cmd.stdout}"
            stderr: "{cmd.stderr}"
            '''
            print(msg, file=stderr)
            raise
