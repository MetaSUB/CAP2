
import luigi
import subprocess

from sys import stderr
from os.path import join

from ..config import PipelineConfig


class CapTask(luigi.Task):
    """Base class for CAP2 tasks.

    Currently implements some basic shared logic.
    """
    sample_name = luigi.Parameter()
    pe1 = luigi.Parameter()
    pe2 = luigi.Parameter()
    config_filename = luigi.Parameter(default='')
    cores = luigi.IntParameter(default=1)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.config = PipelineConfig(self.config_filename)
        self.out_dir = self.config.out_dir

    def _module_name(self):
        raise NotImplementedError()

    def module_name(self):
        return 'cap2::' + self._module_name()

    def get_target(self, field_name, ext):
        filename = f'{self.sample_name}.{self.module_name()}.{field_name}.{ext}'
        filepath = join(self.config.out_dir, self.sample_name, filename)
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

    def _run(self):
        raise NotImplementedError()

    def run(self):
        return self._run()

    def run_cmd(self, cmd):
        job = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if job.returncode != 0:
            msg = f'''
            cmd_failed: "{cmd}"
            return_code: {job.returncode}
            stdout: "{job.stdout}"
            stderr: "{job.stderr}"
            '''
            print(msg, file=stderr)
            job.check_returncode()  # raises CalledProcessError


class CapGroupTask(luigi.Task):
    group_name = luigi.Parameter()
    samples = luigi.TupleParameter()
    config_filename = luigi.Parameter()
    cores = luigi.IntParameter(default=1)

    @property
    def module_name(self):
        raise NotImplementedError()

    def get_target(self, field_name, ext):
        filename = f'{self.group_name}.{self.module_name}.{field_name}.{ext}'
        filepath = join(self.config.out_dir, filename)
        target = luigi.LocalTarget(filepath)
        target.makedirs()
        return target

    @classmethod
    def from_samples(cls, group_name, samples, config_path, cores=1):
        return cls(
            group_name=group_name,
            samples=tuple([s.as_tuple() for s in samples]),
            config_filename=config_path,
            cores=cores,
        )
