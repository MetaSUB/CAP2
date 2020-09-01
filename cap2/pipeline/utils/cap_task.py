
import luigi
import subprocess
import datetime
import json
import os

from hashlib import sha256
from sys import stderr
from os.path import join

from ..config import PipelineConfig


class BaseCapTask(luigi.Task):
    config_filename = luigi.Parameter(default='')
    cores = luigi.IntParameter(default=1)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.version()  # force this method to be implemented
        self.task_build_time = datetime.datetime.now().isoformat()
        self.config = PipelineConfig(self.config_filename)
        self.out_dir = self.config.out_dir
        self.pre_run_hooks = []

    @classmethod
    def version(cls):
        raise NotImplementedError()

    @classmethod
    def dependencies(cls):
        raise NotImplementedError()

    @classmethod
    def version_tree(cls, terminal=True):
        """Return a newick tree with versions."""
        out = f'{cls._module_name()}=={cls.version()}'
        if cls.dependencies:
            depends = [
                el if isinstance(el, str) else el.version_tree(terminal=False)
                for el in cls.dependencies()
            ]
            depends = ','.join(depends)
            out = f'({depends}){out}'
        if terminal:
            out += ';'
        return out

    @classmethod
    def version_hash(cls):
        try:
            version = cls.version()
        except:
            print(cls, file=stderr)
            raise
        out = ''
        for el in [version] + cls.dependencies():
            if not isinstance(el, str):
                el = el.version_hash()
            result = sha256(el.encode())
            out += result.hexdigest()
        return out

    @classmethod
    def short_version_hash(cls):
        myhash = cls.version_hash()
        return myhash[:4] + myhash[20:24] + myhash[-4:]

    @classmethod
    def _module_name(cls):
        raise NotImplementedError(cls.module_name())

    @classmethod
    def module_name(cls):
        return 'cap2::' + cls._module_name()

    def tool_version(self):
        return 'tool_version_unknown'

    def get_run_metadata(self):
        uname = os.uname()
        blob = {
            'task_build_time': self.task_build_time,
            'run_start_time': self.run_start_time,
            'cores': self.cores,
            'current_time': datetime.datetime.now().isoformat(),
            'tool_version': self.tool_version(),
            'version_hash': self.version_hash(),
            'module_version': self.version(),
            'host_info': {
                'system_name': uname.sysname,
                'node_name': uname.nodename,
                'release': uname.release,
                'version': uname.version,
                'machine': uname.machine,
            },
        }
        return blob

    def _run(self):
        raise NotImplementedError()

    def run(self):
        self.run_start_time = datetime.datetime.now().isoformat()
        for hook in self.pre_run_hooks:
            hook()
        run = self._run()
        with open(self.get_target('run_metadata', 'json').path, 'w') as metafile:
            metafile.write(json.dumps(self.get_run_metadata()))
        return run

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
        return job


class CapDbTask(BaseCapTask):
    """Currently a stub. May do something in future."""


class CapTask(BaseCapTask):
    """Base class for CAP2 tasks.

    Currently implements some basic shared logic.
    """
    sample_name = luigi.Parameter()
    pe1 = luigi.Parameter()
    pe2 = luigi.Parameter()
    data_type = luigi.Parameter(default='short_read')

    def get_target(self, field_name, ext):
        filename = '.'.join([
            self.sample_name, self.module_name(), self.short_version_hash(), field_name, ext
        ])
        filepath = join(self.config.out_dir, self.sample_name, filename)
        target = luigi.LocalTarget(filepath)
        target.makedirs()
        return target

    @property
    def paired(self):
        return self.pe2 and self.data_type == 'short_read'

    @classmethod
    def from_sample(cls, sample, config_path, cores=1):
        return cls(
            pe1=sample.r1,
            pe2=sample.r2,
            sample_name=sample.name,
            config_filename=config_path,
            cores=cores,
            data_type=sample.kind
        )


class CapGroupTask(BaseCapTask):
    group_name = luigi.Parameter()
    samples = luigi.TupleParameter()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def get_target(self, field_name, ext):
        name = self.group_name.lower().replace(' ', '_') 
        filename = f'{name}.{self.module_name()}.{field_name}.{ext}'
        filepath = join(self.config.out_dir, 'groups', name, filename)
        target = luigi.LocalTarget(filepath)
        target.makedirs()
        return target

    def _make_req_module(self, module_type, pe1, pe2, sample_name, config_filename):
        return module_type(
            pe1=pe1,
            pe2=pe2,
            sample_name=sample_name,
            config_filename=config_filename,
        )

    def module_req_list(self, module_type):
        reqs = [
            self._make_req_module(
                module_type, sample_tuple[1], sample_tuple[2], sample_tuple[0], self.config_filename
            )
            for sample_tuple in self.samples
        ]
        return reqs

    @classmethod
    def from_samples(cls, group_name, samples, config_path='', cores=1):
        samples = [s if isinstance(s, tuple) else s.as_tuple() for s in samples]
        return cls(
            group_name=group_name,
            samples=tuple(samples),
            config_filename=config_path,
            cores=cores,
        )
