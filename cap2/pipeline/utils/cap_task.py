
import luigi
import types
import subprocess
import datetime
import json
import os
import logging

from hashlib import sha256
from sys import stderr
from os.path import join

from ..config import PipelineConfig

logger = logging.getLogger('cap2')


class class_or_instancemethod(classmethod):
    """Decorator that provides similar functionality to classmethod

    For our use case this allows us to overwrite class level
    values in instances. this is important for spoofing the
    versions of old modules.
    """

    def __get__(self, instance, type_):
        descr_get = self.__func__.__get__
        if instance is None:
            descr_get = super().__get__
        return descr_get(instance, type_)


class BaseCapTask(luigi.Task):
    config_filename = luigi.Parameter(default='')
    cores = luigi.IntParameter(default=1)
    max_ram = luigi.IntParameter(default=0)  # maximum allowed ram in GB. 0 means no limit.
    check_versions = luigi.BoolParameter(default=True)
    module_description = "No description for this module."
    MODULE_VERSION = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.version()  # force this method to be implemented
        self.task_build_time = datetime.datetime.now().isoformat()
        self.run_start_time = ''
        self.config = PipelineConfig(self.config_filename)
        self.out_dir = self.config.out_dir
        self.pre_run_hooks = []
        self.version_override = None

        # Check if any of the allowed version already exist
        # If they do we spoof that version in.
        # NB. This will check for the current output fields
        # using the old version. If version fields were added
        # or changed this will not find an old version
        # (Indeed that would introduce difficult dependency
        # issues if downstreams relied on the new fields)
        if self.check_versions:
            for version_str, version_hash in self.config.allowed_versions(self):
                if self.version_exists(version_str, version_hash):
                    self.version_override = version_str, version_hash
                    break

    @class_or_instancemethod
    def version(cls):
        """Return a string giving a human readable version for this module only."""
        if hasattr(cls, 'version_override') and cls.version_override:
            return cls.version_override[0]
        elif cls.MODULE_VERSION:
            return cls.MODULE_VERSION
        raise NotImplementedError()

    @classmethod
    def dependencies(cls):
        """Return a list of modules this module depends on.

        Modules are either other `BaseCapTask` classes or strings.
        """
        raise NotImplementedError()

    def is_type_of_cap_task(self, cap_task_type):
        """Return True iff self is of cap_task_type.

        This method makes it easy for duck typed CAP Tasks
        to spoof their type as another CAP Task. i.e. PangeaCapTasks
        """
        try:
            return isinstance(self, cap_task_type)
        except TypeError:
            return False

    def _reqs_as_list(self):
        reqs = self.requires()
        try:
            len(reqs)
        except TypeError:
            reqs = [reqs]
        return reqs

    @class_or_instancemethod
    def _instantiated_dependencies(cls):
        """Return a list of dependencies for this class.

        If this is called on an instance replace dependencies
        that are CAPTasks with their instantied version from
        requirements.
        """
        dependencies = cls.dependencies()
        if not isinstance(cls, CapTask):
            return dependencies
        reqs = cls._reqs_as_list()
        depends_out = []
        for dependency in dependencies:
            added = False
            for req in reqs:
                if hasattr(req, 'is_type_of_cap_task') and req.is_type_of_cap_task(dependency):
                    depends_out.append(req)
                    added = True
                    break
            if not added:
                depends_out.append(dependency)
        return depends_out

    @class_or_instancemethod
    def version_tree(cls, terminal=True):
        """Return a newick tree with versions."""
        out = f'{cls.module_name()}=={cls.version()}'
        dependencies = cls._instantiated_dependencies()
        if dependencies:
            depends = [
                el.version_tree(terminal=False) if hasattr(el, 'version_tree') else str(el)
                for el in dependencies
            ]
            depends = ','.join(depends)
            out = f'({depends}){out}'
        if terminal:
            out += ';'
        return out

    @class_or_instancemethod
    def version_hash(cls):
        """Return a hash string giving the version of this task and all upstream tasks."""
        if hasattr(cls, 'version_override') and cls.version_override:
            return cls.version_override[1]
        try:
            version = cls.version()
        except:
            print(cls, file=stderr)
            raise
        dependencies = cls._instantiated_dependencies()
        out = ''
        for el in [version] + list(dependencies):
            if not isinstance(el, str):
                el = el.version_hash()
            result = sha256(el.encode())
            out += result.hexdigest()
        return out

    @class_or_instancemethod
    def short_version_hash(cls):
        """Return a 12 character hash string giving the version of this task and all upstream tasks."""
        myhash = cls.version_hash()
        return myhash[:4] + myhash[20:24] + myhash[-4:]

    @classmethod
    def _module_name(cls):
        raise NotImplementedError(cls)

    @classmethod
    def module_name(cls):
        """Return a string giving the human readable name for this task."""
        return 'cap2::' + cls._module_name()

    def tool_version(self):
        """Return a string giving a version for any software used."""
        return 'tool_version_unknown'

    def get_run_metadata_filepath(self):
        """Return a local filepath with metadata about a completed run of this task."""
        return self.get_target('run_metadata', 'json').path

    def get_run_metadata(self):
        """Return a dict with metadata about a completed run of this task."""
        uname = os.uname()
        blob = {
            'task_build_time': self.task_build_time,
            'run_start_time': self.run_start_time,
            'cores': self.cores,
            'current_time': datetime.datetime.now().isoformat(),
            'tool_version': self.tool_version(),
            'short_version_hash': self.short_version_hash(),
            'version_hash': self.version_hash(),
            'version_tree': self.version_tree(),
            'module_version': self.version(),
            'module_description': self.module_description,
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
        """Run an instance of this task."""
        logger.debug(f'starting run for {self}')
        self.run_start_time = datetime.datetime.now().isoformat()
        for hook in self.pre_run_hooks:
            logger.debug(f'running pre-run hook {hook} for {self}')
            hook()
        logger.debug(f'running module for {self}')
        run = self._run()
        with open(self.get_run_metadata_filepath(), 'w') as metafile:
            logger.debug(f'writing run metadata for {self}')
            metafile.write(json.dumps(self.get_run_metadata()))
        return run

    def run_cmd(self, cmd):
        job = subprocess.run(
            cmd,
            shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, executable='/bin/bash'
        )
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

    @classmethod
    def from_cap_task(cls, other):
        return cls(
            config_filename=other.config_filename,
            cores=other.cores,
        )

    @classmethod
    def from_cap_db_task(cls, other, **kwargs):
        return cls(
            config_filename=other.config_filename,
            cores=other.cores,
            **kwargs,
        )

    def version_exists(self, version):
        """Return True iff this verion of this module already exists."""
        clone = type(self).from_cap_db_task(self, check_versions=False)
        clone.version = lambda: version
        return clone.complete()


class CapTask(BaseCapTask):
    """Base class for CAP2 tasks.

    Currently implements some basic shared logic.
    """
    sample_name = luigi.Parameter()  # Name of the sample being processed
    pe1 = luigi.Parameter()  # Local filepath to read 1
    pe2 = luigi.Parameter()  # Local filepath to read 2 or None if thsi is single ended
    data_type = luigi.Parameter(default='short_read')

    def get_target(self, field_name, ext):
        """Return a luigi LocalTarget for this instance."""
        filename = '.'.join([
            self.sample_name, self.module_name(), self.short_version_hash(), field_name, ext
        ])
        filepath = join(self.config.out_dir, self.sample_name, filename)
        target = luigi.LocalTarget(filepath)
        target.makedirs()
        return target

    @property
    def paired(self):
        """Return true iff this instance is processing paired end data."""
        return self.pe2 and self.data_type == 'short_read'

    @classmethod
    def from_sample(cls, sample, config_path, cores=1):
        """Return an instance of this module from a Sample."""
        return cls(
            pe1=sample.r1,
            pe2=sample.r2,
            sample_name=sample.name,
            config_filename=config_path,
            cores=cores,
            data_type=sample.kind
        )

    @classmethod
    def from_cap_task(cls, other, **kwargs):
        out = cls(
            pe1=other.pe1,
            pe2=other.pe2,
            sample_name=other.sample_name,
            config_filename=other.config_filename,
            cores=other.cores,
            max_ram=other.max_ram,
            data_type=other.data_type,
            **kwargs,
        )
        return out

    def __str__(self):
        try:
            module_name = self.module_name()
            short_hash = self.short_version_hash()
            return f'<CapTask::{module_name}::{short_hash} {self.sample_name}/>'
        except:
            return repr(self)

    def version_exists(self, version_str, version_hash):
        """Return True iff this verion of this module already exists."""
        clone = type(self).from_cap_task(self, check_versions=False)
        clone.version_override = version_str, version_hash
        is_complete = clone.complete()
        return is_complete


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

    @classmethod
    def from_cap_group_task(cls, other, **kwargs):
        return cls(
            group_name=other.group_name,
            config_filename=other.config_filename,
            cores=other.cores,
            max_ram=other.max_ram,
            samples=other.samples,
            **kwargs,
        )

    def version_exists(self, version):
        """Return True iff this verion of this module already exists."""
        clone = type(self).from_cap_group_task(self, check_versions=False)
        clone.version = lambda: version
        return clone.complete()
