import luigi
import subprocess
import json
import logging
from pangea_api import (
    Knex,
    User,
    Organization,
    Pipeline,
)
from pangea_api.blob_constructors import sample_from_uuid, sample_group_from_uuid

from sys import stderr
from os.path import join, basename
from requests.exceptions import HTTPError

from .pangea_sample import PangeaSample

from ..pipeline.utils.cap_task import (
    BaseCapTask,
    CapTask,
    CapGroupTask,
)
from ..pipeline.config import PipelineConfig
from ..pipeline.preprocessing import FastQC

logger = logging.getLogger('cap2')

PANGEA_URL = 'https://pangea.gimmebio.com'
_GLOBAL_REGISTRY_OF_PANGEA_CAP_TASK_TYPES = {}


def get_pangea_config(key, default=None):
    val = luigi.configuration.get_config().get('pangea', key)
    if val is None and default is not None:
        val = default
    return val


class PangeaLoadTaskError(Exception):
    pass


class PangeaBaseCapTaskLuigiTask(luigi.Task):
    pass


class PangeaBaseCapTaskCapTask(PangeaBaseCapTaskLuigiTask, CapTask):
    pass


class PangeaBaseCapTaskMetaClass(type):
    """Metaclass for PangeaBaseCapTask that sends class attribute requests to the wrapped type."""

    def __getattr__(cls, key):
        return getattr(cls.wrapped_type, key)

    def __instancecheck__(cls, instance):
        """Sppof isinstance. See __class__ in PBCT below for why."""
        return isinstance(instance, PangeaBaseCapTaskLuigiTask)


class PangeaBaseCapTask(metaclass=PangeaBaseCapTaskMetaClass):
    """Handle comunication and i/o with Pangea for a CapTask.

    PangeaBaseCapTask is an abstract class twice over. First,
    either PangeaGroupCapTask or PangeaCapTask should be used.
    Second, types should be dynamically generated from CapTasks.

    download_only, if true do not run the module, just download results from pangea (if present)
    upload_allowed, if false do not attempt to uplod new results to panga
    """
    wrapped_type = None

    @property
    def __class__(self):
        """Luigi uses hard instance checks to determine if something is
        a task but we only have a duck typed class here. By overwriting
        this method we can trick the luigi scheduler into allowing this
        task to be run.

        This is not free and can introduce a number of problems. The most
        obvious problem is that isinstance(<an instance of PBCT>, PBCT)
        will return False. We can get around this in part by modifying
        PBCTMC as above.
        """
        return PangeaBaseCapTaskLuigiTask

    @classmethod
    def new_task_type(cls, cap_task_type):
        """Return a new task type that wraps the specified `cap_task_type`."""
        name = f'{cls.__name__}__{cap_task_type.__name__}'
        try:
            return _GLOBAL_REGISTRY_OF_PANGEA_CAP_TASK_TYPES[name]
        except KeyError:
            _GLOBAL_REGISTRY_OF_PANGEA_CAP_TASK_TYPES[name] = type(
                name,
                (cls,),
                {'wrapped_type': cap_task_type}
            )
        return _GLOBAL_REGISTRY_OF_PANGEA_CAP_TASK_TYPES[name]

    def __init__(self, *args, **kwargs):
        self.wrapped_instance = None
        self.requires_reads = kwargs.pop('requires_reads', False)

        self.upload_allowed = get_pangea_config('upload_allowed', True)
        self.download_only = get_pangea_config('download_only', False)
        self.name_is_uuid = get_pangea_config('name_is_uuid')
        self.endpoint = get_pangea_config('pangea_endpoint', PANGEA_URL)
        self.knex = Knex(self.endpoint)
        user = get_pangea_config('user')
        if user:
            User(self.knex, user, get_pangea_config('password')).login()
        if self.name_is_uuid:
            self.org_name = None
            self.grp_name = None
            self.grp = None
        else:
            self.org_name = get_pangea_config('org_name')
            self.grp_name = get_pangea_config('grp_name')
            org = Organization(self.knex, self.org_name).get()
            self.grp = org.sample_group(self.grp_name).get()

        self.wrapped_instance = self.wrapped_type(*args, **kwargs)

    def __getattr__(self, key):
        if key == 'wrapped_instance':  # only occurs in the constructor
            return None
        if self.wrapped_instance:
            return getattr(self.wrapped_instance, key)

    def __setattr__(self, key, val):
        if self.wrapped_instance and hasattr(self.wrapped_instance, key):
            setattr(self.wrapped_instance, key, val)
        else:
            self.__dict__[key] = val

    def output(self):
        wrapped_out = self.wrapped_instance.output()
        wrapped_out['upload_flag'] = self.get_target('uploaded', 'flag')
        return wrapped_out

    def results_available_locally(self):
        return self.wrapped_instance.complete()

    def results_available(self):
        """Check for results on Pangea."""
        try:
            self.get_results()
        except PangeaLoadTaskError:
            return False
        return True

    def _get_analysis_result(self):
        try:
            ar = self.pangea_obj.analysis_result(
                self.module_name(),
                replicate=self._replicate()
            ).get()
        except HTTPError:
            msg = (
                f'Could not load analysis result "{self.module_name()}" '
                f'for pangea object "{self.pangea_obj.name}"'
            )
            raise PangeaLoadTaskError(msg)
        return ar

    def get_results(self):
        ar = self._get_analysis_result()
        for field_name in self.wrapped_instance.output().keys():
            try:
                ar.field(field_name).get()
            except HTTPError:
                msg = (
                    f'Could not load analysis result field "{field_name}" '
                    f'for analysis result "{ar.module_name}"'
                )
                raise PangeaLoadTaskError(msg)

    def _download_results(self):
        ar = self._get_analysis_result()
        for field_name, local_target in self.wrapped_instance.output().items():
            field = ar.field(field_name).get()
            field.download_file(filename=local_target.path)
        open(self.output()['upload_flag'].path, 'w').close()  # we do this just for consistency. If we downloaded the results it means they were uploaded at some point

    def _replicate(self):
        return f'{self.version()} {self.short_version_hash()}'

    def _upload_results(self):
        metadata = json.loads(open(self.get_run_metadata_filepath()).read())
        ar = self.pangea_obj.analysis_result(
            self.module_name(), replicate=self._replicate()
        ).idem()
        ar.metadata = metadata
        ar.save()
        for field_name, local_target in self.wrapped_instance.output().items():
            field = ar.field(field_name).idem()
            field.upload_file(local_target.path)
        open(self.output()['upload_flag'].path, 'w').close()

    def register_module(self):
        """Register this tasks module with Pangea."""
        pipeline_name = '::'.join(self.module_name().split('::')[:-1])
        pipeline_module_name = self.module_name().split('::')[-1]
        try:
            pipeline = Pipeline(self.knex, pipeline_name).idem()
        except:
            logger.error(f'Failed to make Pipeline with name: "{pipeline_name}"')
            raise
        try:
            module = pipeline.module(pipeline_module_name, self._replicate())
            if not module.exists():
                module_description = self.module_description.strip()
                module.description = module_description.split('\n')[0]
                module.long_description = module_description
                module.metadata = {
                    'version': self.version(),
                    'version_hash': self.version_hash(),
                    'version_tree': self.version_tree(),
                    'short_version_hash': self.short_version_hash(),
                }
                module = module.create()
            else:
                module = module.get()
        except:
            msg = (
                f'Failed to make PipelineModule for pipeline {pipeline} with'
                f'\nname: "{pipeline_module_name}"'
                f'\ndescription: "{module_description}"'
                f'\nversion: "{self._replicate()}"'
                f'\nmetadata: "{json.dumps(module.metadata)}"'
            )
            logger.error(msg)
            raise

    def is_type_of_cap_task(self, cap_task_type):
        """Return True iff self is of cap_task_type.

        Check the wrapped type not this itself.
        """
        try:
            return isinstance(self.wrapped_instance, cap_task_type)
        except TypeError:
            return False

class PangeaCapTask(PangeaBaseCapTask):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if self.name_is_uuid:
            self.sample = sample_from_uuid(self.knex, self.sample_name)
        else:
            self.sample = self.grp.sample(self.sample_name).get()
        self.pangea_obj = self.sample

        # call results available to find any allowed versions
        # that might be present and set the wrapped_instance
        # accordingly. Not importatn if the result is not
        # available
        self.results_available()

    def _download_reads(self):
        PangeaSample(
            self.sample_name,
            None,
            None,
            None,
            None,
            None,
            knex=self.knex,
            sample=self.sample,
        ).download()

    def results_available_for_version(self, version_str, version_hash):
        """Check for results of a specific version on Pangea.

        Side effect: if the results are found we set the wrapped
        instance to point at those results.
        """
        original_wrapped_task = self.wrapped_instance
        clone = self.wrapped_type.from_cap_task(self.wrapped_instance, check_versions=False)
        clone.version_override = version_str, version_hash
        self.wrapped_instance = clone
        try:
            self.get_results()
        except PangeaLoadTaskError:
            # reset the version if the given version is not found
            self.wrapped_instance = original_wrapped_task
            return False
        # if results are available we do NOT reset the version
        return True

    def results_available(self):
        """Check for results on Pangea."""
        current_version_available = super().results_available()
        if current_version_available:
            return True
        for version_str, version_hash in self.config.allowed_versions(self):
            if self.results_available_for_version(version_str, version_hash):
                return True
        return False

    def requires(self):
        if self.results_available():  # the wrapped result is on pangea
            return None
        elif not self.download_only:  # the W.R. is not on pangea but we are allowed to run it
            return self.wrapped_instance.requires()
        elif self.results_available_locally():  # we are not allowed to run the W.R. but it is already done
            return self.wrapped_instance.requires()
        raise PangeaLoadTaskError('Running tasks is not permitted AND results are not available')

    def run(self):
        """If the results are present download and return them.

        Otherwise run the wrapped module, upload the results, and then return.
        """
        if self.results_available():
            logger.debug(f'results are available for {self}, downloading...')
            self._download_results()
        else:
            if self.requires_reads:
                logger.debug(f'reads are required for {self}, downloading...')
                self._download_reads()
            logger.debug(f'running wrapped_instance for {self}, running...')
            self.register_module()  # only register this module if it seems like everything else is working
            self.wrapped_instance.run()  # see above. we reassign the original CT._run to CT._wrapped_run
            if self.upload_allowed:
                logger.debug(f'uploading results for {self}, uploading...')
                self._upload_results()
            else:
                open(self.output()['upload_flag'].path, 'w').close()

    def __str__(self):
        module_name = self.module_name()
        short_hash = self.short_version_hash()
        return f'<PangeaCapTask::{module_name}::{short_hash} {self.sample_name}/>'

    def __repr__(self):
        module_name = self.module_name()
        short_hash = self.short_version_hash()
        return f'<PangeaCapTask::{module_name}::{short_hash} {self.sample_name}/>'


class PangeaGroupCapTask(PangeaBaseCapTask):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if self.name_is_uuid:
            self.grp = sample_group_from_uuid(self.knex, self.grp_name)
        self.pangea_obj = self.grp
        self.module_requires_reads = {}

    @property
    def wrapped(self):
        instance = self.wrapped_module.from_samples(
            self.grp_name, self.samples, self.config_filename
        )
        instance._make_req_module = self._make_req_module
        return instance

    def _make_req_module(self, module_type, pe1, pe2, sample_name, config_filename):
        task = PangeaLoadTask(
            pe1=pe1,
            pe2=pe2,
            wraps=module_type.module_name(),
            sample_name=sample_name,
            config_filename=config_filename,
        )
        task.wrapped_module = module_type
        task.requires_reads = self.module_requires_reads.get(module_type, False)
        return task

    def requires(self):
        if self.results_available():
            return None
        if self.requires_reads:
            raise NotImplementedError('Group modules that rely directly on reads not yet supported.')
        return self.wrapped_instance.requires()

    def _run(self):
        """If the results are present download and return them.

        Otherwise run the wrapped module, upload the results, and then return.
        """
        if self.results_available():
            self._download_results()
        else:
            self.wrapped_instance._run()
            if self.upload_allowed:
                self._upload_results()
            else:
                open(self.output()['upload_flag'].path, 'w').close()
