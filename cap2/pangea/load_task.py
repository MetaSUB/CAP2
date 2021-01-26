import luigi
import subprocess
import json
from pangea_api import (
    Knex,
    User,
    Organization,
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

PANGEA_URL = 'https://pangea.gimmebio.com'
_GLOBAL_REGISTRY_OF_PANGEA_CAP_TASK_TYPES = {}


def get_pangea_config(key, default=None):
    if default:
        return luigi.configuration.get_config().get('pangea', key, default)
    return luigi.configuration.get_config().get('pangea', key)


class PangeaLoadTaskError(Exception):
    pass


class PangeaBaseCapTaskMetaClass(type):

    def __getattr__(cls, key):
        return getattr(cls.wrapped_type, key)


class PangeaBaseCapTask(metaclass=PangeaBaseCapTaskMetaClass):
    """
    Concept: replace the unweildy PangeaLoadTask with a set of classes
    that dynamically reference the CapTask that they wrap.

    download_only, if true do not run the module, just download results from pangea (if present)
    upload_allowed, if false do not attempt to uplod new results to panga
    """
    wrapped_type = None

    @classmethod
    def new_task_type(cls, cap_task_type):
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
        self.requires_reads = kwargs.pop('requires_reads', False)

        self.upload_allowed = get_pangea_config('upload_allowed', True)
        self.download_only = get_pangea_config('download_only', False)
        self.name_is_uuid = get_pangea_config('name_is_uuid')
        self.endpoint = get_pangea_config('endpoint_url', PANGEA_URL)
        self.knex = Knex(self.endpoint)
        User(self.knex, get_pangea_config('user'), get_pangea_config('password')).login()
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
        return getattr(self.wrapped_instance, key)

    def __setattr__(self, key, val):
        return setattr(self.wrapped_instance, key, val)

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
        for field_name in self.wrapped.output().keys():
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


class PangeaCapTask(PangeaBaseCapTask):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if self.name_is_uuid:
            self.sample = sample_from_uuid(self.knex, self.sample_name)
        else:
            self.sample = self.grp.sample(self.sample_name).get()
        self.pangea_obj = self.sample

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

    def requires(self):
        if self.results_available():  # the wrapped result is on pangea
            return None
        elif not self.download_only:  # the W.R. is not on pangea but we are allowed to run it
            return self.wrapped_instance.requires()
        elif self.results_available_locally():  # we are not allowed to run the W.R. but it is already done
            return self.wrapped_instance.requires()
        raise PangeaLoadTaskError('Running tasks is not permitted AND results are not available')

    def _run(self):
        """If the results are present download and return them.

        Otherwise run the wrapped module, upload the results, and then return.
        """
        if self.results_available():
            self._download_results()
        else:
            if self.requires_reads:
                self._download_reads()
            self.wrapped_instance._run()
            if self.upload_allowed:
                self._upload_results()
            else:
                open(self.output()['upload_flag'].path, 'w').close()


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
