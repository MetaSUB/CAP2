import luigi
import subprocess

from pangea_api import (
    Knex,
    User,
    Organization,
)

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


class PangeaLoadTaskError(Exception):
    pass


def pangea_module_name(module):
    return module.module_name()


class PangeaBaseLoadTask(BaseCapTask):
    wraps = luigi.Parameter()
    endpoint = luigi.Parameter(default='https://pangea.gimmebio.com')

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.wrapped_module = None
        self.requires_reads = False
        user = luigi.configuration.get_config().get('pangea', 'user')
        password = luigi.configuration.get_config().get('pangea', 'password')
        self.org_name = luigi.configuration.get_config().get('pangea', 'org_name')
        self.grp_name = luigi.configuration.get_config().get('pangea', 'grp_name')
        self.knex = Knex(self.endpoint)
        User(self.knex, user, password).login()
        org = Organization(self.knex, self.org_name).get()
        self.grp = org.sample_group(self.grp_name).get()
        self.upload_allowed = True
        self.download_only = False

    def module_name(self):
        return 'pangea_load_task_' + self.wrapped.module_name()

    @classmethod
    def version(cls):
        return 'v1.1.0'

    @classmethod
    def dependencies(cls):
        return []

    def output(self):
        wrapped_out = self.wrapped.output()
        wrapped_out['upload_flag'] = self.get_target('uploaded', 'flag')
        return wrapped_out

    def results_available(self):
        """Check for results on Pangea."""
        try:
            ar = self.pangea_obj.analysis_result(pangea_module_name(self.wrapped)).get()
        except HTTPError:
            return False
        for field_name in self.wrapped.output().keys():
            try:
                ar.field(field_name).get()
            except HTTPError:
                return False
        return True

    def get_results(self):
        module_name = pangea_module_name(self.wrapped)
        try:
            ar = self.pangea_obj.analysis_result(module_name).get()
        except HTTPError:
            raise PangeaLoadTaskError(f'Could not load analysis result "{module_name}" for pangea object "{self.pangea_obj.name}"')
        for field_name in self.wrapped.output().keys():
            try:
                ar.field(field_name).get()
            except HTTPError:
                raise PangeaLoadTaskError(f'Could not load analysis result field "{field_name}" for analysis result "{ar.module_name}"')
        return True

    def _download_results(self):
        ar = self.pangea_obj.analysis_result(pangea_module_name(self.wrapped)).get()
        for field_name, local_target in self.wrapped.output().items():
            field = ar.field(field_name).get()
            field.download_file(filename=local_target.path)
        open(self.output()['upload_flag'].path, 'w').close()  # we do this just for consistency. If we downloaded the results it means they were uploaded at some point

    def _upload_results(self):
        replicate = f'{self.wrapped.version()} {self.wrapped.short_version_hash()}'
        ar = self.pangea_obj.analysis_result(
            pangea_module_name(self.wrapped),
            replicate=replicate,
        ).idem()
        for field_name, local_target in self.wrapped.output().items():
            field = ar.field(field_name).idem()
            field.upload_file(local_target.path)
        open(self.output()['upload_flag'].path, 'w').close()

    def _run(self):
        """If the results are present download and return them.

        Otherwise run the wrapped module, upload the results, and then return.
        """
        if self.results_available():
            self._download_results()
        elif self.upload_allowed:
            self._upload_results()
        elif not self.upload_allowed:
            open(self.output()['upload_flag'].path, 'w').close()


class PangeaLoadTask(PangeaBaseLoadTask, CapTask):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.sample = self.grp.sample(self.sample_name).get()
        self.pangea_obj = self.sample
        self._wrapped = None

    @property
    def wrapped(self):
        if self._wrapped:
            return self._wrapped
        instance = self.wrapped_module(
            pe1=self.pe1,
            pe2=self.pe2,
            sample_name=self.sample_name,
            config_filename=self.config_filename,
            cores=self.cores,
        )
        instance.pre_run_hooks.append(self._download_reads)
        self._wrapped = instance
        return self._wrapped

    def _download_reads(self):
        if self.requires_reads:
            PangeaSample(
                self.sample.name,
                None,
                None,
                None,
                None,
                None,
                knex=self.knex,
                sample=self.sample,
            ).download()

    def requires(self):
        if self.results_available():
            print('RESULTS AVAILABLE')
            return None
        if not self.download_only:
            print('RESULTS WRAPPED', self.wrapped.module_name())
            return self.wrapped
        raise PangeaLoadTaskError('Running tasks is not permitted AND results are not available')



class PangeaGroupLoadTask(PangeaBaseLoadTask, CapGroupTask):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        assert self.grp_name == self.group_name
        self.pangea_obj = self.grp
        self.module_requires_reads = {}

    @property
    def wrapped(self):
        instance = self.wrapped_module.from_samples(
            self.group_name, self.samples, self.config_filename
        )
        instance._make_req_module = self._make_req_module
        return instance

    def _make_req_module(self, module_type, pe1, pe2, sample_name, config_filename):
        task = PangeaLoadTask(
            pe1=pe1,
            pe2=pe2,
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
        return self.wrapped
