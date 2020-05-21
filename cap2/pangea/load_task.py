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


def pangea_module_name(module):
    return module.module_name()


class PangeaBaseLoadTask(BaseCapTask):
    endpoint = luigi.Parameter(default='https://pangea.gimmebio.com')

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.wrapped_module = None
        self.requires_reads = False
        user = luigi.configuration.get_config().get('pangea', 'user')
        password = luigi.configuration.get_config().get('pangea', 'password')
        self.org_name = luigi.configuration.get_config().get('pangea', 'org_name')
        self.grp_name = luigi.configuration.get_config().get('pangea', 'grp_name')
        self.s3_bucket_name = luigi.configuration.get_config().get('pangea', 's3_bucket_name')
        self.s3_endpoint_url = luigi.configuration.get_config().get('pangea', 's3_endpoint_url')
        self.s3_profile = luigi.configuration.get_config().get('pangea', 's3_profile')
        self.knex = Knex(self.endpoint)
        User(self.knex, user, password).login()
        org = Organization(self.knex, self.org_name).get()
        self.grp = org.sample_group(self.grp_name).get()

    def module_name(self):
        return 'pangea_load_task_' + self.wrapped.module_name()

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

    def _download_results(self):
        ar = self.pangea_obj.analysis_result(pangea_module_name(self.wrapped)).get()
        for field_name, local_path in self.wrapped.output().items():
            field = ar.field(field_name).get()
            field.download_file(filename=local_path.path)
        open(self.output()['upload_flag'].path, 'w').close()  # we do this just for consistency. If we downloaded the results it means they were uploaded at some point

    def _upload_results(self):
        ar = self.pangea_obj.analysis_result(pangea_module_name(self.wrapped)).idem()
        for field_name, local_path in self.wrapped.output().items():
            uri = self._uri(local_path.path)
            cmd = (
                'aws '
                f'--profile {self.s3_profile} '
                f's3 --endpoint-url={self.s3_endpoint_url} '
                f'cp {local_path.path} {uri}'
            )
            self.run_cmd(cmd)
            field = ar.field(field_name, data={
                '__type__': 's3',
                'uri': uri,
                'endpoint_url': self.s3_endpoint_url,
            }).idem()
        open(self.output()['upload_flag'].path, 'w').close()

    def _run(self):
        """If the results are present download and return them.

        Otherwise run the wrapped module, upload the results, and then return.
        """
        if self.results_available():
            self._download_results()
        else:
            self._upload_results()

class PangeaLoadTask(PangeaBaseLoadTask, CapTask):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.sample = self.grp.sample(self.sample_name).get()
        self.pangea_obj = self.sample

    @property
    def wrapped(self):
        instance = self.wrapped_module(
            pe1=self.pe1,
            pe2=self.pe2,
            sample_name=self.sample_name,
            config_filename=self.config_filename,
        )
        instance.pre_run_hooks.append(self._download_reads)
        return instance

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
            return None
        return self.wrapped

    def _uri(self, local_path):
        uri = f's3://{self.s3_bucket_name}/analysis/metasub_cap/v2/{self.sample_name}/{basename(local_path)}'
        return uri


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

    def _uri(self, local_path):
        name = self.grp_name.lower().replace(' ', '_')
        uri = f's3://{self.s3_bucket_name}/analysis/metasub_cap/v2/groups/{name}/{basename(local_path)}'
        return uri
