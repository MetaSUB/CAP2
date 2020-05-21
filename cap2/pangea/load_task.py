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

from ..pipeline.utils.cap_task import CapTask
from ..pipeline.config import PipelineConfig
from ..pipeline.preprocessing import FastQC


def pangea_module_name(module):
    return module.module_name()


class PangeaLoadTask(CapTask):
    endpoint = luigi.Parameter(default='https://pangea.gimmebio.com')

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.wrapped_module = None
        self.requires_reads = False
        user = luigi.configuration.get_config().get('pangea', 'user')
        password = luigi.configuration.get_config().get('pangea', 'password')
        org_name = luigi.configuration.get_config().get('pangea', 'org_name')
        grp_name = luigi.configuration.get_config().get('pangea', 'grp_name')
        self.s3_bucket_name = luigi.configuration.get_config().get('pangea', 's3_bucket_name')
        self.s3_endpoint_url = luigi.configuration.get_config().get('pangea', 's3_endpoint_url')
        self.s3_profile = luigi.configuration.get_config().get('pangea', 's3_profile')
        self.knex = Knex(self.endpoint)
        User(self.knex, user, password).login()
        org = Organization(self.knex, org_name).get()
        grp = org.sample_group(grp_name).get()
        self.sample = grp.sample(self.sample_name).get()

    @property
    def wrapped(self):
        return self.wrapped_module(
            pe1=self.pe1,
            pe2=self.pe2,
            sample_name=self.sample_name,
            config_filename=self.config_filename,
        )

    def requires(self):
        if self.results_available():
            return None
        if self.requires_reads:
            PangeaSample(
                sample.name,
                None,
                None,
                None,
                None,
                None,
                knex=self.knex,
                sample=self.sample,
            ).download()
        return self.wrapped

    def module_name(self):
        return 'pangea_load_task_' + self.wrapped.module_name()

    def output(self):
        wrapped_out = self.wrapped.output()
        wrapped_out['upload_flag'] = self.get_target('uploaded', 'flag')
        return wrapped_out

    def results_available(self):
        """Check for results on Pangea."""
        try:
            ar = self.sample.analysis_result(pangea_module_name(self.wrapped)).get()
        except HTTPError:
            return False
        for field_name in self.wrapped.output().keys():
            try:
                ar.field(field_name).get()
            except HTTPError:
                return False
        return True

    def _download_results(self):
        ar = self.sample.analysis_result(pangea_module_name(self.wrapped)).get()
        for field_name, local_path in self.wrapped.output().items():
            field = ar.field(field_name).get()
            field.download_file(filename=local_path.path)

    def _uri(self, local_path):
        uri = f's3://{self.s3_bucket_name}/analysis/metasub_cap/v2/{self.sample_name}/{basename(local_path)}'
        return uri

    def _upload_results(self):
        ar = self.sample.analysis_result(pangea_module_name(self.wrapped)).idem()
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
