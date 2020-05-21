import luigi
import subprocess
import os

from pangea_api import (
    Knex,
    User,
    Organization,
)

from sys import stderr
from os.path import join, isfile


class PangeaSample:
    endpoint = luigi.Parameter(default='https://pangea.gimmebio.com')
    email = luigi.Parameter()
    password = luigi.Parameter()

    def __init__(self, sample_name, email, password, endpoint, org_name, grp_name, knex=None, sample=None):
        if not knex:
            knex = Knex(endpoint)
            User(knex, email, password).login()
        self.sample = sample
        if self.sample is None:
            org = Organization(knex, org_name).get()
            grp = org.sample_group(grp_name).get()
            self.sample = grp.sample(sample_name).get()
        self.name = sample_name

    def download(self):
        ar = self.sample.analysis_result('raw::raw_reads').get()
        r1 = ar.field('read_1').get()
        r2 = ar.field('read_2').get()
        self.r1 = f'downloaded_data/{self.name}.R1.fq.gz'
        self.r2 = f'downloaded_data/{self.name}.R2.fq.gz'
        os.makedirs('downloaded_data', exist_ok=True)
        if not isfile(self.r1):
            r1.download_file(filename=self.r1)
        if not isfile(self.r2):
            r2.download_file(filename=self.r2)


class PangeaGroup:
    endpoint = luigi.Parameter(default='https://pangea.gimmebio.com')
    email = luigi.Parameter()
    password = luigi.Parameter()

    def __init__(self, grp_name, email, password, endpoint, org_name):
        self.knex = Knex(endpoint)
        User(self.knex, email, password).login()
        org = Organization(self.knex, org_name).get()
        self.grp = org.sample_group(grp_name).get()
        self.name = grp_name

    def samples(self):
        for sample in self.grp.get_samples():
            yield PangeaSample(
                sample.name,
                None,
                None,
                None,
                None,
                None,
                knex=self.knex,
                sample=sample,
            )
