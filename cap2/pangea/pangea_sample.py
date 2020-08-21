import luigi
import random
import subprocess
import os
from requests.exceptions import HTTPError

from pangea_api import (
    Knex,
    User,
    Organization,
)

from sys import stderr
from os.path import join, isfile

from ..sample import Sample


class PangeaSampleError(Exception):
    pass


class PangeaSample:

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
        self.r1 = f'downloaded_data/{self.name}.R1.fq.gz'
        self.r2 = f'downloaded_data/{self.name}.R2.fq.gz'
        self.cap_sample = Sample(self.name, self.r1, self.r2)

    def has_reads(self):
        for name in ['raw::raw_reads', 'raw_reads']:
            try:
                self.sample.analysis_result(name).get()
                return True
            except HTTPError:
                continue
        return False

    def has_clean_reads(self):
        for name in ['cap2::clean_reads']:
            try:
                self.sample.analysis_result(name).get()
                return True
            except HTTPError:
                continue
        return False

    def get_clean_reads(self):
        module_name = 'cap2::clean_reads'
        try:
            ar = self.sample.analysis_result(module_name).get()
        except HTTPError:
            raise PangeaSampleError(f'Could not load analysis result "{module_name}" for pangea object "{self.sample.name}"')
        for field_name in self.wrapped.output().keys():
            try:
                ar.field(field_name).get()
            except HTTPError:
                raise PangeaSampleError(f'Could not load analysis result field "{field_name}" for analysis result "{ar.module_name}"')
        return ar

    def download(self):
        print('DOWNLOADING READS')
        try:
            ar = self.sample.analysis_result('raw::raw_reads').get()
        except HTTPError:
            ar = self.sample.analysis_result('raw_reads').get()
        r1 = ar.field('read_1').get()
        r2 = ar.field('read_2').get()
        os.makedirs('downloaded_data', exist_ok=True)
        if not isfile(self.r1):
            r1.download_file(filename=self.r1)
        if not isfile(self.r2):
            r2.download_file(filename=self.r2)


class PangeaGroup:

    def __init__(self, grp_name, email, password, endpoint, org_name):
        self.knex = Knex(endpoint)
        User(self.knex, email, password).login()
        org = Organization(self.knex, org_name).get()
        self.grp = org.sample_group(grp_name).get()
        self.name = grp_name

    def pangea_samples(self, randomize=False):
        if randomize:
            samples = list(self.grp.get_samples())
            random.shuffle(samples)
        else:
            samples = self.grp.get_samples()
        for sample in samples:
            psample = PangeaSample(
                sample.name,
                None,
                None,
                None,
                None,
                None,
                knex=self.knex,
                sample=sample,
            )
            if psample.has_reads():
                yield psample

    def cap_samples(self):
        for sample in self.pangea_samples():
            yield sample.cap_sample
