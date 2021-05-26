import luigi
import random
import subprocess
import os
import logging

from requests.exceptions import HTTPError

from pangea_api import (
    Knex,
    User,
    Organization,
    WorkOrderProto,
    PangeaNotFoundError,
)
from pangea_api.contrib.tagging import Tag
from pangea_api.blob_constructors import sample_from_uuid

from sys import stderr
from os.path import join, isfile, dirname
from os import makedirs

from .sra_utils import sra_to_fastqs
from ..sample import Sample
from ..exceptions import CAPSampleError

logger = logging.getLogger('cap2')


class PangeaSampleError(Exception):
    pass


class PangeaSample:

    def __init__(self, sample_name, email, password, endpoint, org_name, grp_name,
                 kind='short_read', knex=None, sample=None, name_is_uuid=False):
        if not knex:
            knex = Knex(endpoint)
            User(knex, email, password).login()
        self.sample = sample
        if self.sample is None:
            if name_is_uuid:
                self.sample = sample_from_uuid(knex, sample_name)
            else:
                org = Organization(knex, org_name).get()
                grp = org.sample_group(grp_name).get()
                self.sample = grp.sample(sample_name).get()
        self.kind = kind
        self.name = sample_name
        self.sra = f'downloaded_data/{self.name}.sra'
        self.r1 = f'downloaded_data/{self.name}.R1.fq.gz'
        self.r2 = None
        try:
            if self.has_reads():
                if self.kind == 'short_read':
                    self.kind = 'single_short_read'
                    if self.is_paired():
                        self.kind = 'paired_short_read'
            
            if self.kind == 'paired_short_read':
                self.r2 = f'downloaded_data/{self.name}.R2.fq.gz'

            self.cap_sample = Sample(self.name, self.r1, self.r2, kind=self.kind)
        except CAPSampleError:
            self.kind = 'undetermined'

    def has_reads(self):
        for name in ['raw::raw_reads', 'raw_reads']:
            try:
                self.sample.analysis_result(name).get()
                return True
            except PangeaNotFoundError:
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
        logger.info(f'downloading reads for sample: {self.name}')
        try:
            ar = self.sample.analysis_result('raw::raw_reads').get()
        except HTTPError:
            ar = self.sample.analysis_result('raw_reads').get()
        try:
            ar.field('read_1').get()
            self.download_fastqs(ar)
        except HTTPError:
            self.download_sra(ar)

    def is_paired(self):
        """Return True iff this sample contains paired end data."""
        logger.info(f'checking if sample reads are paired: {self.name}')
        try:
            ar = self.sample.analysis_result('raw::raw_reads').get()
        except PangeaNotFoundError:
            ar = self.sample.analysis_result('raw_reads').get()
        try:
            ar.field('read_1').get()
            return ar.field('read_2').exists()
        except PangeaNotFoundError:
            raise CAPSampleError(f'Cannot determine if sample is paired: {self.name}')

    def download_fastqs(self, ar):
        logger.debug(f'downloading fastqs for sample: {self.name}')
        r1 = ar.field('read_1').get()
        makedirs(dirname(self.r1), exist_ok=True)
        if not isfile(self.r1):
            r1.download_file(filename=self.r1)
        r2 = ar.field('read_2').get()
        if not isfile(self.r2):
            r2.download_file(filename=self.r2)

    def download_sra(self, ar):
        logger.debug(f'downloading sra_run for sample: {self.name}')
        sra = ar.field('sra_run').get()
        makedirs(dirname(self.sra), exist_ok=True)
        if not isfile(self.sra):
            sra.download_file(filename=self.sra)
        logger.debug(f'converting sra to fastqs for sample: {self.name}')
        gzipped_files = sra_to_fastqs(self.name, self.sra, dirpath=dirname(self.sra))
        if len(gzipped_files) == 1:
            self.r2 = None
            self.cap_sample.r2 = None


class PangeaGroup:

    def __init__(self, grp_name, email, password, endpoint, org_name):
        self.knex = Knex(endpoint)
        User(self.knex, email, password).login()
        org = Organization(self.knex, org_name).get()
        self.grp = org.sample_group(grp_name).get()
        self.name = grp_name

    def pangea_samples(self, randomize=False, seed=None, kind='short_read', check_for_reads=True):
        if randomize:
            if seed:
                random.seed(seed)
            samples = list(self.grp.get_samples())
            random.shuffle(samples)
        else:
            samples = self.grp.get_samples()
        for sample in samples:
            psample = PangeaSample(
                sample.uuid,
                None,
                None,
                None,
                None,
                None,
                knex=self.knex,
                sample=sample,
                kind=kind,
            )
            if not check_for_reads or psample.has_reads():
                yield psample

    def cap_samples(self):
        for sample in self.pangea_samples():
            yield sample.cap_sample


class PangeaTag:

    def __init__(self, tag_name, email, password, endpoint):
        self.knex = Knex(endpoint)
        User(self.knex, email, password).login()
        self.tag = Tag(self.knex, tag_name).get()
        self.name = tag_name

    def pangea_samples(self, randomize=False, seed=None, n=100):
        if randomize:
            samples = list(self.tag.get_random_samples(n=n))
        else:
            samples = self.tag.get_samples()
        for sample in samples:
            psample = PangeaSample(
                sample.uuid,
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


class PangeaWorkOrder:

    def __init__(self, work_order_uuid, email, password, endpoint):
        self.knex = Knex(endpoint)
        User(self.knex, email, password).login()
        self.wop = WorkOrderProto.from_uuid(self.knex, work_order_uuid)

    def pangea_samples(self, randomize=True, seed=None, n=100):
        samples = []
        for wo in self.wop.get_active_work_orders(random=randomize, max_num=n, not_status='success'):
            if wo.status == 'success':
                continue
            samples.append(wo.get_sample())
        for sample in samples:
            psample = PangeaSample(
                sample.uuid,
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
