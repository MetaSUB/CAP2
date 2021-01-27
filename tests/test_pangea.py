
import luigi

from os import environ

from time import time
from cap2.pangea.load_task import PangeaCapTask
from cap2.pipeline.preprocessing import FastQC
from unittest import TestCase
from cap2.pangea.cli import set_config
from cap2.pangea.api import get_task_list_for_sample
from cap2.pangea.pangea_sample import PangeaSample
from os.path import join, dirname, isfile, isdir, abspath

from pangea_api.blob_constructors import sample_from_uuid
from pangea_api import Knex, Organization, User

RAW_READS_1 = join(dirname(__file__), 'data/zymo_pos_cntrl.r1.fq.gz')
RAW_READS_2 = join(dirname(__file__), 'data/zymo_pos_cntrl.r2.fq.gz')

PANGEA_ENDPOINT = 'https://pangea.gimmebio.com'
PANGEA_USER = 'cap2tester@fake.com'
PANGEA_PASS = environ['CAP2_PANGEA_TEST_PASSWORD']


def create_test_sample():
    timestamp = int(time())
    knex = Knex(PANGEA_ENDPOINT)
    User(knex, PANGEA_USER, PANGEA_PASS).login()
    org = Organization(knex, 'MetaSUB Consortium').get()
    lib = org.sample_group('CAP2 Test Sandbox').get()
    sample = lib.sample(f'CAP2 Test Sample {timestamp}').create()
    reads_ar = sample.analysis_result('raw::raw_reads').create()
    r1 = reads_ar.field('read_1').create()
    r1.upload_file(RAW_READS_1)
    r2 = reads_ar.field('read_2').create()
    r2.upload_file(RAW_READS_2)
    return sample

PANGEA_SAMPLE = create_test_sample()


class TestPangea(TestCase):
    """Test the CAP2 API, essentially integration tests."""

    def test_get_pangea_cap_task_properties(self):
        c = PangeaCapTask.new_task_type(FastQC)
        self.assertTrue(c.version())
        self.assertTrue(c.version_tree())

    def test_set_pangea_cap_task_properties(self):
        c = PangeaCapTask.new_task_type(FastQC)
        c.max_ram = 1000
        self.assertEqual(c.max_ram, 1000)

    def test_pre_task_list_pangea(self):
        psample = PangeaSample(
            PANGEA_SAMPLE.uuid,
            None,
            None,
            None,
            None,
            None,
            knex=PANGEA_SAMPLE.knex,
            sample=PANGEA_SAMPLE,
        )
        set_config(PANGEA_ENDPOINT, PANGEA_USER, PANGEA_PASS, '', '', name_is_uuid=True)
        tasks = get_task_list_for_sample(psample, 'pre')
        self.assertEqual(len(tasks), 1)
        self.assertTrue( isinstance(tasks[0], PangeaCapTask))
        self.assertFalse(isinstance(tasks[0].ec_reads, PangeaCapTask))
        self.assertTrue( isinstance(tasks[0].ec_reads.nonhuman_reads, PangeaCapTask))
        self.assertFalse(isinstance(tasks[0].ec_reads.nonhuman_reads.mouse_removed_reads, PangeaCapTask))
        self.assertFalse(isinstance(tasks[0].ec_reads.nonhuman_reads.mouse_removed_reads.adapter_removed_reads, PangeaCapTask))
        self.assertTrue( isinstance(tasks[0].ec_reads.nonhuman_reads.mouse_removed_reads.adapter_removed_reads.reads, PangeaCapTask))

    def test_pct_instance_is_pct_and_luigi_task(self):
        psample = PangeaSample(
            PANGEA_SAMPLE.uuid,
            None,
            None,
            None,
            None,
            None,
            knex=PANGEA_SAMPLE.knex,
            sample=PANGEA_SAMPLE,
        )
        set_config(PANGEA_ENDPOINT, '', '', '', '', name_is_uuid=True)
        pct_type = PangeaCapTask.new_task_type(FastQC)
        pct_instance = pct_type(
            pe1=psample.r1,
            pe2=psample.r2,
            sample_name=psample.name,
            config_filename='',
        )
        self.assertTrue(isinstance(pct_instance, PangeaCapTask))
        self.assertTrue(isinstance(pct_instance, luigi.Task))
        self.assertTrue(isinstance(pct_instance, pct_type))

    def test_pangea_cap_task_instance(self):
        psample = PangeaSample(
            PANGEA_SAMPLE.uuid,
            None,
            None,
            None,
            None,
            None,
            knex=PANGEA_SAMPLE.knex,
            sample=PANGEA_SAMPLE,
        )
        set_config(PANGEA_ENDPOINT, '', '', '', '', name_is_uuid=True)
        pct_type = PangeaCapTask.new_task_type(FastQC)
        pct_instance = pct_type(
            pe1=psample.r1,
            pe2=psample.r2,
            sample_name=psample.name,
            config_filename='',
        )
        self.assertTrue(pct_instance.wrapped_instance)

    def test_run_pangea_on_sample(self):
        psample = PangeaSample(
            PANGEA_SAMPLE.uuid,
            None,
            None,
            None,
            None,
            None,
            knex=PANGEA_SAMPLE.knex,
            sample=PANGEA_SAMPLE,
        )
        set_config(PANGEA_ENDPOINT, PANGEA_USER, PANGEA_PASS, '', '', name_is_uuid=True)
        tasks = get_task_list_for_sample(psample, 'qc')
        luigi.build(tasks, local_scheduler=True)
        self.assertTrue(PANGEA_SAMPLE.analysis_result('cap2::fastqc').exists())
