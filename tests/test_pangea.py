
import luigi

from shutil import rmtree

from os import environ, remove

from time import time, sleep
from cap2.pangea.load_task import PangeaCapTask
from cap2.pipeline.preprocessing import FastQC
from unittest import TestCase, skip
from cap2.pangea.cli import set_config
from cap2.pangea.api import get_task_list_for_sample, wrap_task, recursively_wrap_task
from cap2.pangea.pangea_sample import PangeaSample
from os.path import join, dirname, isfile, isdir, abspath

from pangea_api.blob_constructors import sample_from_uuid
from pangea_api import Knex, Organization, User
from pangea_api.work_orders import WorkOrderProto, JobOrder

from .test_versions import (
    FlagTaskVersionA,
    FlagTaskVersionB,
    TaskThatReliesOnFlagTask,
)

RAW_READS_1 = join(dirname(__file__), 'data/zymo_pos_cntrl.r1.fq.gz')
RAW_READS_2 = join(dirname(__file__), 'data/zymo_pos_cntrl.r2.fq.gz')
TEST_CONFIG = join(dirname(__file__), 'data/test_config.yaml')

PANGEA_ENDPOINT = 'https://pangeabio.io'
PANGEA_USER = 'cap2tester@fake.com'
PANGEA_PASS = environ['CAP2_PANGEA_TEST_PASSWORD']
WOP_NAME = '374304a9-f5c1-4e44-a87b-9e8032d03d09'


def data_file(fname):
    return join(dirname(__file__), 'data', fname)


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
PANGEA_SAMPLE_2 = create_test_sample()


class DummyFastKraken2(luigi.ExternalTask):

    def output(self):
        return {
            'report': luigi.LocalTarget(data_file('kraken2_report.tsv')),
            'read_assignments': luigi.LocalTarget(data_file('kraken2_read_assignments.tsv')),
        }


class TestPangea(TestCase):
    """Test the CAP2 API, essentially integration tests."""

    def setUpClass():
        pass
        try:
            rmtree('.pangea_api_cache')
        except FileNotFoundError:
            pass

    def tearDownClass():
        pass
        try:
            rmtree('test_out')
        except FileNotFoundError:
            pass

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
        tasks = get_task_list_for_sample(psample, 'fast')
        tasks = [tasks[1]]  # just basic stats class
        tasks[0].taxa = DummyFastKraken2()
        luigi.build(tasks, local_scheduler=True)
        self.assertTrue(PANGEA_SAMPLE.analysis_result('cap2::basic_sample_stats').exists())

    def test_run_pangea_on_sample_with_work_order(self):
        psample = PangeaSample(
            PANGEA_SAMPLE_2.uuid,
            None,
            None,
            None,
            None,
            None,
            knex=PANGEA_SAMPLE_2.knex,
            sample=PANGEA_SAMPLE_2,
        )

        wop = WorkOrderProto.from_name(PANGEA_SAMPLE_2.knex, WOP_NAME)
        wo = wop.create_work_order_for_sample(PANGEA_SAMPLE_2)
        set_config(PANGEA_ENDPOINT, PANGEA_USER, PANGEA_PASS, '', '',
                   name_is_uuid=True, work_order_name=WOP_NAME)
        tasks = get_task_list_for_sample(psample, 'fast')
        tasks = [tasks[1]]  # just basic stats class
        tasks[0].taxa = DummyFastKraken2()
        luigi.build(tasks, local_scheduler=True)
        self.assertTrue(PANGEA_SAMPLE_2.analysis_result('cap2::basic_sample_stats').exists())
        jo = wo.get_job_order_by_name('cap2::basic_sample_stats')
        jo.invalidate_cache()
        jo = JobOrder.from_uuid(wo, jo.uuid)
        self.assertTrue(jo.exists())
        self.assertEqual(jo.status, 'success')

    def test_load_task_versions(self):
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
        wrapped_flag_b = wrap_task(psample, FlagTaskVersionB, config_path=TEST_CONFIG, check_versions=False)
        luigi.build([wrapped_flag_b], local_scheduler=True)
        remove(wrapped_flag_b.flag_filepath)
        self.assertTrue(PANGEA_SAMPLE.analysis_result(
            'cap2::test_flag',
            replicate='B df7ebee60a5c',
        ).exists())

        instance = recursively_wrap_task(psample, TaskThatReliesOnFlagTask, config_path=TEST_CONFIG)
        self.assertEqual(instance.flag.version(), 'B')
        ivt = instance.version_tree()
        cvt = TaskThatReliesOnFlagTask.version_tree()
        self.assertNotEqual(ivt, cvt)
        self.assertEqual(
            instance.version_tree().replace('==B', '==A'),
            TaskThatReliesOnFlagTask.version_tree()
        )
        self.assertNotEqual(instance.version_hash(), TaskThatReliesOnFlagTask.version_hash())

    def test_pangea_versions(self):
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
        wrapped_flag_b = wrap_task(psample, FlagTaskVersionB, config_path=TEST_CONFIG, check_versions=False)
        luigi.build([wrapped_flag_b], local_scheduler=True)
        remove(wrapped_flag_b.flag_filepath)
        self.assertTrue(PANGEA_SAMPLE.analysis_result(
            'cap2::test_flag',
            replicate='B df7ebee60a5c',
        ).exists())

        wrapped_downstream = recursively_wrap_task(psample, TaskThatReliesOnFlagTask, config_path=TEST_CONFIG)
        self.assertIn('B', [x[0] for x in wrapped_downstream.config.allowed_versions(wrapped_flag_b)])
        luigi.build([wrapped_downstream], local_scheduler=True)
        self.assertEqual(wrapped_downstream.flag.version(), 'B')
        self.assertTrue(PANGEA_SAMPLE.analysis_result(
            'cap2::test_task_that_relies_on_flag',
        ).exists())
