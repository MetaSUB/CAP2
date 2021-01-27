
import luigi

from cap2.pangea.load_task import PangeaCapTask
from cap2.pipeline.preprocessing import FastQC
from unittest import TestCase
from cap2.pangea.cli import set_config
from cap2.pangea.api import get_task_list_for_sample
from cap2.pangea.pangea_sample import PangeaSample

from pangea_api.blob_constructors import sample_from_uuid
from pangea_api import Knex


PANGEA_SAMPLE_UUID = 'fe502684-2068-4d10-84ca-8f3aab87221f'
PANGEA_ENDPOINT = 'https://pangea.gimmebio.com'


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

    def test_pct_instance_is_pct_and_luigi_task(self):
        knex = Knex(PANGEA_ENDPOINT)
        sample = sample_from_uuid(knex, PANGEA_SAMPLE_UUID)
        psample = PangeaSample(
            sample.uuid,
            None,
            None,
            None,
            None,
            None,
            knex=knex,
            sample=sample,
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
        knex = Knex(PANGEA_ENDPOINT)
        sample = sample_from_uuid(knex, PANGEA_SAMPLE_UUID)
        psample = PangeaSample(
            sample.uuid,
            None,
            None,
            None,
            None,
            None,
            knex=knex,
            sample=sample,
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
        knex = Knex(PANGEA_ENDPOINT)
        sample = sample_from_uuid(knex, PANGEA_SAMPLE_UUID)
        psample = PangeaSample(
            sample.uuid,
            None,
            None,
            None,
            None,
            None,
            knex=knex,
            sample=sample,
        )
        set_config(PANGEA_ENDPOINT, '', '', '', '', name_is_uuid=True)
        tasks = get_task_list_for_sample(psample, 'qc')
        luigi.build(tasks, local_scheduler=True)
