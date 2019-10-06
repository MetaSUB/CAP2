
import luigi

from shutil import rmtree
from os.path import join, dirname, isfile, isdir
from unittest import TestCase

from cap2.pipeline.short_read.krakenuniq import KrakenUniq

RAW_READS_1 = join(dirname(__file__), 'data/zymo_pos_cntrl.r1.fq.gz')
RAW_READS_2 = join(dirname(__file__), 'data/zymo_pos_cntrl.r2.fq.gz')
TEST_CONFIG = join(dirname(__file__), 'data/test_config.yaml')


class DummyTaxonomicDB(luigi.ExternalTask):

    @property
    def krakenuniq_db(self):
        return join(dirname(__file__), 'data/')

    def output(self):
        return luigi.LocalTarget(join(dirname(__file__), 'data/'))


class TestShortRead(TestCase):

    def test_invoke_krakenuniq(self):
        instance = KrakenUniq(
            pe1=RAW_READS_1,
            pe2= RAW_READS_2,
            sample_name='test_sample',
            config_filename=TEST_CONFIG
        )
        instance.db = DummyTaxonomicDB()
        luigi.build([instance], local_scheduler=True)
        self.assertTrue(isfile(instance.output()['report'].path))
        self.assertTrue(isfile(instance.output()['read_assignments'].path))
