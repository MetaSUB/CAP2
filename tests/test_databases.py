import luigi

from shutil import rmtree
from os.path import join, dirname, isfile, isdir
from unittest import TestCase

from cap2.pipeline.databases.human_removal_db import HumanRemovalDB
from cap2.pipeline.databases.hmp_db import HmpDB
from cap2.pipeline.databases.taxonomic_db import TaxonomicDB
from cap2.pipeline.databases.uniref import Uniref90

GENOME_SAMPLE = join(dirname(__file__), 'data/hg38/genome_sample.fa')
TEST_CONFIG = join(dirname(__file__), 'data/test_config.yaml')


class TestDatabases(TestCase):

    def test_build_human_removal_db(self):
        instance = HumanRemovalDB(config_filename=TEST_CONFIG)
        instance.fastas = [GENOME_SAMPLE]
        luigi.build([instance], local_scheduler=True)
        self.assertTrue(isfile(instance.output().fn))
        rmtree('test_db')

    def test_build_taxonomic_db(self):
        instance = TaxonomicDB(config_filename=TEST_CONFIG)
        instance.kraken_db_dir = ''
        luigi.build([instance], local_scheduler=True)
        self.assertTrue(isfile(instance.output().fn))
        rmtree('test_db')

    def test_build_hmp_db(self):
        instance = HmpDB(config_filename=TEST_CONFIG)
        instance.fastqs = []
        luigi.build([instance], local_scheduler=True)
        self.assertTrue(isfile(instance.output().fn))
        rmtree('test_db')

    def test_build_uniref90_db(self):
        instance = Uniref90(config_filename=TEST_CONFIG)
        instance.fastas = []
        luigi.build([instance], local_scheduler=True)
        self.assertTrue(isfile(instance.output().fn))
        rmtree('test_db')
