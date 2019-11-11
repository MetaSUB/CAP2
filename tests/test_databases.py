import luigi

from shutil import rmtree
from os.path import join, dirname, isfile, isdir
from unittest import TestCase

from cap2.pipeline.databases.human_removal_db import HumanRemovalDB
from cap2.pipeline.databases.hmp_db import HmpDB
from cap2.pipeline.databases.taxonomic_db import TaxonomicDB
from cap2.pipeline.databases.uniref import Uniref90


def data_file(fname):
    return join(dirname(__file__), 'data', fname)


GENOME_SAMPLE = data_file('hg38/genome_sample.fa')
TEST_CONFIG = data_file('test_config.yaml')


class TestDatabases(TestCase):

    def test_build_human_removal_db(self):
        instance = HumanRemovalDB(config_filename=TEST_CONFIG)
        instance.fastas = [GENOME_SAMPLE]
        luigi.build([instance], local_scheduler=True)
        self.assertTrue(isfile(instance.output()['bt2_index_1'].path))
        rmtree('test_db')

    def test_build_taxonomic_db(self):
        instance = TaxonomicDB(config_filename=TEST_CONFIG)
        instance.kraken_db_dir = ''
        # luigi.build([instance], local_scheduler=True)
        # self.assertTrue(isfile(instance.output().path))
        # rmtree('test_db')

    def test_build_hmp_db(self):
        instance = HmpDB(config_filename=TEST_CONFIG)
        instance.fastqs = [
            data_file('hmp/left_retroauricular_crease/SRS024620/SRS024620.denovo_duplicates_marked.trimmed.1.fastq.gz'),
            data_file('hmp/palatine_tonsils/SRS014474/SRS014474.denovo_duplicates_marked.trimmed.1.fastq.gz'),
        ]
        luigi.build([instance], local_scheduler=True)
        self.assertTrue(isfile(instance.output().path))
        rmtree('test_db')

    def test_build_uniref90_db(self):
        instance = Uniref90(config_filename=TEST_CONFIG)
        instance.fastas = [data_file('uniref90/uniref90.sample.fasta.gz')]
        # luigi.build([instance], local_scheduler=True)
        # self.assertTrue(isfile(instance.output().path))
        # rmtree('test_db')
