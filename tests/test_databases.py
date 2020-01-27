import luigi

from shutil import rmtree
from os.path import join, dirname, isfile, isdir
from unittest import TestCase, skip

from cap2.pipeline.databases.human_removal_db import HumanRemovalDB
from cap2.pipeline.databases.hmp_db import HmpDB
from cap2.pipeline.databases.taxonomic_db import TaxonomicDB
from cap2.pipeline.databases.uniref import Uniref90
from cap2.pipeline.databases.amr_db import GrootDB


def data_file(fname):
    return join(dirname(__file__), 'data', fname)


GENOME_SAMPLE = data_file('hg38/genome_sample.fa')
TEST_CONFIG = data_file('test_config.yaml')


class TestDatabases(TestCase):
    @skip(reason='Groot not complete')
    def test_build_groot_db(self):
        instance = GrootDB(config_filename=TEST_CONFIG)
        instance.msas = data_file('groot_amrs')
        luigi.build([instance], local_scheduler=True)
        self.assertTrue(isdir(instance.output()['groot_index'].path))
        rmtree('test_db')

    @skip(reason="krakenuniq creates dependency conflict")
    def test_build_taxonomic_db(self):
        instance = TaxonomicDB(config_filename=TEST_CONFIG)
        instance.kraken_db_dir = data_file('krakenuniq')
        luigi.build([instance], local_scheduler=True)
        self.assertTrue(isfile(instance.output()['krakenuniq_db_taxa'].path))
        rmtree('test_db')

    def test_build_hmp_db(self):
        instance = HmpDB(config_filename=TEST_CONFIG)
        instance.fastqs = [
            data_file('hmp/left_retroauricular_crease/SRS024620/SRS024620.denovo_duplicates_marked.trimmed.1.fastq.gz'),
            data_file('hmp/palatine_tonsils/SRS014474/SRS014474.denovo_duplicates_marked.trimmed.1.fastq.gz'),
        ]
        instance.sketch_size = 100
        luigi.build([instance], local_scheduler=True)
        self.assertTrue(isfile(instance.output()['hmp_sketch'].path))
        rmtree('test_db')

    def test_build_uniref90_db(self):
        instance = Uniref90(config_filename=TEST_CONFIG)
        instance.fasta = data_file('uniref90/uniref90.sample.fasta.gz')
        luigi.build([instance], local_scheduler=True)
        self.assertTrue(isfile(instance.output()['diamond_index'].path))
        rmtree('test_db')
