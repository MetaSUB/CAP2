import luigi
import logging
import os

from shutil import rmtree
from os.path import join, dirname, isfile, isdir, abspath
from unittest import TestCase

from cap2.extensions.experimental.strains import (
    AlignReadsToGenome,
    AlignReadsToGenomeDb,
    MakePileup,
)

logging.basicConfig(level=logging.INFO)

BAM_FILEPATH = join(dirname(__file__), 'data/a_sorted_bam_file.bam')
RAW_READS_1 = join(dirname(__file__), 'data/zymo_pos_cntrl.r1.fq.gz')
RAW_READS_2 = join(dirname(__file__), 'data/zymo_pos_cntrl.r2.fq.gz')
TEST_CONFIG = join(dirname(__file__), 'data/test_config.yaml')


class DummyAlignReadsToGenome(luigi.ExternalTask):

    @property
    def bam_path(self):
        return join(dirname(__file__), 'data/hg38/genome_sample')

    def output(self):
        return {
            'bam__my_test_genome_name': luigi.LocalTarget(join(
                dirname(__file__),
                'data/a_sorted_bam_file.bam'
            )),
        }


class DummyAlignReadsToGenomeDb(luigi.ExternalTask):

    @property
    def bowtie2_index(self):
        return join(dirname(__file__), 'data/hg38/genome_sample')

    def output(self):
        return {
            'bt2_index_1': luigi.LocalTarget(join(
                dirname(__file__),
                'data/hg38/genome_sample.1.bt2'
            )),
        }


class DummyHumanRemovedReads(luigi.ExternalTask):

    @property
    def reads(self):
        return [RAW_READS_1, RAW_READS_2]

    def output(self):
        return {
            'bam': None,
            'nonhuman_reads_1': luigi.LocalTarget(self.reads[0]),
            'nonhuman_reads_2': luigi.LocalTarget(self.reads[1]),
        }


class TestPipelinePreprocessing(TestCase):

    def tearDownClass():
        pass
        rmtree('test_out')

    def test_align_to_genome_db(self):
        instance = AlignReadsToGenomeDb(
            genome_name='my_test_genome_name',
            genome_path=join(dirname(__file__), 'data/krakenuniq'),
            config_filename=TEST_CONFIG,
            cores=1
        )
        luigi.build([instance], local_scheduler=True)
        self.assertTrue(isfile(instance.output()['bt2_index_1'].path))

    def test_align_to_genome(self):
        instance = AlignReadsToGenome(
            genome_name='my_test_genome_name',
            genome_path='/this/is/not/a/real/path',
            pe1=RAW_READS_1,
            pe2=RAW_READS_2,
            sample_name='test_sample',
            config_filename=TEST_CONFIG,
            cores=1
        )
        instance.nonhuman_reads = DummyHumanRemovedReads()
        instance.db = DummyAlignReadsToGenomeDb()
        luigi.build([instance], local_scheduler=True)
        self.assertFalse(isfile(instance.temp_bam_path))
        self.assertTrue(isfile(instance.bam_path))

    def test_make_pileup(self):
        instance = MakePileup(
            genome_name='my_test_genome_name',
            genome_path='/this/is/not/a/real/path',
            pe1=RAW_READS_1,
            pe2=RAW_READS_2,
            sample_name='test_sample',
            config_filename=TEST_CONFIG,
            cores=1
        )
        instance.bam = DummyAlignReadsToGenome()
        luigi.build([instance], local_scheduler=True)
        self.assertTrue(isfile(instance.pileup_path))
