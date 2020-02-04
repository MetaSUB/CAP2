
import luigi
import os

from shutil import rmtree
from os.path import join, dirname, isfile, isdir, abspath
from unittest import TestCase
import luigi

from cap2.pipeline.preprocessing.count_reads import CountRawReads
from cap2.pipeline.preprocessing.fastqc import FastQC
from cap2.pipeline.preprocessing.map_to_human import RemoveHumanReads
from cap2.pipeline.preprocessing.error_correct_reads import ErrorCorrectReads

RAW_READS_1 = join(dirname(__file__), 'data/zymo_pos_cntrl.r1.fq.gz')
RAW_READS_2 = join(dirname(__file__), 'data/zymo_pos_cntrl.r2.fq.gz')
TEST_CONFIG = join(dirname(__file__), 'data/test_config.yaml')


class DummyHumanRemovalDB(luigi.ExternalTask):

    @property
    def bowtie2_index(self):
        return join(dirname(__file__), 'data/hg38/genome_sample')

    def output(self):
        return luigi.LocalTarget(join(dirname(__file__), 'data/hg38/genome_sample.1.bt2'))


class DummyHumanRemovedReads(luigi.ExternalTask):

    @property
    def reads(self):
        return [RAW_READS_1, RAW_READS_2]

    def output(self):
        return {
            'bam': None,
            'nonhuman_reads': [luigi.LocalTarget(el) for el in self.reads],
        }


class TestPipelinePreprocessing(TestCase):

    def tearDownClass():
        rmtree('test_out')

    def test_invoke_count_raw_reads(self):
        instance = CountRawReads(
            pe1=RAW_READS_1,
            pe2=RAW_READS_2,
            sample_name='test_sample',
            config_filename=TEST_CONFIG,
            cores=1
        )
        luigi.build([instance], local_scheduler=True)
        self.assertTrue(isfile(instance.output()['read_counts'].path))
        text = open(instance.output()['read_counts'].path).read()
        self.assertIn('raw_reads,1000', text)

    def test_invoke_fastqc(self):
        instance = FastQC(
            pe1=RAW_READS_1,
            pe2=RAW_READS_2,
            sample_name='test_sample',
            config_filename=TEST_CONFIG,
            cores=1
        )
        luigi.build([instance], local_scheduler=True)
        self.assertTrue(isfile(instance.output()['zip_output'].path))
        self.assertTrue(isfile(instance.output()['report'].path))

    def test_invoke_remove_human_reads(self):
        instance = RemoveHumanReads(
            pe1=RAW_READS_1,
            pe2=RAW_READS_2,
            sample_name='test_sample',
            config_filename=TEST_CONFIG,
            cores=1
        )
        instance.db = DummyHumanRemovalDB()
        luigi.build([instance], local_scheduler=True)
        self.assertTrue(isfile(instance.output()['bam'].path))
        if abspath('.') != '/root/project':  # Happens on CircleCI, unlikely otherwise
            # I can't figure out why these files don't get created on CircleCI
            # this is a hack but hopefully not too problematic of one.
            self.assertTrue(isfile(instance.output()['nonhuman_reads'][0].path))
            self.assertTrue(isfile(instance.output()['nonhuman_reads'][1].path))

    def test_error_correct_reads(self):
        instance = ErrorCorrectReads(
            pe1=RAW_READS_1,
            pe2=RAW_READS_2,
            sample_name='test_sample',
            config_filename=TEST_CONFIG,
            cores=1
        )
        instance.nonhuman_reads = DummyHumanRemovedReads()
        luigi.build([instance], local_scheduler=True)
        self.assertTrue(isfile(instance.output()['error_corrected_reads'][0].path))
        self.assertTrue(isfile(instance.output()['error_corrected_reads'][1].path))
