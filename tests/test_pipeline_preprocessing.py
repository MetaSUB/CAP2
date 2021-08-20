
import luigi
import os

from shutil import rmtree
from os.path import join, dirname, isfile, isdir, abspath
from unittest import TestCase, skip
import luigi

from cap2.pipeline.preprocessing.count_reads import CountRawReads
from cap2.pipeline.preprocessing.fastqc import FastQC
from cap2.pipeline.preprocessing.map_to_human import RemoveHumanReads
from cap2.pipeline.preprocessing.map_to_mouse import RemoveMouseReads
from cap2.pipeline.preprocessing.error_correct_reads import ErrorCorrectReads
from cap2.pipeline.preprocessing.remove_adapters import AdapterRemoval
from cap2.pipeline.preprocessing.fast_taxa import FastKraken2
from cap2.pipeline.preprocessing.basic_sample_stats import BasicSampleStats

RAW_READS_1 = join(dirname(__file__), 'data/zymo_pos_cntrl.r1.fq.gz')
RAW_READS_2 = join(dirname(__file__), 'data/zymo_pos_cntrl.r2.fq.gz')
TEST_CONFIG = join(dirname(__file__), 'data/test_config.yaml')

def data_file(fname):
    return join(dirname(__file__), 'data', fname)


class DummyHumanRemovalDB(luigi.ExternalTask):

    @property
    def bowtie2_index(self):
        return join(dirname(__file__), 'data/hg38/genome_sample')

    def output(self):
        return luigi.LocalTarget(join(dirname(__file__), 'data/hg38/genome_sample.1.bt2'))


class DummyMouseRemovalDB(luigi.ExternalTask):

    @property
    def bowtie2_index(self):
        return join(dirname(__file__), 'data/hg38/genome_sample')

    def output(self):
        return luigi.LocalTarget(join(dirname(__file__), 'data/hg38/genome_sample.1.bt2'))


class DummyAdapterRemovedReads(luigi.ExternalTask):

    @property
    def reads(self):
        return [RAW_READS_1, RAW_READS_2]

    def output(self):
        return {
            'adapter_removed_reads_1': luigi.LocalTarget(self.reads[0]),
            'adapter_removed_reads_2': luigi.LocalTarget(self.reads[1]),
        }


class DummyMouseRemovedReads(luigi.ExternalTask):

    @property
    def reads(self):
        return [RAW_READS_1, RAW_READS_2]

    def output(self):
        return {
            'bam': None,
            'nonmouse_reads_1': luigi.LocalTarget(self.reads[0]),
            'nonmouse_reads_2': luigi.LocalTarget(self.reads[1]),
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


class DummyKraken2DB(luigi.ExternalTask):

    @property
    def kraken2_db(self):
        return join(dirname(__file__), 'data/kraken2')

    def output(self):
        return {'kraken2_db_taxa': luigi.LocalTarget(self.kraken2_db)}


class DummyFastKraken2(luigi.ExternalTask):

    def output(self):
        return {
            'report': luigi.LocalTarget(data_file('kraken2_report.tsv')),
            'read_assignments': luigi.LocalTarget(data_file('kraken2_read_assignments.tsv')),
        }


class TestPipelinePreprocessing(TestCase):

    def tearDownClass():
        pass
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

    def test_fast_taxa(self):
        instance = FastKraken2(
            pe1=RAW_READS_1,
            pe2=RAW_READS_2,
            sample_name='test_sample',
            config_filename=TEST_CONFIG,
            cores=1
        )
        instance.db = DummyKraken2DB()
        luigi.build([instance], local_scheduler=True)
        self.assertTrue(isfile(instance.output()['read_assignments'].path))
        self.assertTrue(isfile(instance.output()['report'].path))

    def test_basic_sample_stats(self):
        instance = BasicSampleStats(
            pe1=RAW_READS_1,
            pe2=RAW_READS_2,
            sample_name='test_sample',
            config_filename=TEST_CONFIG,
            cores=1
        )
        instance.taxa = DummyFastKraken2()
        instance.READ_STATS_DROPOUT = 1 / 10
        luigi.build([instance], local_scheduler=True)
        self.assertTrue(isfile(instance.output()['report'].path))

    def test_adapter_remove_reads(self):
        instance = AdapterRemoval(
            pe1=RAW_READS_1,
            pe2=RAW_READS_2,
            sample_name='test_sample',
            config_filename=TEST_CONFIG,
            cores=1
        )
        luigi.build([instance], local_scheduler=True)
        self.assertTrue(isfile(instance.output()['adapter_removed_reads_1'].path))
        self.assertTrue(isfile(instance.output()['adapter_removed_reads_2'].path))

    def test_invoke_remove_mouse_reads(self):
        if 'CIRCLECI_TESTS' in os.environ:  # do not run this test on circleci
            return
        instance = RemoveMouseReads(
            pe1=RAW_READS_1,
            pe2=RAW_READS_2,
            sample_name='test_sample',
            config_filename=TEST_CONFIG,
            cores=1
        )
        instance.db = DummyMouseRemovalDB()
        instance.adapter_removed_reads = DummyAdapterRemovedReads()
        luigi.build([instance], local_scheduler=True)
        self.assertTrue(isfile(instance.output()['bam'].path))
        self.assertTrue(isfile(instance.output()['nonmouse_reads_1'].path))
        self.assertTrue(isfile(instance.output()['nonmouse_reads_2'].path))

    def test_invoke_remove_human_reads(self):
        if 'CIRCLECI_TESTS' in os.environ:  # do not run this test on circleci
            return
        instance = RemoveHumanReads(
            pe1=RAW_READS_1,
            pe2=RAW_READS_2,
            sample_name='test_sample',
            config_filename=TEST_CONFIG,
            cores=1
        )
        instance.db = DummyHumanRemovalDB()
        instance.mouse_removed_reads = DummyMouseRemovedReads()
        luigi.build([instance], local_scheduler=True)
        self.assertTrue(isfile(instance.output()['bam'].path))
        self.assertTrue(isfile(instance.output()['nonhuman_reads_1'].path))
        self.assertTrue(isfile(instance.output()['nonhuman_reads_2'].path))

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
        self.assertTrue(isfile(instance.output()['error_corrected_reads_1'].path))
        self.assertTrue(isfile(instance.output()['error_corrected_reads_2'].path))
