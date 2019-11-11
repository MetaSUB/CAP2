
import luigi

from shutil import rmtree
from os.path import join, dirname, isfile, isdir
from unittest import TestCase

from cap2.pipeline.short_read.krakenuniq import KrakenUniq
from cap2.pipeline.short_read.humann2 import MicaUniref90, Humann2
from cap2.pipeline.short_read.mash import Mash
from cap2.pipeline.short_read.hmp_comparison import HmpComparison
from cap2.pipeline.short_read.microbe_census import MicrobeCensus

RAW_READS_1 = join(dirname(__file__), 'data/zymo_pos_cntrl.r1.fq.gz')
RAW_READS_2 = join(dirname(__file__), 'data/zymo_pos_cntrl.r2.fq.gz')
TEST_CONFIG = join(dirname(__file__), 'data/test_config.yaml')


class DummyTaxonomicDB(luigi.ExternalTask):

    @property
    def krakenuniq_db(self):
        return join(dirname(__file__), 'data/')

    def output(self):
        return luigi.LocalTarget(join(dirname(__file__), 'data/'))


class DummyHmpDB(luigi.ExternalTask):

    @property
    def mash_sketch(self):
        return join(dirname(__file__), 'dbs/hmp_mash_sketch.msh')

    def output(self):
        return {'hmp_sketch': luigi.LocalTarget(self.mash_sketch)}


class DummyUnirefDB(luigi.ExternalTask):

    @property
    def diamond_index(self):
        return join(dirname(__file__), 'dbs/uniref90.dmnd')

    def output(self):
        return {'diamond_index': luigi.LocalTarget(self.diamond_index)}


class DummyAlignUniref90(luigi.ExternalTask):

    def output(self):
        m8 = luigi.LocalTarget(
            join(self.out_dir, f'{self.sample_name}.uniref90.m8.gz')
        )
        return {
            'm8': m8,
        }


class DummyMash(luigi.ExternalTask):

    def output(self):
        return {
            '10M_mash_sketch': luigi.LocalTarget(
                join(dirname(__file__), f'data/zymo_pos_cntrl.mash.sketch.msh')
            ),
        }


class DummyCleanReads(luigi.ExternalTask):

    @property
    def reads(self):
        return [RAW_READS_1, RAW_READS_2]

    def output(self):
        return {
            'clean_reads': [luigi.LocalTarget(el) for el in self.reads],
        }


class TestShortRead(TestCase):

    def test_invoke_krakenuniq(self):
        instance = KrakenUniq(
            pe1=RAW_READS_1,
            pe2=RAW_READS_2,
            sample_name='test_sample',
            config_filename=TEST_CONFIG
        )
        instance.db = DummyTaxonomicDB()
        luigi.build([instance], local_scheduler=True)
        # self.assertTrue(isfile(instance.output()['report'].path))
        # self.assertTrue(isfile(instance.output()['read_assignments'].path))

    def test_invoke_align_uniref90(self):
        instance = MicaUniref90(
            pe1=RAW_READS_1,
            pe2=RAW_READS_2,
            sample_name='test_sample',
            config_filename=TEST_CONFIG
        )
        instance.db = DummyUnirefDB()
        instance.reads = DummyCleanReads()
        luigi.build([instance], local_scheduler=True)
        self.assertTrue(isfile(instance.output()['m8'].path))

    def test_invoke_hmp_comparison(self):
        instance = HmpComparison(
            pe1=RAW_READS_1,
            pe2=RAW_READS_2,
            sample_name='test_sample',
            config_filename=TEST_CONFIG
        )
        instance.db = DummyHmpDB()
        instance.mash = DummyMash()
        luigi.build([instance], local_scheduler=True)
        self.assertTrue(isfile(instance.output()['hmp_dists'].path))

    def test_invoke_humann2(self):
        instance = Humann2(
            pe1=RAW_READS_1,
            pe2=RAW_READS_2,
            sample_name='test_sample',
            config_filename=TEST_CONFIG
        )
        instance.alignment = DummyAlignUniref90()
        luigi.build([instance], local_scheduler=True)
        # self.assertTrue(isfile(instance.output()['genes'].path))
        # self.assertTrue(isfile(instance.output()['path_abunds'].path))
        # self.assertTrue(isfile(instance.output()['path_covs'].path))

    def test_invoke_mash(self):
        instance = Mash(
            pe1=RAW_READS_1,
            pe2=RAW_READS_2,
            sample_name='test_sample',
            config_filename=TEST_CONFIG
        )
        instance.reads = DummyCleanReads()
        luigi.build([instance], local_scheduler=True)
        self.assertTrue(isfile(instance.output()['10M_mash_sketch'].path))

    def test_invoke_microbe_census(self):
        instance = MicrobeCensus(
            pe1=RAW_READS_1,
            pe2=RAW_READS_2,
            sample_name='test_sample',
            config_filename=TEST_CONFIG
        )
        luigi.build([instance], local_scheduler=True)
        # self.assertTrue(isfile(instance.output()['report'].path))
