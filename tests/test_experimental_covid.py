import luigi
import logging
import os

from shutil import rmtree
from os.path import join, dirname, isfile, isdir, abspath
from unittest import TestCase

from cap2.extensions.experimental.covid import (
    CovidGenomeDb,
    AlignReadsToCovidGenome,
    MakeCovidPileup,
    CovidGenomeCoverage,
    CallCovidVariants,
    MakeCovidConsensusSeq,
)

logging.basicConfig(level=logging.INFO)

BAM_FILEPATH = join(dirname(__file__), 'data/covid/covid_alignment_test_bam.bam')
BAM_INDEX_FILEPATH = join(dirname(__file__), 'data/covid/covid_alignment_test_bam.bai')
COVID_FASTA_FILEPATH = join(dirname(__file__), 'data/covid/GCF_009858895.2_ASM985889v3_genomic_noPolyAtail.fna')

PILEUP_FILEPATH = join(dirname(__file__), 'data/covid/covid_test_pileup.pileup.gz')
GENOMECOV_FILEPATH = join(dirname(__file__), 'data/covid/covid_test_genome_coverage.genomecov')


RAW_READS_1 = join(dirname(__file__), 'data/zymo_pos_cntrl.r1.fq.gz')
RAW_READS_2 = join(dirname(__file__), 'data/zymo_pos_cntrl.r2.fq.gz')
TEST_CONFIG = join(dirname(__file__), 'data/test_config.yaml')


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


class DummyCovidGenomeDb(luigi.ExternalTask):

    @property
    def fastas(self):
        return [COVID_FASTA_FILEPATH]

    @property
    def bowtie2_index(self):
        return None

    def output(self):
        return {}


class DummyAlignReadsToCovidGenome(luigi.ExternalTask):

    @property
    def db(self):
        return DummyCovidGenomeDb()

    @property
    def bam_path(self):
        return BAM_FILEPATH

    @property
    def bam_index_path(self):
        return BAM_INDEX_FILEPATH

    def output(self):
        return {
            'bam': luigi.LocalTarget(BAM_FILEPATH),
            'bam_index': luigi.LocalTarget(BAM_INDEX_FILEPATH),
        }


class DummyMakeCovidPileup(luigi.ExternalTask):

    @property
    def bam(self):
        return DummyAlignReadsToCovidGenome()

    @property
    def pileup_path(self):
        return PILEUP_FILEPATH

    def output(self):
        return {
            'pileup': luigi.LocalTarget(PILEUP_FILEPATH),
        }


class DummyCovidGenomeCoverage(luigi.ExternalTask):

    @property
    def bam(self):
        return DummyAlignReadsToCovidGenome()

    @property
    def genomecov_path(self):
        return GENOMECOV_FILEPATH

    def output(self):
        return {
            'genomecov': luigi.LocalTarget(GENOMECOV_FILEPATH),
        }


class TestCovidPipeline(TestCase):

    def tearDownClass():
        pass
        rmtree('test_out')

    def test_covid_genome_db(self):
        instance = CovidGenomeDb(
            config_filename=TEST_CONFIG,
            cores=1
        )
        luigi.build([instance], local_scheduler=True)
        self.assertTrue(isfile(instance.output()['bt2_index_1'].path))

    def test_align_to_covid_genome(self):
        instance = AlignReadsToCovidGenome(
            pe1=RAW_READS_1,
            pe2=RAW_READS_2,
            sample_name='test_sample',
            config_filename=TEST_CONFIG,
            cores=1
        )
        instance.reads = DummyHumanRemovedReads()
        luigi.build([instance], local_scheduler=True)
        self.assertFalse(isfile(instance.temp_bam_path))
        self.assertTrue(isfile(instance.bam_path))

    def test_make_pileup(self):
        instance = MakeCovidPileup(
            pe1=RAW_READS_1,
            pe2=RAW_READS_2,
            sample_name='test_sample',
            config_filename=TEST_CONFIG,
            cores=1
        )
        instance.bam = DummyAlignReadsToCovidGenome()
        luigi.build([instance], local_scheduler=True)
        self.assertTrue(isfile(instance.pileup_path))

    def test_covid_consensus_seq(self):
        instance = MakeCovidConsensusSeq(
            pe1=RAW_READS_1,
            pe2=RAW_READS_2,
            sample_name='test_sample',
            config_filename=TEST_CONFIG,
            cores=1
        )
        instance.pileup = DummyMakeCovidPileup()
        luigi.build([instance], local_scheduler=True)
        self.assertTrue(isfile(instance.fasta_path))

    def test_covid_variants(self):
        instance = CallCovidVariants(
            pe1=RAW_READS_1,
            pe2=RAW_READS_2,
            sample_name='test_sample',
            config_filename=TEST_CONFIG,
            cores=1
        )
        instance.pileup = DummyMakeCovidPileup()
        luigi.build([instance], local_scheduler=True)
        self.assertTrue(isfile(instance.variants_path))

    def test_genome_coverage(self):
        instance = CovidGenomeCoverage(
            pe1=RAW_READS_1,
            pe2=RAW_READS_2,
            sample_name='test_sample',
            config_filename=TEST_CONFIG,
            cores=1
        )
        instance.bam = DummyAlignReadsToCovidGenome()
        luigi.build([instance], local_scheduler=True)
        self.assertTrue(isfile(instance.genomecov_path))
