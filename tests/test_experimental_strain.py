import luigi
import logging
import os

from shutil import rmtree
from os.path import join, dirname, isfile, isdir, abspath
from unittest import TestCase, skip

from cap2.extensions.experimental.strains.strainotyping import graph_from_bam_filepath
from cap2.extensions.experimental.strains import (
    AlignReadsToGenome,
    AlignReadsToGenomeDb,
    MakePileup,
    MakeSNPGraph,
)
from cap2.extensions.experimental.strains.get_microbial_genome import get_microbial_genome

logging.basicConfig(level=logging.INFO)

BAM_FILEPATH = join(dirname(__file__), 'data/covid/covid_alignment_test_bam.bam')
RAW_READS_1 = join(dirname(__file__), 'data/zymo_pos_cntrl.r1.fq.gz')
RAW_READS_2 = join(dirname(__file__), 'data/zymo_pos_cntrl.r2.fq.gz')
TEST_CONFIG = join(dirname(__file__), 'data/test_config.yaml')


class DummyAlignReadsToGenome(luigi.ExternalTask):

    @property
    def bam_path(self):
        return BAM_FILEPATH

    def output(self):
        return {
            'bam__Genometest_fakegenome': luigi.LocalTarget(join(
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


class TestStrainPipeline(TestCase):

    def tearDownClass():
        pass
        rmtree('test_out')

    def test_align_to_genome_db(self):
        instance = AlignReadsToGenomeDb(
            genome_name='Genometest_fakegenome',
            genome_path=join(dirname(__file__), 'data/krakenuniq'),
            config_filename=TEST_CONFIG,
            cores=1
        )
        luigi.build([instance], local_scheduler=True)
        self.assertTrue(isfile(instance.output()['bt2_index_1'].path))

    @skip(reason="slow")
    def test_align_to_genome_db_with_fetch(self):
        instance = AlignReadsToGenomeDb(
            genome_name='Serratia_proteamaculans',
            config_filename=TEST_CONFIG,
            cores=1
        )
        luigi.build([instance], local_scheduler=True)
        self.assertTrue(isfile(instance.output()['bt2_index_1'].path))

    def test_align_to_genome(self):
        instance = AlignReadsToGenome(
            genome_name='Genometest_fakegenome',
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
            genome_name='Genometest_fakegenome',
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

    def test_make_snp_graph(self):
        instance = MakeSNPGraph(
            genome_name='Genometest_fakegenome',
            genome_path='/this/is/not/a/real/path',
            pe1=RAW_READS_1,
            pe2=RAW_READS_2,
            sample_name='test_sample',
            config_filename=TEST_CONFIG,
            cores=1
        )
        instance.bam = DummyAlignReadsToGenome()
        luigi.build([instance], local_scheduler=True)
        self.assertTrue(isfile(instance.graph_path))

    def test_make_snp_graph_inner(self):
        graph_from_bam_filepath(BAM_FILEPATH)

    @skip(reason="slow, no rsync on circleci")
    def test_bacterial_genome_getter(self):
        genomes = get_microbial_genome('serratia_proteamaculans', outdir='test_out/serratia_proteamaculans')
        for filepath in genomes:
            self.assertTrue(isfile(filepath))
