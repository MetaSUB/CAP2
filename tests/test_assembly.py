
import luigi
import os

from shutil import rmtree
from os.path import join, dirname, isfile, isdir, abspath
from unittest import TestCase

from cap2.pipeline.assembly.metabat2 import MetaBat2Binning
from cap2.pipeline.contigs.prodigal import Prodigal


TEST_CONTIGS = join(dirname(__file__), 'data/assembly/test_contigs.fa')
RAW_READS_1 = join(dirname(__file__), 'data/zymo_pos_cntrl.r1.fq.gz')
RAW_READS_2 = join(dirname(__file__), 'data/zymo_pos_cntrl.r2.fq.gz')
TEST_CONFIG = join(dirname(__file__), 'data/test_config.yaml')


class DummyMetaspadesAssembly(luigi.ExternalTask):

    def output(self):
        return {'scaffolds_fasta': luigi.LocalTarget(TEST_CONTIGS)}


class DummyMetaBat2Binning(luigi.ExternalTask):

    def untar_then_list_bin_files(self):
        return [join(dirname(__file__), 'data/assembly/test_sample_metabat2.1.fa')]

    def clean_up_bin_files(self):
        pass

    def output(self):
        return {
            'scaffold_bins': luigi.LocalTarget(join(dirname(__file__), 'data/assembly/test_sample.scaffold_bins.tar.gz')),
            'bin_assignments': luigi.LocalTarget(join(dirname(__file__), 'data/assembly/test_sample.bin_assignments.tsv')),
        }


class TestPipelineAssembly(TestCase):

    def tearDownClass():
        pass
        rmtree('test_out')

    def test_invoke_metabat2(self):
        instance = MetaBat2Binning(
            pe1=RAW_READS_1,
            pe2=RAW_READS_2,
            sample_name='test_sample',
            config_filename=TEST_CONFIG,
            cores=1
        )
        instance.contigs = DummyMetaspadesAssembly()
        luigi.build([instance], local_scheduler=True)
        self.assertTrue(isfile(instance.output()['scaffold_bins'].path))
        self.assertTrue(isfile(instance.output()['bin_assignments'].path))

    def test_invoke_prodigal(self):
        instance = Prodigal(
            pe1=RAW_READS_1,
            pe2=RAW_READS_2,
            sample_name='test_sample',
            config_filename=TEST_CONFIG,
            cores=1
        )
        instance.contigs = DummyMetaBat2Binning()
        luigi.build([instance], local_scheduler=True)
        self.assertTrue(isfile(instance.output()['gene_table'].path))
        self.assertTrue(isfile(instance.output()['protein_fasta'].path))

