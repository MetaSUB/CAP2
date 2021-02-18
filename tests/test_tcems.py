import luigi

from shutil import rmtree
from os.path import join, dirname, isfile, isdir
from unittest import TestCase, skip
from cap2.pipeline.config import PipelineConfig

from cap2.extensions.experimental.tcems.tcem_aa_db import TcemNrAaDb
from cap2.extensions.experimental.tcems.tcem_repertoire import AnnotatedTcemRepertoire
import logging

logger = logging.getLogger('tcems')

logger.addHandler(logging.StreamHandler())


def data_file(fname):
    return join(dirname(__file__), 'data', fname)


TEST_CONFIG = data_file('test_config.yaml')
RAW_READS_1 = data_file('zymo_pos_cntrl.r1.fq.gz')
RAW_READS_2 = data_file('zymo_pos_cntrl.r2.fq.gz')

class DummyTcemNrAaDb(luigi.ExternalTask):

    @property
    def tcem_index(self):
        return data_file('tcems/sample_tcem_db.sqlite')

    def output(self):
        return {'tcem_index': luigi.LocalTarget(self.tcem_index)}

class DummyTcemRepetoire(luigi.ExternalTask):

    @property
    def tcem_counts_path(self):
        return data_file('tcems/sample_tcem_rep.csv')

    def output(self):
        return {'tcem_counts': luigi.LocalTarget(self.tcem_counts_path)}


class TestTcems(TestCase):

    def test_build_tcem_db(self):
        instance = TcemNrAaDb(cores=2, config_filename=TEST_CONFIG)
        self.assertEqual(instance.config.db_mode, PipelineConfig.DB_MODE_BUILD)
        instance.fasta = data_file('nr/swissprot_sample.fasta.gz')
        instance.flush_count = 10
        luigi.build([instance], local_scheduler=True)
        self.assertTrue(isfile(instance.output()['tcem_index'].path))
        rmtree('test_db')

    def test_annotate_tcem_rep(self):
        instance = AnnotatedTcemRepertoire(
            pe1=RAW_READS_1,
            pe2=RAW_READS_2,
            sample_name='test_sample',
            config_filename=TEST_CONFIG
        )
        instance.db = DummyTcemNrAaDb()
        instance.repetoire = DummyTcemRepetoire()
        luigi.build([instance], local_scheduler=True)
        self.assertTrue(isfile(instance.tcem_annotation_path))
