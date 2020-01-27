
from shutil import rmtree
from os.path import join, dirname, isfile, isdir
from unittest import TestCase, skip

from cap2.api import (
    run_short_read_stage,
    run_preprocessing_stage,
)
from cap2.sample import Sample

RAW_READS_1 = join(dirname(__file__), 'data/zymo_pos_cntrl.r1.fq.gz')
RAW_READS_2 = join(dirname(__file__), 'data/zymo_pos_cntrl.r2.fq.gz')
TEST_CONFIG = join(dirname(__file__), 'data/test_config.yaml')
SAMPLE = Sample('test_sample', RAW_READS_1, RAW_READS_2)


class TestApi(TestCase):
    """Test the CAP2 API, essentially integration tests."""

    def test_short_read_stage(self):
        run_short_read_stage([SAMPLE], TEST_CONFIG)

    def test_preprocessing_stage(self):
        run_preprocessing_stage([SAMPLE], TEST_CONFIG)
