
import luigi

from shutil import rmtree
from os.path import join, dirname, isfile, isdir
from unittest import TestCase, skip

from cap2.pipeline.utils.cap_task import CapTask, class_or_instancemethod

RAW_READS_1 = join(dirname(__file__), 'data/zymo_pos_cntrl.r1.fq.gz')
RAW_READS_2 = join(dirname(__file__), 'data/zymo_pos_cntrl.r2.fq.gz')
TEST_CONFIG = join(dirname(__file__), 'data/test_config.yaml')


class FlagTask(CapTask):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @classmethod
    def _module_name(cls):
        return 'test_flag'

    @property
    def flag_filepath(self):
        return self.output()['flag'].path

    def output(self):
        return {
            'flag': self.get_target('flag', 'flag'),
        }

    def _run(self):
        open(self.flag_filepath, 'w').close()

    @classmethod
    def dependencies(cls):
        return []


class FlagTaskVersionA(FlagTask):
    MODULE_VERSION = 'A'


class FlagTaskVersionB(FlagTask):
    MODULE_VERSION = 'B'


class TaskThatReliesOnFlagTask(CapTask):
    MODULE_VERSION = 'v0.1.0'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.flag = FlagTaskVersionA.from_cap_task(self)

    @classmethod
    def _module_name(cls):
        return 'test_task_that_relies_on_flag'

    def requires(self):
        return self.flag

    @classmethod
    def dependencies(cls):
        return [FlagTaskVersionA]

    @property
    def flag_filepath(self):
        return self.output()['flag'].path

    def output(self):
        return {
            'flag': self.get_target('flag', 'flag'),
        }

    def _run(self):
        open(self.flag_filepath, 'w').close()


class TestVersion(TestCase):

    def tearDownClass():
        pass
        rmtree('test_out')

    def test_create_flag_a(self):
        flag_a = FlagTaskVersionA(
            pe1=RAW_READS_1,
            pe2=RAW_READS_2,
            sample_name='test_sample',
            config_filename=TEST_CONFIG,
            check_versions=False,
        )
        self.assertFalse(flag_a.check_versions)
        self.assertEqual(flag_a.version(), 'A')

    def test_invoke_kraken2(self):
        flag_b = FlagTaskVersionB(
            pe1=RAW_READS_1,
            pe2=RAW_READS_2,
            sample_name='test_sample',
            config_filename=TEST_CONFIG
        )
        luigi.build([flag_b], local_scheduler=True)
        self.assertTrue(isfile(flag_b.flag_filepath))
        instance = TaskThatReliesOnFlagTask(
            pe1=RAW_READS_1,
            pe2=RAW_READS_2,
            sample_name='test_sample',
            config_filename=TEST_CONFIG
        )
        # By default relies on A but config is set to allow B
        self.assertIn('B', instance.config.allowed_versions(flag_b))
        self.assertEqual(instance.flag.version(), 'B')
        luigi.build([instance], local_scheduler=True)

        # check that FlagA is unaffected (no side effects)
        self.assertEqual(FlagTaskVersionA.version(), 'A')
        flag_a = FlagTaskVersionA(
            pe1=RAW_READS_1,
            pe2=RAW_READS_2,
            sample_name='different_sample',  # NB if sample is the same luigi will just use the old task
            config_filename=TEST_CONFIG,
            check_versions=False,
        )
        self.assertFalse(flag_a.check_versions)
        self.assertIsNone(flag_a.version_override)
        self.assertEqual(flag_a.version(), 'A')
        self.assertFalse(isfile(flag_a.flag_filepath))



