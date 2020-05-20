
import luigi

from .pipeline.databases import MODULES as DB_MODULES
from .pipeline.preprocessing import MODULES as PRE_MODULES
from .pipeline.preprocessing import QC_MODULES
from .pipeline.short_read import MODULES as SHORT_READ_MODULES


def run_db_stage(config_path='', **kwargs):
    instances = []
    for module in DB_MODULES:
        instances.append(
            module(
                config_filename=config_path,
                cores=kwargs.pop('cores', 1)
            )
        )
    luigi.build(instances, local_scheduler=True, **kwargs)


def run_qc_stage(samples, config_path='', **kwargs):
    run_modules(samples, QC_MODULES, config_path=config_path, **kwargs)


def run_preprocessing_stage(samples, config_path='', **kwargs):
    run_modules(samples, PRE_MODULES, config_path=config_path, **kwargs)


def run_short_read_stage(samples, config_path='', **kwargs):
    run_modules(samples, SHORT_READ_MODULES, config_path=config_path, **kwargs)


def run_modules(samples, modules, config_path='', **kwargs):
    instances = []
    for sample in samples:
        for module in modules:
            instance = module.from_sample(sample, config_path)
            instances.append(instance)
    luigi.build(instances, local_scheduler=True, **kwargs)
