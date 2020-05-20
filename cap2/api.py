
import luigi

from .pipeline.databases import MODULES as DB_MODULES


def run_db_stage(config_path='', cores=1, **kwargs):
    instances = []
    for module in DB_MODULES:
        instances.append(
            module(
                config_filename=config_path,
                cores=cores
            )
        )
    luigi.build(instances, local_scheduler=True, **kwargs)


def run_modules(samples, modules, config_path='', cores=1, **kwargs):
    instances = []
    for sample in samples:
        for module in modules:
            instance = module.from_sample(sample, config_path, cores=cores)
            instances.append(instance)
    luigi.build(instances, local_scheduler=True, **kwargs)
