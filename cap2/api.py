
import luigi

from .pipeline.databases import MODULES as DB_MODULES

from .constants import (
    STAGES,
    STAGES_GROUP,
)


def run_db_stage(config_path='', cores=1, **kwargs):
    """Run the database stage of the pipeline."""
    instances = []
    for module in DB_MODULES:
        instances.append(
            module(
                config_filename=config_path,
                cores=cores
            )
        )
    luigi.build(instances, local_scheduler=True, **kwargs)


def run_stage(samples, stage_name, config_path='', cores=1, workers=1, **kwargs):
    """Run a subpipeline on a list of samples. stage_name can be one of `qc`, `pre`, `reads`."""
    modules = STAGES[stage_name]
    group_modules = STAGES_GROUP.get(stage_name, [])
    run_modules(
        samples, modules,
        group_modules=group_modules,
        config_path=config_path,
        cores=cores,
        workers=workers,
        **kwargs
    )


def run_modules(samples, modules, group_modules=[], config_path='', cores=1, workers=1, **kwargs):
    """Run a set of modules for a list of samples."""
    instances = []
    for sample in samples:
        for module in modules:
            instance = module.from_sample(sample, config_path, cores=cores)
            instances.append(instance)

    for grp_module in group_modules:
        instances.append(grp_module.from_samples('all', samples, config_path))

    luigi.build(instances, local_scheduler=True, workers=workers, **kwargs)
