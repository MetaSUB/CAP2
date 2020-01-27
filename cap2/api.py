
import luigi

from .pipeline.short_read import (
    KrakenUniq,
    MicaUniref90,
    Humann2,
    Mash,
    HmpComparison,
    MicrobeCensus,
    ReadStats,
    GrootAMR,
)


def run_short_read_stage(samples, config_path, **kwargs):
    modules = [
        KrakenUniq,
        MicaUniref90,
        Humann2,
        Mash,
        HmpComparison,
        MicrobeCensus,
        ReadStats,
        GrootAMR,
    ]
    run_modules(samples, modules, config_path, **kwargs)


def run_modules(samples, modules, config_path, **kwargs):
    instances = []
    for sample in samples:
        for module in modules:
            instance = module.from_sample(sample, config_path)
            instances.append(instance)
    luigi.build(instances, local_scheduler=True, **kwargs)
