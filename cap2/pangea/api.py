
from .load_task import PangeaLoadTask
from ..pipeline.preprocessing import FastQC
from ..pipeline.preprocessing import CleanReads
from ..pipeline.preprocessing import AdapterRemoval
from ..pipeline.short_read import (
    MicaUniref90,
    Mash,
    MicrobeCensus,
    ReadStats,
    Kraken2,

    Humann2,
    HmpComparison,
)
from ..pipeline.preprocessing import BaseReads
from ..pipeline.assembly.metaspades import MetaspadesAssembly 

STAGES = {
    'qc': [FastQC],
    'pre': [CleanReads],
    'reads': SHORT_READ_MODULES,
    'assembly': [MetaspadesAssembly],
}


def wrap_task(sample, module, requires_reads=True, upload=True, config_path=''):
    task = PangeaLoadTask(
        pe1=sample.r1,
        pe2=sample.r2,
        sample_name=sample.name,
        wraps=module.module_name(),
    )
    task.upload_allowed = upload
    task.wrapped_module = module
    task.requires_reads = requires_reads
    return task


def get_task_list_for_sample(sample, stage, upload=True, config_path=''):
    reads = wrap_task(sample, BaseReads, upload=False, config_path=config_path)
    clean_reads = wrap_task(sample, CleanReads, requires_reads=False, config_path=config_path)
    clean_reads.wrapped.ec_reads.nonhuman_reads.adapter_removed_reads.reads = reads
    dmnd_uniref90 = wrap_task(sample, MicaUniref90, config_path=config_path)
    dmnd_uniref90.reads = clean_reads
    humann2 = wrap_task(sample, Humann2, config_path=config_path)
    humann2.alignment = dmnd_uniref90
    mash = wrap_task(sample, Mash, config_path=config_path)
    mash.reads = clean_reads
    hmp = wrap_task(sample, HmpComparison, config_path=config_path)
    hmp.mash = mash
    microbe_census = wrap_task(sample, MicrobeCensus, config_path=config_path)
    microbe_census.reads = clean_reads
    read_stats = wrap_task(sample, ReadStats, config_path=config_path)
    read_stats.reads = clean_reads
    kraken2 = wrap_task(sample, Kraken2, config_path=config_path)
    kraken2.reads = clean_reads
    tasks = [
        clean_reads,
        dmnd_uniref90,
        humann2,
        mash,
        hmp,
        microbe_census,
        read_stats,
        kraken2,
    ]
    return tasks
