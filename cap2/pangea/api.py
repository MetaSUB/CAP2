
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
    ProcessedReads,
)
from ..pipeline.preprocessing import BaseReads
from ..pipeline.assembly.metaspades import MetaspadesAssembly 

STAGES = {
    'qc': [FastQC],
    'pre': [CleanReads],
    'assembly': [MetaspadesAssembly],
    'reads': [],
}


def wrap_task(sample, module, requires_reads=True, upload=True, config_path='', cores=1):
    task = PangeaLoadTask(
        pe1=sample.r1,
        pe2=sample.r2,
        sample_name=sample.name,
        wraps=module.module_name(),
        config_filename=config_path,
        cores=cores,
    )
    task.upload_allowed = upload
    task.wrapped_module = module
    task.requires_reads = requires_reads
    return task


def get_task_list_for_sample(sample, stage, upload=True, config_path='', cores=1):
    reads = wrap_task(sample, BaseReads, upload=False, config_path=config_path, cores=cores)
    clean_reads = wrap_task(sample, CleanReads, config_path=config_path, cores=cores)
    clean_reads.wrapped.ec_reads.nonhuman_reads.adapter_removed_reads.reads = reads
    dmnd_uniref90 = wrap_task(sample, MicaUniref90, config_path=config_path, cores=cores)
    dmnd_uniref90.wrapped.reads = clean_reads
    humann2 = wrap_task(sample, Humann2, config_path=config_path, cores=cores)
    humann2.wrapped.alignment = dmnd_uniref90
    mash = wrap_task(sample, Mash, config_path=config_path, cores=cores)
    mash.wrapped.reads = clean_reads
    hmp = wrap_task(sample, HmpComparison, config_path=config_path, cores=cores)
    hmp.wrapped.mash = mash
    read_stats = wrap_task(sample, ReadStats, config_path=config_path, cores=cores)
    read_stats.wrapped.reads = clean_reads
    kraken2 = wrap_task(sample, Kraken2, config_path=config_path, cores=cores)
    kraken2.wrapped.reads = clean_reads

    processed = ProcessedReads.from_sample(sample, config_path, cores=cores)
    processed.hmp = hmp
    processed.humann2 = humann2
    processed.kraken2 = kraken2
    processed.mash = mash
    processed.read_stats = read_stats

    if stage == 'qc':
        tasks = [
            clean_reads,
        ]
    if stage == 'reads':
        tasks = [
            clean_reads,
            processed,
        ]
    return tasks
