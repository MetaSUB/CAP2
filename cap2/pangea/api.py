
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
from ..pipeline.full_pipeline import FullPipeline

STAGES = [
    'qc',
    'pre',
    'reads',
    'assembly',
    'all',
]


def wrap_task(sample, module, requires_reads=False, upload=True, config_path='', cores=1):
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


def get_task_list_for_read_stage(sample, clean_reads, upload=True, config_path='', cores=1):
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
    return processed


def get_task_list_for_sample(sample, stage, upload=True, config_path='', cores=1):
    reads = wrap_task(
        sample, BaseReads,
        upload=False, config_path=config_path, cores=cores, requires_reads=True
    )
    # qc stage
    fastqc = wrap_task(
        sample, FastQC, upload=upload, config_path=config_path, cores=cores
    )
    fastqc.wrapped.reads = reads
    # pre stage
    clean_reads = wrap_task(
        sample, CleanReads, upload=upload, config_path=config_path, cores=cores
    )
    clean_reads.wrapped.ec_reads.nonhuman_reads.adapter_removed_reads.reads = reads
    # reads stage
    processed = get_task_list_for_read_stage(
        sample, clean_reads, upload=upload, config_path=config_path, cores=cores
    )
    # assembly stage
    assembly = wrap_task(
        sample, MetaspadesAssembly, upload=upload, config_path=config_path, cores=cores
    )
    assembly.wrapped.reads = clean_reads
    # all stage
    full = FullPipeline.from_sample(sample, config_path, cores=cores)
    full.qc = fastqc
    full.short_reads = processed
    full.assembly = assembly

    if stage == 'qc':
        tasks = [fastqc]
    if stage == 'pre':
        tasks = [clean_reads]
    if stage == 'reads':
        tasks = [clean_reads, processed]
    if stage == 'assembly':
        tasks = [clean_reads, assembly]
    if stage == 'all':
        tasks = [full]
    return tasks
