
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
    BrakenKraken2,
    Humann2,
    HmpComparison,
    ProcessedReads,
)
from ..pipeline.preprocessing import BaseReads
from ..pipeline.assembly.metaspades import MetaspadesAssembly
from ..pipeline.full_pipeline import FullPipeline

STAGES = [
    'data',
    'qc',
    'pre',
    'reads',
    'assembly',
    'all',
]


def wrap_task(sample, module,
              requires_reads=False, upload=True, download_only=False,
              config_path='', cores=1, **kwargs):
    task = PangeaLoadTask(
        pe1=sample.r1,
        pe2=sample.r2,
        sample_name=sample.name,
        wraps=module.module_name(),
        config_filename=config_path,
        cores=cores,
        **kwargs,
    )
    task.upload_allowed = upload
    task.wrapped_module = module
    task.requires_reads = requires_reads
    task.download_only = download_only
    return task


def get_task_list_for_read_stage(sample, clean_reads, upload=True, download_only=False, config_path='', cores=1):
    wrapit = lambda module: wrap_task(sample, module, config_path=config_path, cores=cores, upload=upload, download_only=download_only)
    dmnd_uniref90 = wrapit(MicaUniref90)
    dmnd_uniref90.wrapped.reads = clean_reads
    humann2 = wrapit(Humann2)
    humann2.wrapped.alignment = dmnd_uniref90
    mash = wrapit(Mash)
    mash.wrapped.reads = clean_reads
    hmp = wrapit(HmpComparison)
    hmp.wrapped.mash = mash
    read_stats = wrapit(ReadStats)
    read_stats.wrapped.reads = clean_reads
    kraken2 = wrapit(Kraken2)
    kraken2.wrapped.reads = clean_reads
    braken = wrapit(BrakenKraken2)
    braken.wrapped.report = kraken2
    braken.wrapped.reads = clean_reads

    processed = ProcessedReads.from_sample(sample, config_path, cores=cores)
    processed.hmp = hmp
    processed.humann2 = humann2
    processed.kraken2 = braken
    processed.mash = mash
    processed.read_stats = read_stats
    return processed, [dmnd_uniref90, humann2, mash, hmp, read_stats, kraken2, braken]


def get_task_list_for_sample(sample, stage, upload=True, download_only=False, config_path='', cores=1, require_clean_reads=False):
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
        sample, CleanReads, upload=upload, download_only=download_only, config_path=config_path, cores=cores
    )
    clean_reads.wrapped.ec_reads.nonhuman_reads.adapter_removed_reads.reads = reads
    if require_clean_reads:
        clean_reads.download_only = True
    # reads stage
    processed, read_task_list = get_task_list_for_read_stage(
        sample, clean_reads, upload=upload, download_only=download_only, config_path=config_path, cores=cores
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
    if stage == 'data':
        tasks = [reads]
    if stage == 'qc':
        tasks = [fastqc]
    if stage == 'pre':
        tasks = [clean_reads]
    if stage == 'reads':
        tasks = [clean_reads, processed] + read_task_list
    if stage == 'assembly':
        tasks = [clean_reads, assembly]
    if stage == 'all':
        tasks = [full]
    return tasks
