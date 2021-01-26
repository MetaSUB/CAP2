
from .load_task import PangeaCapTask
from ..pipeline.preprocessing import FastQC
from ..pipeline.preprocessing import CleanReads
from ..pipeline.preprocessing import RemoveMouseReads
from ..pipeline.preprocessing import RemoveHumanReads
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
    Jellyfish,
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
    'kmer',
]


def wrap_task(sample, module, config_path='', **kwargs):
    task = PangeaCapTask.new_task_type(module)(
        pe1=sample.r1,
        pe2=sample.r2,
        sample_name=sample.name,
        config_filename=config_path,
        **kwargs,
    )
    return task


def get_task_list_for_kmer_stage(sample, clean_reads, config_path='', **kwargs):
    wrapit = lambda module: wrap_task(sample, module, config_path=config_path, **kwargs)
    jellyfish = wrapit(Jellyfish)
    jellyfish.reads = clean_reads

    return [jellyfish]


def get_task_list_for_read_stage(sample, clean_reads, config_path='', **kwargs):
    wrapit = lambda module: wrap_task(sample, module, config_path=config_path, **kwargs)
    dmnd_uniref90 = wrapit(MicaUniref90)
    dmnd_uniref90.reads = clean_reads
    humann2 = wrapit(Humann2)
    humann2.alignment = dmnd_uniref90
    mash = wrapit(Mash)
    mash.reads = clean_reads
    jellyfish = wrapit(Jellyfish)
    jellyfish.reads = clean_reads
    hmp = wrapit(HmpComparison)
    hmp.mash = mash
    read_stats = wrapit(ReadStats)
    read_stats.reads = clean_reads
    kraken2 = wrapit(Kraken2)
    kraken2.reads = clean_reads
    braken = wrapit(BrakenKraken2)
    braken.report = kraken2
    braken.reads = clean_reads

    processed = ProcessedReads.from_sample(sample, config_path, cores=cores, max_ram=max_ram)
    processed.hmp = hmp
    processed.humann2 = humann2
    processed.kraken2 = braken
    processed.mash = mash
    processed.read_stats = read_stats
    return processed, [dmnd_uniref90, humann2, mash, hmp, read_stats, kraken2, braken, jellyfish]


def get_task_list_for_sample(sample, stage, config_path='', require_clean_reads=False, **kwargs):
    reads = wrap_task(
        sample, BaseReads, upload=False, config_path=config_path, requires_reads=True, **kwargs
    )
    wrapit = lambda module: wrap_task(sample, module, config_path=config_path, **kwargs)

    # qc stage
    fastqc = wrapit(FastQC)
    fastqc.reads = reads

    # pre stage
    nonhuman_reads = wrapit(RemoveHumanReads)
    nonhuman_reads.mouse_removed_reads.adapter_removed_reads.reads = reads
    clean_reads = wrapit(CleanReads)
    clean_reads.ec_reads.nonhuman_reads = nonhuman_reads
    if require_clean_reads:
        clean_reads.download_only = True

    # reads stage
    processed, read_task_list = get_task_list_for_read_stage(sample, clean_reads, config_path=config_path, **kwargs)

    # kmer stage
    kmer_task_list = get_task_list_for_kmer_stage(sample, clean_reads, config_path=config_path, **kwargs)

    # assembly stage
    assembly = wrapit(MetaspadesAssembly)
    assembly.reads = clean_reads

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
    if stage == 'kmer':
        tasks = [clean_reads] + kmer_task_list
    if stage == 'assembly':
        tasks = [clean_reads, assembly]
    if stage == 'all':
        tasks = [full]
    return tasks
