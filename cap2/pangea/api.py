
from .load_task import PangeaCapTask
from ..pipeline.utils.cap_task import CapTask

from ..pipeline.preprocessing import (
    BaseReads,
    FastQC,
    CleanReads,
    RemoveMouseReads,
    RemoveHumanReads,
    AdapterRemoval,
    ErrorCorrectReads,
)
from ..pipeline.short_read import (
    ProcessedReads,
    Jellyfish,
)
from ..pipeline.assembly.metaspades import MetaspadesAssembly

STAGES = [
    'data',
    'qc',
    'pre',
    'reads',
    'assembly',
    'kmer',
]


def wrap_task(sample, module, config_path='', no_wrap=False, **kwargs):
    task_type = module
    if not no_wrap:
        task_type = PangeaCapTask.new_task_type(module)
    task = task_type(
        pe1=sample.r1,
        pe2=sample.r2,
        sample_name=sample.name,
        config_filename=config_path,
        **kwargs,
    )
    return task


def recursively_wrap_task(sample, module,
                          config_path='',
                          no_wrap_tasks=[],
                          no_recurse_tasks=[],
                          module_substitute_tasks={},
                          **kwargs):
    if module in module_substitute_tasks:
        task = module_substitute_tasks[module]
    else:
        task = wrap_task(sample, module,
                         config_path=config_path,
                         no_wrap=(module in no_wrap_tasks),
                         **kwargs)
    if module in no_recurse_tasks:
        return task

    attr_dict = task.__dict__
    if hasattr(task, 'wrapped_instance'):
        attr_dict = task.wrapped_instance.__dict__

    for attr, value in attr_dict.items():
        if isinstance(value, CapTask):
            subtask = recursively_wrap_task(sample, type(value),
                                            config_path=config_path,
                                            no_wrap_tasks=no_wrap_tasks,
                                            no_recurse_tasks=no_recurse_tasks,
                                            module_substitute_tasks=module_substitute_tasks,
                                            **kwargs)
            setattr(task, attr, subtask)
    return task


def kmer_stage_task(sample, clean_reads, config_path='', **kwargs):
    jellyfish = wrap_task(sample, Jellyfish, config_path=config_path, **kwargs)
    jellyfish.reads = clean_reads
    return jellyfish


def read_stage_task(sample, clean_reads, config_path='', **kwargs):
    processed = recursively_wrap_task(
        sample,
        ProcessedReads,
        config_path=config_path,
        no_recurse_tasks=[CleanReads],
        module_substitute_tasks={CleanReads: clean_reads},
        **kwargs,
    )
    return processed


def assembly_stage_task(sample, clean_reads, config_path='', **kwargs):
    metaspades = wrap_task(sample, MetaspadesAssembly, config_path=config_path, **kwargs)
    metaspades.reads = clean_reads
    return metaspades


def qc_stage_task(sample, base_reads, config_path='', **kwargs):
    fastqc = wrap_task(sample, FastQC, config_path=config_path, **kwargs)
    fastqc.reads = base_reads
    return fastqc


def pre_stage_task(sample, base_reads, config_path='', **kwargs):
    clean_reads = recursively_wrap_task(
        sample,
        CleanReads,
        config_path=config_path,
        no_wrap_tasks=[RemoveMouseReads, AdapterRemoval, ErrorCorrectReads],
        module_substitute_tasks={BaseReads: base_reads},
        **kwargs,
    )
    return clean_reads


def nonhuman_stage_task(sample, base_reads, config_path='', **kwargs):
    clean_reads = recursively_wrap_task(
        sample,
        RemoveHumanReads,
        config_path=config_path,
        no_wrap_tasks=[RemoveMouseReads, AdapterRemoval],
        module_substitute_tasks={BaseReads: base_reads},
        **kwargs,
    )
    return clean_reads


def get_task_list_for_sample(sample, stage, config_path='', require_clean_reads=False, **kwargs):
    base_reads = wrap_task(
        sample, BaseReads, config_path=config_path, requires_reads=True, **kwargs
    )
    base_reads.upload_allowed = False
    if stage == 'data':
        return [base_reads]
    if stage == 'qc':
        return [qc_stage_task(sample, base_reads, config_path=config_path, **kwargs)]

    clean_reads = pre_stage_task(sample, base_reads, config_path=config_path, **kwargs)
    if require_clean_reads:
        clean_reads.download_only = True

    if stage == 'pre':
        return [clean_reads]
    if stage == 'reads':
        return [read_stage_task(sample, clean_reads, config_path=config_path, **kwargs)]
    if stage == 'kmer':
        return [kmer_stage_task(sample, clean_reads, config_path=config_path, **kwargs)]
    if stage == 'assembly':
        return [assembly_stage_task(sample, clean_reads, config_path=config_path, **kwargs)]
