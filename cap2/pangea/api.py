
from .load_task import PangeaLoadTask
from ..pipeline.preprocessing import FastQC
from ..pipeline.preprocessing import CleanReads
from ..pipeline.short_read import MODULES as SHORT_READ_MODULES


def wrap_task(sample, module, requires_reads=True, upload=True):
    task = PangeaLoadTask(
        pe1=sample.r1,
        pe2=sample.r2,
        sample_name=sample.name,
    )
    task.upload_allowed = upload
    task.wrapped_module = module
    task.requires_reads = requires_reads
    return task


def get_task_list_for_sample(sample, stage, upload=True):
    tasks = []
    for module in [FastQC, CleanReads] + SHORT_READ_MODULES:
        tasks.append(wrap_task(sample, module, upload=upload))
    return tasks
