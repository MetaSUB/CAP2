
from .load_task import PangeaLoadTask
from ..pipeline.preprocessing import FastQC
from ..pipeline.preprocessing import CleanReads
from ..pipeline.preprocessing import AdapterRemoval
from ..pipeline.short_read import MODULES as SHORT_READ_MODULES
from ..pipeline.assembly.metaspades import MetaspadesAssembly 

STAGES = {
    'qc': [FastQC],
    'pre': [CleanReads],
    'reads': SHORT_READ_MODULES,
    'assembly': [MetaspadesAssembly],
}


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
    ar = wrap_task(sample, AdapterRemoval, upload=False)
    clean_reads = wrap_task(sample, CleanReads, requires_reads=False)
    clean_reads.ec_reads.nonhuman_reads.reads = AR
    tasks = [
        clean_reads,
        wrap_task(sample, FastQC)
    ]
    return tasks

