
from .clean_reads import CleanReads
from .count_reads import CountRawReads
from .fastqc import FastQC
from .map_to_human import RemoveHumanReads
from .remove_adapters import AdapterRemoval
from .error_correct_reads import ErrorCorrectReads
from .multiqc import MultiQC
from .base_reads import BaseReads

MODULES = [
    CleanReads,
]

QC_MODULES = [
    CountRawReads,
    FastQC,
]

QC_GRP_MODULES = [
    MultiQC,
]
