
from .count_reads import CountRawReads
from .fastqc import FastQC
from .map_to_human import RemoveHumanReads
from .error_correct_reads import ErrorCorrectReads
from .multiqc import MultiQC

MODULES = [
    CountRawReads,
    FastQC,
    RemoveHumanReads,
    ErrorCorrectReads,
]

QC_MODULES = [
    CountRawReads,
    FastQC,
]

QC_GRP_MODULES = [
    MultiQC,
]
