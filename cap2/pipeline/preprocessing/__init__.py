
from .count_reads import CountRawReads
from .fastqc import FastQC
from .map_to_human import RemoveHumanReads
from .error_correct_reads import ErrorCorrectReads

MODULES = [
    CountRawReads,
    FastQC,
    RemoveHumanReads,
    ErrorCorrectReads,
]
