
from .clean_reads import CleanReads
from .count_reads import CountRawReads
from .fastqc import FastQC
from .map_to_human import RemoveHumanReads
from .map_to_mouse import RemoveMouseReads
from .remove_adapters import AdapterRemoval
from .error_correct_reads import ErrorCorrectReads
from .multiqc import MultiQC
from .base_reads import BaseReads
from .basic_sample_stats import BasicSampleStats
from .fast_taxa import FastKraken2

MODULES = [
    CleanReads,
]

QC_MODULES = [
    CountRawReads,
    FastQC,
    BasicSampleStats,
    FastKraken2,
]

QC_GRP_MODULES = [
    MultiQC,
]
