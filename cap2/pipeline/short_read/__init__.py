
from .processed_reads import ProcessedReads
from .kraken2 import Kraken2, BrakenKraken2
from .humann2 import MicaUniref90, Humann2
from .mash import Mash
from .hmp_comparison import HmpComparison
from .microbe_census import MicrobeCensus
from .read_stats import ReadStats
from .amrs import GrootAMR
from .jellyfish import Jellyfish

MODULES = [
    ProcessedReads,
]
