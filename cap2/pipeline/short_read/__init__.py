
from .krakenuniq import KrakenUniq
from .humann2 import MicaUniref90, Humann2
from .mash import Mash
from .hmp_comparison import HmpComparison
from .microbe_census import MicrobeCensus
from .read_stats import ReadStats
from .amrs import GrootAMR

MODULES = [
    KrakenUniq,
    MicaUniref90,
    Humann2,
    Mash,
    HmpComparison,
    MicrobeCensus,
    ReadStats,
    GrootAMR,
]
