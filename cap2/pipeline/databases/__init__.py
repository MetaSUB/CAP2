
from .mouse_removal_db import MouseRemovalDB
from .human_removal_db import HumanRemovalDB
from .hmp_db import HmpDB
from .uniref import Uniref90
from .kraken2_db import Kraken2DB, BrakenKraken2DB


MODULES = [
    HumanRemovalDB,
    HmpDB,
    Uniref90,
    Kraken2DB,
    BrakenKraken2DB,
]
