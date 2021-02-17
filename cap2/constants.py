
from .pipeline.preprocessing import MODULES as PRE_MODULES
from .pipeline.preprocessing import QC_MODULES, QC_GRP_MODULES
from .pipeline.short_read import MODULES as SHORT_READ_MODULES


STAGES = {
    'qc': QC_MODULES,
    'pre': PRE_MODULES,
    'reads': SHORT_READ_MODULES,
}
DEFAULT_STAGE = 'reads'

STAGES_GROUP = {
    'qc': QC_GRP_MODULES,
}

DATA_TYPES = [
    'short_read',  # CAP2 will try to figure out if reads are paired but may fail
    'single_short_read',
    'paired_short_read',
]
