
from .utils.cap_task import CapTask
from .short_read.processed_reads import ProcessedReads


class FullPipeline(CapTask):
    """This class represents the culmination of the
    shortread pipeline.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @classmethod
    def version(cls):
        return 'v2.0.0dev'

    @classmethod
    def dependencies(cls):
        return [ProcessedReads]

    @classmethod
    def _module_name(cls):
        return 'metasub_cap'

    def output(self):
        return {}

    def _run(self):
        pass
