
from .utils.cap_task import CapTask
from .short_read.processed_reads import ProcessedReads
from .assembly.metaspades import MetaspadesAssembly
from .preprocessing.fastqc import FastQC


class FullPipeline(CapTask):
    """This class represents the culmination of the
    shortread pipeline.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.qc = FastQC(
            sample_name=self.sample_name,
            pe1=self.pe1,
            pe2=self.pe2,
            config_filename=self.config_filename,
            cores=self.cores,
        )
        self.short_reads = ProcessedReads(
            sample_name=self.sample_name,
            pe1=self.pe1,
            pe2=self.pe2,
            config_filename=self.config_filename,
            cores=self.cores,
        )
        self.assembly = MetaspadesAssembly(
            sample_name=self.sample_name,
            pe1=self.pe1,
            pe2=self.pe2,
            config_filename=self.config_filename,
            cores=self.cores,
        )

    def requires(self):
        return self.qc, self.short_reads, self.assembly

    @classmethod
    def version(cls):
        return 'v2.0.0dev'

    @classmethod
    def dependencies(cls):
        return [ProcessedReads, MetaspadesAssembly]

    @classmethod
    def _module_name(cls):
        return 'metasub_cap'

    def complete(self):
        for depends in self.requires():
            if not depends.complete():
                return False
        return True

    def output(self):
        return {}

    def _run(self):
        pass
