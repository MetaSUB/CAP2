
import luigi

from ....pangea.load_task import PangeaLoadTask
from ....pipeline.utils.cap_task import (
    BaseCapTask,
    CapTask,
    CapDbTask,
)


class StrainBaseCapTask(BaseCapTask):
    genome_name = luigi.Parameter()  # A genome name with only lowercase characters and underscores
    genome_path = luigi.Parameter(default='', significant=False)  # A filepath to a folder containing fastas


class StrainCapTask(CapTask, StrainBaseCapTask):
    pass


class StrainCapDbTask(CapDbTask, StrainBaseCapTask):
    pass


class StrainPangeaLoadTask(PangeaLoadTask, StrainCapTask):

    @property
    def wrapped(self):
        if self._wrapped:
            return self._wrapped
        instance = self.wrapped_module(
            pe1=self.pe1,
            pe2=self.pe2,
            sample_name=self.sample_name,
            config_filename=self.config_filename,
            cores=self.cores,
            genome_name=self.genome_name,
            genome_path=self.genome_path,
        )
        instance.pre_run_hooks.append(self._download_reads)
        self._wrapped = instance
        return self._wrapped
