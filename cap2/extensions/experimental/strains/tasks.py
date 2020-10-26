
import luigi

from ....pangea.load_task import PangeaLoadTask, PangeaGroupLoadTask
from ....pipeline.utils.cap_task import (
    BaseCapTask,
    CapTask,
    CapDbTask,
    CapGroupTask,
)
from .utils import clean_microbe_name


class StrainBaseCapTask(BaseCapTask):
    genome_name = luigi.Parameter()  # A genome name with only lowercase characters and underscores
    genome_path = luigi.Parameter(default='', significant=False)  # A filepath to a folder containing fastas

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if self.genome_name != clean_microbe_name(self.genome_name):
            raise ValueError(f'genome_name `{self.genome_name}` is not cleaned')
        if ' ' in self.genome_name:
            raise ValueError(f'genome_name `{self.genome_name}` contains whitespace')


class StrainCapTask(CapTask, StrainBaseCapTask):
    pass


class StrainCapDbTask(CapDbTask, StrainBaseCapTask):
    pass


class StrainCapGroupTask(CapGroupTask, StrainBaseCapTask):

    def _make_req_module(self, module_type, pe1, pe2, sample_name, config_filename):
        return module_type(
            pe1=pe1,
            pe2=pe2,
            sample_name=sample_name,
            config_filename=config_filename,
            genome_name=self.genome_name,
            genome_path=self.genome_path,
        )

    @classmethod
    def from_samples(cls, group_name, samples, config_path='', cores=1, genome_name='', genome_path=''):
        samples = [s if isinstance(s, tuple) else s.as_tuple() for s in samples]
        return cls(
            group_name=group_name,
            samples=tuple(samples),
            config_filename=config_path,
            cores=cores,
            genome_name=genome_name,
            genome_path=genome_path,
        )


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


class StrainPangeaGroupLoadTask(PangeaGroupLoadTask, StrainCapGroupTask):

    @property
    def wrapped(self):
        instance = self.wrapped_module.from_samples(
            self.group_name,
            self.samples,
            self.config_filename,
            genome_name=self.genome_name,
            genome_path=self.genome_path,
        )
        instance._make_req_module = self._make_req_module
        return instance
