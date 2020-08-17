
import luigi
import json

from .preclassify import PreclassifySample

from ....pangea.api import get_task_list_for_sample
from ....pangea.pangea_sample import PangeaSample
from ....pipeline.utils.cap_task import CapTask
from ....pipeline.config import PipelineConfig
from ....constants import STAGES


class DynamicPipelineSample(CapTask):
    pipeline_stage = luigi.Parameter(default='reads')
    pangea = luigi.Parameter(default=False)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.config = PipelineConfig(self.config_filename)
        self.out_dir = self.config.out_dir
        self._sample_type = PreclassifySample(
            sample_name=self.sample_name,
            pe1=self.pe1,
            pe2=self.pe2,
            config_filename=self.config_filename
        )

    @classmethod
    def _module_name(cls):
        return 'dynamically_pipeline_sample'

    @classmethod
    def version(cls):
        return 'v0.1.0'

    @classmethod
    def dependencies(cls):
        return [PreclassifySample]

    def sample_type(self):
        path = self._sample_type.output()['report'].path
        blob = json.loads(open(path).read())
        return blob['sample_type']

    def run_pipeline(self):
        print(self.sample_type)
        if self.sample_type in ['METAGENOME', 'AMBIGUOUS']:
            return True
        return False

    def complete(self):
        return False
        if not self._sample_type.complete():
            return False
        if self.run_pipeline():
            for depends in self._get_instance_list():
                if not depends.complete():
                    return False
        return True

    def _run(self):
        yield [self._sample_type]
        if not self.run_pipeline():
            return
        instances = self._get_instance_list()
        yield instances

    def _get_instance_list(self):
        if not self.pangea:
            instances = self._get_local_instance_list()
        else:
            instances = self._get_pangea_instance_list()
        return instances

    def _get_pangea_instance_list(self):
        psample = PangeaSample(
            self.sample_name,
            None,
            None,
            None,
            None,
            None,
        )
        instances = get_task_list_for_sample(psample, self.pipeline_stage, cores=self.cores)
        return instances

    def _get_local_instance_list(self):
        modules = STAGES[self.pipeline_stage]
        instances = []
        for module in modules:
            instances.append(module(
                pe1=self.pe1,
                pe2=self.pe2,
                sample_name=self.sample_name,
                config_filename=self.config_filename,
                cores=self.cores
            ))
        return instances
