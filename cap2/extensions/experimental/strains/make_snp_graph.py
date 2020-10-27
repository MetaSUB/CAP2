
import luigi
import subprocess
from os.path import join, dirname, basename

from .align_to_genome import AlignReadsToGenome

from .tasks import StrainCapTask
from ....pipeline.config import PipelineConfig
from ....pipeline.utils.conda import CondaPackage
from ....pipeline.preprocessing.map_to_human import RemoveHumanReads

from .strainotyping import VERSION


class MakeSNPGraph(StrainCapTask):
    module_description = """
    This module 

    Motivation: 

    Negatives: 
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.config = PipelineConfig(self.config_filename)
        self.bam = AlignReadsToGenome(
            genome_name=self.genome_name,
            genome_path=self.genome_path,
            pe1=self.pe1,
            pe2=self.pe2,
            sample_name=self.sample_name,
            config_filename=self.config_filename,
            cores=self.cores,
            data_type=self.data_type,
        )

    def requires(self):
        return self.bam

    @classmethod
    def version(cls):
        return 'v0.1.0'

    def tool_version(self):
        return VERSION

    @classmethod
    def dependencies(cls):
        return [AlignReadsToGenome]

    @classmethod
    def _module_name(cls):
        return 'experimental::make_snp_graph'

    def output(self):
        out = {
            f'snp_graph__{self.genome_name}': self.get_target(f'snp_graph__{self.genome_name}', 'gml.gz'),
        }
        return out

    @property
    def graph_path(self):
        return self.output()[f'snp_graph__{self.genome_name}'].path

    def _run(self):
        graph = graph_from_bam_filepath(self.bam.bam_path)
        prezip_path = self.graph_path.replace('.gz', '')
        nx.write_graphml(graph, prezip_path)
        self.run_cmd(f'gzip {prezip_path}')
