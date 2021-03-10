
from .tasks import StrainCapGroupTask
from ....pipeline.config import PipelineConfig

from .strainotyping import (
    VERSION,
    partition,
    load_graph_from_filepath,
)
from .merge_snp_graph import MergeSNPGraph


class MergeSNPClusters(StrainCapGroupTask):
    module_description = """
    This module 

    Motivation: 

    Negatives: 
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.config = PipelineConfig(self.config_filename)
        self.graph = MergeSNPGraph(
            genome_name=self.genome_name,
            genome_path=self.genome_path,
            group_name=self.group_name,
            samples=self.samples,
            config_filename=self.config_filename,
            cores=self.cores,
        )


    def requires(self):
        return self.graph

    @classmethod
    def version(cls):
        return 'v0.1.0'

    def tool_version(self):
        return VERSION

    @classmethod
    def dependencies(cls):
        return [MergeSNPGraph]

    @classmethod
    def _module_name(cls):
        return 'experimental::merge_snp_clusters'

    def output(self):
        out = {
            f'merged_snp_clusters__{self.genome_name}': self.get_target(f'merged_snp_clusters__{self.genome_name}', 'csv.gz'),        }
        return out

    @property
    def cluster_path(self):
        return self.output()[f'merged_snp_clusters__{self.genome_name}'].path

    def _run(self):
        graph = load_graph_from_filepath(self.graph.graph_path)
        tbl = partition(graph)
        tbl.to_csv(self.cluster_path, compression='gzip')
