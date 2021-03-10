
from .tasks import StrainCapGroupTask
from ....pipeline.config import PipelineConfig

from .strainotyping import (
    VERSION, 
    merge_filter_graphs_from_filepaths,
    write_graph_to_filepath,
    graph_node_table,
)
from .make_snp_graph import MakeSNPGraph


class MergeSNPGraph(StrainCapGroupTask):
    MIN_WEIGHT = 2
    module_description = """
    This module 

    Motivation: 

    Negatives: 
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.config = PipelineConfig(self.config_filename)

    @property
    def snp_graphs(self):
        return self.module_req_list(MakeSNPGraph)

    def requires(self):
        return self.snp_graphs

    @classmethod
    def version(cls):
        return 'v0.1.0'

    def tool_version(self):
        return VERSION

    @classmethod
    def dependencies(cls):
        return [MakeSNPGraph]

    @classmethod
    def _module_name(cls):
        return 'experimental::merge_snp_graph'

    def output(self):
        out = {
            f'merged_snp_graph__{self.genome_name}': self.get_target(f'merged_snp_graph__{self.genome_name}', 'gml.gz'),
            f'merged_snp_nodes__{self.genome_name}': self.get_target(f'merged_snp_nodes__{self.genome_name}', 'csv.gz'),
        }
        return out

    @property
    def graph_path(self):
        return self.output()[f'merged_snp_graph__{self.genome_name}'].path

    @property
    def node_path(self):
        return self.output()[f'merged_snp_nodes__{self.genome_name}'].path

    def _run(self):
        graph_paths = [snp_graph.graph_path for snp_graph in self.snp_graphs]
        merged_graph = merge_filter_graphs_from_filepaths(graph_paths, min_weight=self.MIN_WEIGHT)
        write_graph_to_filepath(merged_graph, self.graph_path)
        tbl = graph_node_table(merged_graph)
        tbl.to_csv(self.node_path, compression='gzip')
