
import networkx as nx

from .make_snp_graph import MakeSNPGraph

from .tasks import StrainCapTask
from ....pipeline.config import PipelineConfig

from .strainotyping import (
    VERSION,
    graph_node_table,
    load_graph_from_filepath,
)


class MakeSNPTable(StrainCapTask):
    module_description = """
    This module 

    Motivation: 

    Negatives: 
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.config = PipelineConfig(self.config_filename)
        self.graph = MakeSNPGraph(
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
        return self.graph

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
        return 'experimental::make_snp_table'

    def output(self):
        out = {
            f'snp_nodes__{self.genome_name}': self.get_target(f'snp_nodes__{self.genome_name}', 'tsv.gz'),
        }
        return out

    @property
    def node_path(self):
        return self.output()[f'snp_nodes__{self.genome_name}'].path

    def _run(self):
        graph = load_graph_from_filepath(self.graph.graph_path)
        tbl = graph_node_table(graph)
        tbl.to_csv(self.node_path, compression='gzip')
