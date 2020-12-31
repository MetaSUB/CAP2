
import pysam
import networkx as nx

from .io import (
    load_graph_from_filepath,
    write_graph_to_filepath,
)
from .graphs import (
    build_graph,
    append_graph,
    filter_graph_by_weight,
)


def graph_from_bam_filepath(filepath, G=nx.Graph()):
    """Build a SNP graph from a BAM file."""
    bam = pysam.AlignmentFile(filepath, 'rb')
    G = build_graph(bam, G=G)
    return G


def merge_filter_graphs_from_filepaths(filepaths, min_weight=2):
    G = nx.Graph()
    for filepath in filepaths:
        new_graph = load_graph_from_filepath(filepath)
        G = append_graph(G, new_graph)
    G = filter_graph_by_weight(G, min_weight=min_weight)
    return G
