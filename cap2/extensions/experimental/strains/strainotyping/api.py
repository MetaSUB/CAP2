
import pysam
import networkx as nx

from .graphs import build_graph


def graph_from_bam_filepath(filepath, G=nx.Graph()):
    """Build a SNP graph from a BAM file."""
    bam = pysam.AlignmentFile(filepath, 'rb')
    G = build_graph(bam, G=G)
    return G
