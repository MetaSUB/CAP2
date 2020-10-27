from gimmebio.seqs import reverseComplement
import networkx as nx


def get_mismatches(rec):
    """Yield mismatches between a read and a reference based on a BAM alignment."""
    qseq = rec.get_forward_sequence().upper()
    if rec.is_reverse:
        qseq = reverseComplement(qseq)
    rseq = rec.get_reference_sequence().upper()
    for qpos, rpos in rec.get_aligned_pairs():
        if qpos == None or rpos == None:
            continue  # no indels yet
        q = qseq[qpos]
        r = rseq[rpos - rec.reference_start]
        if q != r:
            position = (rec.reference_name, rpos)
            change = (r, q)
            yield (position, change)


def filter_to_region(node, contig=None, coords=None):
    """Return True iff a node is within a given region (and region is specified)."""
    ((seq, coord), miss) = node
    if contig and seq != contig:
        return False
    if coords and coord < coords[0]:
        return False
    if coords and coord > coords[1]:
        return False
    return True


def build_graph(rec_iter, G=nx.Graph(), contig=None, coords=None):
    """Return a weighted Graph built from the supplied BAM iterator.

    Nodes are variants `(position, base)`
    Edges are between variants that occur on the same read.

    Filtered by region, if specified
    """
    for rec in rec_iter:
        misses = list(get_mismatches(rec))
        misses = [
            miss for miss in misses
            if filter_to_region(miss, contig=contig, coords=coords)
        ]
        for missA in misses:
            for missB in misses:
                if missA == missB:
                    break
                try:
                    w = G[missA][missB]['weight']
                except KeyError:
                    w = 0
                G.add_edge(missA, missB, weight=w + 1)
    return G


def merge_graphs(graphs, contig=None, coords=None):
    """Return a graph that is the concatenation of `graphs`."""
    pass
