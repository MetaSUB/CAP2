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


def append_graph(target_graph, new_graph, contig=None, coords=None):
    for s, t, d in new_graph.edges(data=True):
        if not filter_to_region(s, contig=contig, coords=coords):
            continue
        if not filter_to_region(t, contig=contig, coords=coords):
            continue
        try:
            w = target_graph[s][t]['weight']
        except KeyError:
            w = 0
        w += d['weight']
        target_graph.add_edge(s, t, weight=w)
    return target_graph


def merge_graphs(graphs, G=nx.Graph(), contig=None, coords=None):
    """Return a graph that is the concatenation of `graphs`."""
    for graph in graphs:
        G = append_graph(G, graph, contig=contig, coords=coords)
    return G


def get_node_weights(G):
    node_weights = {}
    for node in G.nodes():
        total_weight = 0
        for el in G.edges(node, data=True):
            total_weight += el[2]['weight']
        node_weights[node] = total_weight
    return node_weights


def filter_graph_by_weight(G, min_weight=2):
    for node, w in get_node_weights(G).items():
        if w < min_weight:
            G.remove_node(node)
    return G
