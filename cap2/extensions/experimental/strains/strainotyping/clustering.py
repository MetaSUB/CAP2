
import pandas as pd
import community
from .graphs import get_node_weights


def partition(G):
    rows = []
    partition = community.best_partition(G)
    weights = get_node_weights(G)
    out = {}
    for node, cluster in partition.items():
        ((seq, coord), (original, changed)) = node
        rows.append({
            'seq': seq,
            'coord': coord,
            'original': original,
            'changed': changed,
            'weight': weights[node],
            'cluster': cluster,
        })
    tbl = pd.DataFrame(rows)
    return tbl
