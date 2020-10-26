
import networkx as nx
import gzip


def load_graph_from_filepath(filepath):
    handle = None
    try:
        if filepath.endswith('.gz'):
            handle = gzip.open(filepath)
        else:
            handle = open(filepath)
        G = nx.read_graphml(handle)
        newG = nx.Graph()
        for a, b, d in G.edges(data=True):
            newG.add_edge(eval(a), eval(b), weight=d['weight'])
        return newG
    finally:
        if handle:
            handle.close()


def write_graph_to_filepath(graph, filepath):
    handle = None
    try:
        if filepath.endswith('.gz'):
            handle = gzip.open(filepath, 'wb')
        else:
            handle = open(filepath, 'wb')
        return nx.write_graphml(graph, handle)
    finally:
        if handle:
            handle.close()
