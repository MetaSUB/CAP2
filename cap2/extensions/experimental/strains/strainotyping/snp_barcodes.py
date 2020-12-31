
import networkx as nx


class SNPBarcode:

    def __init__(self, tbl):
        self.tbl = tbl
        self.sample_name = tbl['sample_name'].unique()[0]
        self.seq = tbl['seq'].unique()[0]
        self.snps = set(zip(tbl['coord'], tbl['changed']))

    def __len__(self):
        return len(self.snps)


class SNPBarcodeSet:

    def __init__(self):
        self.seq = None
        self.snps = set([])
        self.barcodes = []

    def __len__(self):
        return len(self.snps)

    def add_barcode(self, bc):
        if self.seq:
            assert bc.seq == self.seq
        else:
            self.seq = bc.seq
        self.snps |= bc.snps
        self.barcodes.append(bc)
        return self


def barcode_barcode_similarity(b1, b2):
    if b1.seq != b2.seq:
        return 0
    jaccard = len(b1.snps & b2.snps) / len(b1.snps | b2.snps)
    return jaccard


def barcode_barcode_set_similarity(bc, bc_set):
    if bc.seq != bc_set.seq:
        return 0
    jaccard = len(bc.snps & bc_set.snps) / len(bc.snps)
    return jaccard


def build_barcode_sets(barcodes, sim_thresh=0.5):
    """Return a list of SNPBarcodeSets that fulfill sevreal reqs.

     - all barcodes are in one or more barcode sets
     - each barcode in a set has similarity of at least sim_thresh to that set
    """
    barcode_sets = []
    for bc in barcodes:
        added_to_bc_set = False
        for bc_set in barcode_sets:
            s = barcode_barcode_set_similarity(bc, bc_set)
            if s < sim_thresh:
                continue
            added_to_bc_set = True
            bc_set.add_barcode(bc)
        if not added_to_bc_set:
            new_bc_set = SNPBarcodeSet().add_barcode(bc)
            barcode_sets.append(new_bc_set)
    return barcode_sets


def barcode_barcode_similarity_graph(barcodes, sim_thresh=0.5):
    """Return a Graph with edges between similar barcodes.

    - Barcodes with no similar barcdoes are not included
    - weight of each edge is the similarity
    """
    barcode_sets = build_barcode_sets(barcodes, sim_thresh=sim_thresh)
    G = nx.Graph()
    for bc_set in barcode_sets:
        for bc1 in bc_set.barcodes:
            for bc2 in bc_set.barcodes:
                if bc1 == bc2:
                    break
                s = barcode_barcode_similarity(bc1, bc2)
                if s >= sim_thresh:
                    G.add_edge(bc1, bc2, weight=s)
    return G
