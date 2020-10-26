import luigi
import logging
import os
import networkx as nx

from shutil import rmtree
from os.path import join, dirname, isfile, isdir, abspath
from unittest import TestCase

from cap2.extensions.experimental.strains.strainotyping import graph_from_bam_filepath
from cap2.extensions.experimental.strains.strainotyping.graphs import (
    merge_graphs,
    filter_graph_by_weight,
)
from cap2.extensions.experimental.strains.strainotyping.io import (
    load_graph_from_filepath,
    write_graph_to_filepath,
)

logging.basicConfig(level=logging.INFO)

BAM_FILEPATH = join(dirname(__file__), 'data/covid/covid_alignment_test_bam.bam')
RAW_READS_1 = join(dirname(__file__), 'data/zymo_pos_cntrl.r1.fq.gz')
RAW_READS_2 = join(dirname(__file__), 'data/zymo_pos_cntrl.r2.fq.gz')
TEST_CONFIG = join(dirname(__file__), 'data/test_config.yaml')


class TestStrainotyping(TestCase):
    NODES = [
        (('seqA', 0), ('A', 'T')),
        (('seqA', 10), ('A', 'C')),
        (('seqA', 100), ('A', 'G')),
        (('seqA', 1000), ('T', 'A')),
        (('seqB', 0), ('A', 'T')),
        (('seqB', 10), ('A', 'C')),
        (('seqB', 100), ('A', 'G')),
        (('seqB', 1000), ('T', 'A')),
    ]

    def test_merge_graphs(self):
        G1 = nx.Graph()
        G1.add_edge(self.NODES[0], self.NODES[1], weight=2)
        G1.add_edge(self.NODES[1], self.NODES[2], weight=3)
        G2 = nx.Graph()
        G2.add_edge(self.NODES[0], self.NODES[1], weight=3)
        G2.add_edge(self.NODES[4], self.NODES[5], weight=10)
        G = merge_graphs([G1, G2])
        self.assertIn(self.NODES[0], G)
        self.assertIn(self.NODES[1], G)
        self.assertIn(self.NODES[2], G1)
        self.assertIn(self.NODES[2], G)
        self.assertIn(self.NODES[4], G)
        self.assertIn(self.NODES[5], G)
        self.assertEqual(G[self.NODES[0]][self.NODES[1]]['weight'], 5)
        self.assertEqual(G[self.NODES[1]][self.NODES[2]]['weight'], 3)

    def test_filter_graph(self):
        G = nx.Graph()
        G.add_edge(self.NODES[0], self.NODES[1], weight=2)
        G.add_edge(self.NODES[1], self.NODES[2], weight=3)
        G.add_edge(self.NODES[4], self.NODES[5], weight=1)
        G = filter_graph_by_weight(G, min_weight=2)
        self.assertTrue(G.has_edge(self.NODES[0], self.NODES[1]))
        self.assertTrue(G.has_edge(self.NODES[1], self.NODES[2]))
        self.assertFalse(G.has_edge(self.NODES[4], self.NODES[5]))

    def test_graph_file_roundtrip(self):
        G = nx.Graph()
        G.add_edge(self.NODES[0], self.NODES[1], weight=2)
        G.add_edge(self.NODES[1], self.NODES[2], weight=3)
        G.add_edge(self.NODES[4], self.NODES[5], weight=1)
        original_graph = G.copy()
        for _ in range(10):
            write_graph_to_filepath(G, 'tmp_graph_path')
            G = load_graph_from_filepath('tmp_graph_path')
        self.assertEqual(list(G.edges(data=True)), list(original_graph.edges(data=True)))

    def test_graph_file_gz_roundtrip(self):
        G = nx.Graph()
        G.add_edge(self.NODES[0], self.NODES[1], weight=2)
        G.add_edge(self.NODES[1], self.NODES[2], weight=3)
        G.add_edge(self.NODES[4], self.NODES[5], weight=1)
        original_graph = G.copy()
        for _ in range(10):
            write_graph_to_filepath(G, 'tmp_graph_path.gz')
            G = load_graph_from_filepath('tmp_graph_path.gz')
        self.assertEqual(list(G.edges(data=True)), list(original_graph.edges(data=True)))
