
import luigi
import logging
import subprocess
import logging
import pandas as pd
import sqlite3
from os.path import join, dirname, basename

from .mixcr import MixcrClones
from .tcem_aa_db import TcemNrAaDb

from ....pipeline.utils.cap_task import CapTask
from ....pipeline.config import PipelineConfig


logger = logging.getLogger('tcems')


def countit(objs):
    """Return a dict with counts for each item in a list."""
    out = {}
    for el in objs:
        out[el] = 1 + out.get(el, 0)
    out = {k: v for k, v in out.items()}
    return out


def get_binding_motifs(seq):
    """Return a dict of dicts with counts for different TCEM motifs."""
    out = {'type_1': [], 'type_2a': [], 'type_2b': []}
    for i in range(len(seq) - 9 + 1):
        kmer = seq[i:i + 9]
        out['type_1'].append(kmer[3:8])
    for i in range(len(seq) - 15 + 1):
        kmer = seq[i:i + 15]
        tail = kmer[5] + kmer[7] + kmer[9] + kmer[10]
        out['type_2a'].append(kmer[4] + tail)
        out['type_2b'].append(kmer[2] + tail)
    counted = {k: countit(v) for k, v in out.items()}
    return counted


def parse_mixcr_table(filepath):
    """Return counts of TCEMs for a set of CDR3 sequences."""
    tbl = pd.read_csv(filepath, sep='\t')
    out = {}
    for _, row in tbl.iterrows():
        motifs = get_binding_motifs(row['aaSeqImputedCDR3'])
        for kind, motif_counts in motifs.items():
            for motif, count in motif_counts.items():
                for mykind in [kind, 'all_types']:
                    key = (mykind, motif)
                    if key not in out:
                        out[key] = {
                            'num_unique_seqs': 0,
                            'num_clones': 0,
                            'num_unique_occurences': 0,
                            'num_clonal_occurences': 0,
                        }
                    out[key]['num_unique_seqs'] += 1
                    out[key]['num_clones'] += row['cloneCount']
                    out[key]['num_unique_occurences'] += count
                    out[key]['num_clonal_occurences'] += count * row['cloneCount']
    return out


class TcemRepertoire(CapTask):
    module_description = """
    This module identifies repertoires of TCEMs in VDJ clonal sequences.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.mixcr = MixcrClones.from_cap_task(self)
        self.config = PipelineConfig(self.config_filename)

    def requires(self):
        return self.mixcr

    @classmethod
    def version(cls):
        return 'v0.1.1'

    def tool_version(self):
        return self.version()

    @classmethod
    def dependencies(cls):
        return [MixcrClones]

    @classmethod
    def _module_name(cls):
        return 'tcems::tcem_repertoire'

    def output(self):
        out = {
            'tcem_counts': self.get_target(f'tcem_repertoire', 'csv'),
        }
        return out

    @property
    def tcem_counts_path(self):
        return self.output()[f'tcem_counts'].path

    def _run(self):
        motif_counts = parse_mixcr_table(self.mixcr.igh_path)
        out = pd.DataFrame.from_dict(motif_counts, orient='index')
        out.to_csv(self.tcem_counts_path)


class AnnotatedTcemRepertoire(CapTask):
    module_description = """
    This module identifies repertoires of TCEMs in VDJ clonal sequences.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.repetoire = TcemRepertoire.from_cap_task(self)
        self.db = TcemNrAaDb.from_cap_task(self)
        self.config = PipelineConfig(self.config_filename)

    def requires(self):
        return self.repetoire

    @classmethod
    def version(cls):
        return 'v0.1.0'

    def tool_version(self):
        return self.version()

    @classmethod
    def dependencies(cls):
        return [TcemRepertoire]

    @classmethod
    def _module_name(cls):
        return 'tcems::annotated_tcem_repertoire'

    def output(self):
        out = {
            'tcem_annotations': self.get_target(f'annotated_tcem_repertoire', 'csv'),
        }
        return out

    @property
    def tcem_annotation_path(self):
        return self.output()[f'tcem_annotations'].path

    def _run(self):
        with sqlite3.connect(self.db.tcem_index) as conn:
            c = conn.cursor()
            rep = pd.read_csv(self.repetoire.tcem_counts_path)
            kmers = rep.iloc[:, 1].unique()
            tbl = {}
            for kmer in kmers:
                tbl[kmer] = {}
                cmd = f'SELECT taxon FROM taxa_kmers WHERE kmer = "{kmer}"'
                for taxon in c.execute(cmd):
                    tbl[kmer][taxon] = 1
        tbl = pd.DataFrame.from_dict(tbl, orient='index')
        tbl.to_csv(self.tcem_annotation_path)
