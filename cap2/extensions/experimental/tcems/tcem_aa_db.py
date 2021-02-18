
import luigi
from os.path import join, dirname
import sqlite3
import gzip
from Bio import SeqIO
from multiprocessing import Pool

from ....pipeline.utils.cap_task import CapDbTask
from ....pipeline.config import PipelineConfig
from ....setup_logging import *
import logging

logger = logging.getLogger('cap2')


def get_taxa(val):
    out, taxon, inblock = set(), '', False
    for char in val:
        if char == '[':
            inblock = True
        elif char == ']':
            out.add(taxon)
            inblock = False
            taxon = ''
        elif inblock:
            taxon += char
    return out


def get_aa_kmers(seq, k=5):
    out = set()
    for i in range(0, len(seq) - k + 1):
        kmer = seq[i:i + k]
        out.add(str(kmer))
    return out


def process_one_seq(rec):
    taxa = get_taxa(rec.description)
    kmers = get_aa_kmers(rec.seq)
    out = set()
    for taxon in taxa:
        for kmer in kmers:
            out.add((taxon, kmer))
    return out


class TcemNrAaDb(CapDbTask):
    config_filename = luigi.Parameter()
    cores = luigi.IntParameter(default=1)
    MODULE_VERSION = 'v0.1.0'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.config = PipelineConfig(self.config_filename)
        self.db_dir = self.config.db_dir
        self.fasta = join(self.db_dir, 'ncbi_nr_fasta', 'nr.gz')
        self.flush_count = 1000 * 1000

    def tool_version(self):
        return self.version()

    def requires(self):
        return []

    @classmethod
    def _module_name(cls):
        return 'tcem_nr_aa_db'

    @classmethod
    def dependencies(cls):
        return ['2021-02-13']

    @property
    def tcem_index(self):
        return join(self.db_dir, 'ncbi_nr_fasta', 'tcems_aa_kmers.sqlite')

    def output(self):
        tcem_index = luigi.LocalTarget(self.tcem_index)
        tcem_index.makedirs()
        return {
            'tcem_index': tcem_index,
        }

    def run(self):
        logger.info(f'Running in database mode: {self.config.db_mode}')
        if self.config.db_mode == PipelineConfig.DB_MODE_BUILD:
            self.build_db()

    def build_db(self):
        logger.info(f'Building TCEM database from {self.fasta}')
        conn = sqlite3.connect(self.tcem_index)
        c = conn.cursor()
        c.execute('''CREATE TABLE taxa_kmers (taxon text, kmer text, UNIQUE(taxon,kmer))''')
        full_set, seq_counter = set(), 0
        with Pool(self.cores) as pool, gzip.open(self.fasta, 'rt') as f:
            seqs = SeqIO.parse(f, 'fasta')
            for taxa_kmer_set in pool.imap_unordered(process_one_seq, seqs):
                full_set |= taxa_kmer_set
                seq_counter += 1
                if seq_counter % (10 * 1000) == 0:
                    logger.info(f'Processed {seq_counter} sequences')
                if len(full_set) >= self.flush_count:
                    logger.info(f'Writing {len(full_set)} taxon kmer pairs to sqlite db')
                    c.executemany('INSERT OR IGNORE INTO taxa_kmers VALUES (?,?)', full_set)
                    full_set = set()
        logger.info(f'Processed {seq_counter} sequences')
        logger.info(f'Writing {len(full_set)} taxon kmer pairs to sqlite db')
        c.executemany('INSERT OR IGNORE INTO taxa_kmers VALUES (?,?)', full_set)
        c.execute('''CREATE INDEX kmer_index ON taxa_kmers(kmer)''')
        conn.commit()
        conn.close()


