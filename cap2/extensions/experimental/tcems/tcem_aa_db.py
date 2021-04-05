
import luigi
import os
from os.path import join, dirname, isfile
import sqlite3
import gzip
from Bio import SeqIO
from multiprocessing import Pool
from bloom_filter import BloomFilter

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


class TcemNrAaDbChunk(CapDbTask):
    chunk_index = luigi.IntParameter()
    total_chunks = luigi.IntParameter()
    fasta = luigi.Parameter()
    MODULE_VERSION = 'v0.2.0'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.config = PipelineConfig(self.config_filename)
        self.db_dir = self.config.db_dir
        self.bloom_size = 1000 * 1000 * 1000
        self.bloom_error = 0.001

    def tool_version(self):
        return self.version()

    def requires(self):
        return []

    @classmethod
    def _module_name(cls):
        return 'tcem_nr_aa_db_chunk'

    @classmethod
    def dependencies(cls):
        return ['2021-02-13']

    @property
    def tcem_index(self):
        fname = f'tcems_aa_kmers.{self.chunk_index}_of_{self.total_chunks}.csv.gz'
        return join(self.db_dir, 'ncbi_nr_fasta', fname)

    def output(self):
        tcem_index = luigi.LocalTarget(self.tcem_index)
        tcem_index.makedirs()
        return {
            'tcem_index': tcem_index,
            'flag': luigi.LocalTarget(self.tcem_index + '.flag'),
        }

    def run(self):
        logger.info(f'Running in database mode: {self.config.db_mode}')
        if self.config.db_mode == PipelineConfig.DB_MODE_BUILD:
            self.build_db()

    def build_db(self):
        if isfile(self.tcem_index):
            os.remove(self.tcem_index)
        logger.info(f'<chunk {self.chunk_index}> Building TCEM database from {self.fasta}')
        bloom = BloomFilter(max_elements=self.bloom_size, error_rate=self.bloom_error)
        seq_counter = 0
        with Pool(self.cores) as pool, gzip.open(self.fasta, 'rt') as f, gzip.open(self.tcem_index, 'wt') as o:
            try:
                seqs = (
                    seq for i, seq in enumerate(SeqIO.parse(f, 'fasta'))
                    if (i % self.total_chunks) == self.chunk_index
                )
                for taxa_kmer_set in pool.imap_unordered(process_one_seq, seqs, chunksize=1000):
                    seq_counter += 1
                    if seq_counter % (10 * 1000) == 0:
                        logger.info(f'<chunk {self.chunk_index}> Processing seq: {seq_counter}')
                    for pair in taxa_kmer_set:
                        pair_str = f'{pair[0]},{pair[1]}'
                        if pair_str in bloom:
                            continue
                        bloom.add(pair_str)
                        print(pair_str, file=o)
            except KeyboardInterrupt:
                pool.terminate()
                raise
        open(self.tcem_index + '.flag', 'w').close()


class TcemSortedNrAaDbChunk(CapDbTask):
    chunk_index = luigi.IntParameter()
    total_chunks = luigi.IntParameter()
    fasta = luigi.Parameter()
    MODULE_VERSION = 'v0.2.0'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.config = PipelineConfig(self.config_filename)
        self.db_dir = self.config.db_dir
        self.bloom_size = 1000 * 1000 * 1000
        self.bloom_error = 0.001
        self.chunk = TcemNrAaDbChunk.from_cap_db_task(
            self,
            chunk_index=self.chunk_index,
            total_chunks=self.total_chunks,
            fasta=self.fasta,
        )

    def tool_version(self):
        return self.version()

    def requires(self):
        return self.chunk

    @classmethod
    def _module_name(cls):
        return 'tcem_sorted_nr_aa_db_chunk'

    @classmethod
    def dependencies(cls):
        return ['2021-02-13']

    @property
    def tcem_index(self):
        fname = f'tcems_aa_kmers.{self.chunk_index}_of_{self.total_chunks}.sorted.csv.gz'
        return join(self.db_dir, 'ncbi_nr_fasta', fname)

    def output(self):
        tcem_index = luigi.LocalTarget(self.tcem_index)
        tcem_index.makedirs()
        return {
            'tcem_index': tcem_index,
            'flag': luigi.LocalTarget(self.tcem_index + '.flag'),
        }

    def run(self):
        logger.info(f'Running in database mode: {self.config.db_mode}')
        if self.config.db_mode == PipelineConfig.DB_MODE_BUILD:
            self.build_db()

    def build_db(self):
        if isfile(self.tcem_index):
            os.remove(self.tcem_index)
        logger.info(f'<chunk {self.chunk_index}> Sorting TCEM chunk {self.chunk_index} of {self.total_chunks}')
        self.run_cmd(f'gunzip -c {self.chunk.tcem_index} | sort | gzip > {self.tcem_index}')
        open(self.tcem_index + '.flag', 'w').close()



class TcemNrAaDbList(CapDbTask):
    MODULE_VERSION = 'v0.1.0'
    fasta = luigi.Parameter()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.config = PipelineConfig(self.config_filename)
        self.db_dir = self.config.db_dir
        self.total_chunks = 1000
        self.bloom_size =  1000 * 1000 * 1000
        self.bloom_error =  0.001
        self.chunks = []

    def tool_version(self):
        return self.version()

    def requires(self):
        if self.config.db_mode == PipelineConfig.DB_MODE_BUILD:
            for i in range(self.total_chunks):
                self.chunks.append(TcemSortedNrAaDbChunk.from_cap_db_task(
                    self,
                    chunk_index=i,
                    total_chunks=self.total_chunks,
                    fasta=self.fasta,
                ))
        return self.chunks

    @classmethod
    def _module_name(cls):
        return 'tcem_nr_aa_db_list'

    @classmethod
    def dependencies(cls):
        return ['2021-02-13']

    @property
    def tcem_index(self):
        return join(self.db_dir, 'ncbi_nr_fasta', 'tcems_aa_kmers.csv.gz')

    def output(self):
        tcem_index = luigi.LocalTarget(self.tcem_index)
        tcem_index.makedirs()
        return {
            'tcem_index': tcem_index,
            'flag': luigi.LocalTarget(self.tcem_index + '.flag'),
        }

    def run(self):
        logger.info(f'Running in database mode: {self.config.db_mode}')
        if self.config.db_mode == PipelineConfig.DB_MODE_BUILD:
            self.build_db()

    def build_db(self):
        if isfile(self.tcem_index):
            os.remove(self.tcem_index)
        logger.info(f'Building full TCEM list from {self.fasta}')
        chunk_cmd = ' '.join([f'<(gunzip -c {c.tcem_index})' for c in self.chunks])
        cmd = f'sort -m {chunk_cmd} | gzip > {self.tcem_index}'
        self.run_cmd(cmd)
        open(self.tcem_index + '.flag', 'w').close()


class TcemNrAaDb(CapDbTask):
    MODULE_VERSION = 'v0.1.0'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.config = PipelineConfig(self.config_filename)
        self.db_dir = self.config.db_dir
        self.fasta = join(self.db_dir, 'ncbi_nr_fasta', 'nr.gz')
        self.total_chunks = 1000
        self.bloom_size = 1000 * 1000 * 1000 * 10
        self.bloom_error = 0.001
        self.pair_buffer_size = 1000 * 1000
        self._tcem_list = None

    def tool_version(self):
        return self.version()

    def requires(self):
        if self.config.db_mode == PipelineConfig.DB_MODE_BUILD:
            return self.tcem_list
        return None

    @classmethod
    def _module_name(cls):
        return 'tcem_nr_aa_db'

    @classmethod
    def dependencies(cls):
        return ['2021-02-13']

    @property
    def tcem_list(self):
        self._tcem_list = TcemNrAaDbList.from_cap_db_task(
            self,
            fasta=self.fasta,
        )
        self._tcem_list.total_chunks = self.total_chunks
        self._tcem_list.bloom_size = self.bloom_size
        self._tcem_list.bloom_error = self.bloom_error
        return self._tcem_list

    @property
    def tcem_index(self):
        return join(self.db_dir, 'ncbi_nr_fasta', 'tcems_aa_kmers.sqlite')

    def output(self):
        tcem_index = luigi.LocalTarget(self.tcem_index)
        tcem_index.makedirs()
        return {
            'tcem_index': tcem_index,
            'flag': luigi.LocalTarget(self.tcem_index + '.flag'),
        }

    def run(self):
        logger.info(f'Running in database mode: {self.config.db_mode}')
        if self.config.db_mode == PipelineConfig.DB_MODE_BUILD:
            self.build_db()

    def add_chunk_to_db(self, chunk, main_c):
        pair_buffer = []

        def flush_pair_buffer():
            if not pair_buffer:
                return
            main_c.executemany(
                'INSERT INTO taxa_kmers VALUES (?,?)',
                (pair for pair in pair_buffer)
            )

        with gzip.open(chunk.tcem_index, 'rt') as chunk:
            for line in chunk:
                pair_str = line.strip()
                pair = pair_str.split(',')
                pair_buffer.append((pair[0], pair[1]))
                if len(pair_buffer) >= self.pair_buffer_size:
                    flush_pair_buffer()
                    pair_buffer = []
            flush_pair_buffer()

    def build_db(self):
        if isfile(self.tcem_index):
            os.remove(self.tcem_index)
        logger.info(f'Building TCEM database from {self.fasta}')
        main_conn = sqlite3.connect(self.tcem_index)
        main_c = main_conn.cursor()
        main_c.execute('''CREATE TABLE taxa_kmers (taxon text, kmer text)''')

        logger.info(f'Adding chunk: {self.tcem_list}')
        self.add_chunk_to_db(self.tcem_list, main_c)

        main_c.execute('''CREATE INDEX kmer_index ON taxa_kmers(kmer)''')
        main_conn.commit()
        main_conn.close()
        open(self.tcem_index + '.flag', 'w').close()
