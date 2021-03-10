
import gzip
from random import random
from Bio import SeqIO


def estimate_read_length(fastq_filename):
    """Return an estimated average read length for the specified path."""
    n, total_len = 0, 0
    with gzip.open(fastq_filename, "rt") as handle:
        for i, rec in enumerate(SeqIO.parse(handle, 'fastq')):
            if i > 1000:
                break
            n += 1
            total_len += len(rec.seq)
    return total_len // n


def stats_one_fastq(fastq, dropout):
    read_count = 0
    gc_count, n_count, base_count = 0, 0, 0
    seq_length = 0
    with gzip.open(fastq) as f:
        for i, line in enumerate(f):
            if i % 4 != 1:
                continue
            seq = line.strip()
            read_count += 1
            seq_length += len(seq)
            if random() > dropout:
                continue
            for base in seq:
                if base in b'GCgc':
                    gc_count += 1
                elif base not in b'ATUatu':
                    n_count += 1
                base_count += 1
    gc_frac = gc_count / (base_count + 1)
    n_frac = n_count / (base_count + 1)
    seq_length /= (read_count + 1)
    return {
        'read_count': read_count,
        'gc_fraction': gc_frac,
        'n_fraction': n_frac,
        'mean_seq_length': seq_length,
    }
