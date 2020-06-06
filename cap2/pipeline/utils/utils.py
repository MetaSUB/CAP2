
from Bio import SeqIO


def estimate_read_length(fastq_filename):
    """Return an estimated average read length for the specified path."""
    n, total_len = 0, 0
    for i, rec in enumerate(SeqIO.parse(fastq_filename, 'fastq')):
        if i > 1000:
            break
        n += 1
        total_len += len(rec.seq)
    return total_len / n
