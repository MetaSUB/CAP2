
import subprocess as sp
from glob import glob


def run_cmd(cmd):
    sp.check_call(cmd, shell=True)


def sra_to_fastqs(sample_name, sra_filepath, exc='fasterq-dump', dirpath='.'):
    """Return a list of gzipped fastq files."""
    sra_dump_cmd = f'{exc} --split-files -O {dirpath} -o {sample_name} {sra_filepath}'
    run_cmd(sra_dump_cmd)
    unzipped_files = " ".join(glob(f'{dirpath}/{sample_name}_*.fastq'))
    gzip_cmd = f'gzip {unzipped_files}'
    run_cmd(gzip_cmd)
    gzipped_files = glob(f'{dirpath}/{sample_name}_*.fastq.gz')
    return gzipped_files
