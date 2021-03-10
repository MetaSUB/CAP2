
import subprocess as sp
from glob import glob
from os.path import join, basename, dirname
from os import makedirs, rename, rmdir


def run_cmd(cmd):
    sp.check_call(cmd, shell=True)


def sra_to_fastqs(sample_name, sra_filepath, exc='fastq-dump', dirpath='.'):
    """Return a list of gzipped fastq files."""
    tmp_dir = join(dirpath, f'temp_sra_{sample_name}')
    makedirs(tmp_dir, exist_ok=True)
    sra_dump_cmd = f'{exc} --split-files --skip-technical --clip --gzip -O {tmp_dir} {sra_filepath}'
    run_cmd(sra_dump_cmd)
    tmp_gzipped_files = glob(f'{tmp_dir}/*.fastq.gz')
    gzipped_files = []
    for filename in tmp_gzipped_files:
        base = basename(filename)
        if '_1.fastq.gz' in base:
            new_base = f'{sample_name}.R1.fq.gz'
        if '_2.fastq.gz' in base:
            new_base = f'{sample_name}.R2.fq.gz'
        new_filename = join(dirpath, new_base)
        rename(filename, new_filename)
        gzipped_files.append(new_filename)
    rmdir(tmp_dir)
    return gzipped_files
