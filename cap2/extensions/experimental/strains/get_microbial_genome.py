
import os
import fnmatch
import subprocess
from glob import glob
from ftplib import FTP
from random import shuffle
from .utils import clean_microbe_name


NCBI_FTP = 'ftp.ncbi.nlm.nih.gov'


def get_ftp_args(file_type):
    if file_type == 'fasta':
        return "--exclude='*cds_from*' --exclude='*rna_from*' --include='*genomic.fna.gz' --exclude='*'"
    # elif file_type == "genbank":
    #     return "--include='*genomic.gbff.gz' --exclude='*'"
    # elif file_type == "gff":
    #     return "--include='*genomic.gff.gz' --exclude='*'"
    # elif file_type == "feature_table":
    #     return "--include='*feature_table.txt.gz' --exclude='*'"
    raise ValueError(f'file_type `{file_type}` is not valid')


def download_genomes(microbe_name,
                    database='refseq', outdir='.',
                    file_type='fasta', ftp_endpoint=NCBI_FTP):
    ftp = FTP(ftp_endpoint)
    ftp_args = get_ftp_args(file_type)
    ftp.login()
    ftp.cwd(f'genomes/{database}/bacteria')
    genome_match = fnmatch.filter(ftp.nlst(), microbe_name)
    if len(genome_match) == 0:
        raise ValueError(f'microbe `{microbe_name}` not found.')
    url = f'rsync://{ftp_endpoint}/genomes/{database}/bacteria/{microbe_name}/latest_assembly_versions/*/'
    cmd = (
        'rsync '
        f'-Lrtv {ftp_args} '
        f'{url} '
        f'{outdir} '
    )
    subprocess.check_call(cmd, shell=True)


def remove_excess_files(new_files, max_n_genomes=100):
    if len(new_files) <= max_n_genomes:
        return new_files
    shuffle(new_files)
    to_remove = new_files[max_n_genomes:]
    for filepath in to_remove:
        os.remove(filepath)
    kept_files = new_files[:max_n_genomes]
    return kept_files


def get_microbial_genome(microbe_name,
                         max_n_genomes=100, database='refseq', outdir='.',
                         file_type='fasta', ftp_endpoint=NCBI_FTP):
    microbe_name = clean_microbe_name(microbe_name)
    download_genomes(
        microbe_name,
        database=database, outdir=outdir, file_type=file_type, ftp_endpoint=ftp_endpoint
    )
    new_files = list(glob(f'{outdir}/*genomic.fna.gz'))
    kept_files = remove_excess_files(new_files, max_n_genomes=max_n_genomes)
    return kept_files
