
import click
import subprocess as sp
from glob import glob
from os.path import basename, isfile
from os import makedirs, remove
from random import shuffle
from pangea_api import (
    Knex,
    User,
    Organization,
)
from pangea_api.contrib.tagging import Tag
from requests.exceptions import HTTPError

MASH_MODULE_NAME = 'megagenome::v1::mash'


def run_cmd(cmd):
    click.echo(f'RUNNING "{cmd}"', err=True)
    sp.check_call(cmd, shell=True)


def sra_to_fastqs(sample_name, sra_filepath, exc='fasterq-dump', dirpath='.'):
    """Return a list of gzipped fastq files."""
    sra_dump_cmd = f'yes n | {exc} --split-files -O {dirpath} -o {sample_name} {sra_filepath}'
    run_cmd(sra_dump_cmd)
    unzipped_files = " ".join(glob(f'{dirpath}/{sample_name}_*.fastq'))
    gzip_cmd = f'yes n | gzip {unzipped_files}'
    run_cmd(gzip_cmd)
    gzipped_files = glob(f'{dirpath}/{sample_name}_*.fastq.gz')
    gzipped_files = sorted(gzipped_files)
    return gzipped_files


def run_mash(reads, mashfile, exc='mash'):
    cmd = (
        f'{exc} '
        f'sketch -s {10 * 1000} '
        f'-o {mashfile[:-4]} '
        f'{reads}'
    )
    run_cmd(cmd)


def upload_mash(sample, mash_path):
    ar = sample.analysis_result(MASH_MODULE_NAME).create()
    arf = ar.field('10K').create()
    arf.upload_file(mash_path)


def get_fastq_sra(sample, sra, outdir='.', fasterq_exc='fasterq-dump'):
    sra_path = f'{outdir}/{sample.name}.sra'
    if isfile(sra_path):
        remove(sra_path)
    sra.download_file(filename=sra_path)
    fastqs = sra_to_fastqs(sample.name, sra_path, exc=fasterq_exc, dirpath=outdir)
    return fastqs[0]


def get_fastq(sample, ar, outdir='.', fasterq_exc='fasterq-dump'):
    try:
        sra = ar.field('sra_run').get()
        return get_fastq_sra(sample, sra, outdir=outdir, fasterq_exc=fasterq_exc)
    except HTTPError:
        r1 = ar.field('read_1').get()
        fq_path = f'{outdir}/{sample.name}.R1.fq.gz'
        if isfile(fq_path):
            remove(fq_path)
        r1.download_file(filename=fq_path)
        return fq_path
        
    
def _process_sample(sample, outdir='.', fasterq_exc='fasterq-dump', mash_exc='mash'):
    mash_path = f'{outdir}/{sample.name}.{MASH_MODULE_NAME}.v0-0-1.10K.msh'
    ar = sample.analysis_result('raw::raw_reads').get()
    fastq = get_fastq(sample, ar, outdir=outdir, fasterq_exc=fasterq_exc)
    run_mash(fastq, mash_path, exc=mash_exc)
    upload_mash(sample, mash_path)


def process_sample(sample, outdir='.', fasterq_exc='fasterq-dump', mash_exc='mash'):
    mash_path = f'{outdir}/{sample.name}.{MASH_MODULE_NAME}.v0-0-1.10K.msh'
    if isfile(mash_path):
        return
    try:
        sample.analysis_result(MASH_MODULE_NAME).get()
        return
    except Exception as e:
        if not isinstance(e, HTTPError):
            raise
    return _process_sample(sample, outdir=outdir, fasterq_exc=fasterq_exc, mash_exc=mash_exc)


@click.group()
def main():
    pass


@main.command('sample')
@click.option('-e', '--email')
@click.option('-p', '--password')
@click.option('-o', '--outdir')
@click.option('--fasterq-exc', default='fasterq-dump')
@click.option('--mash-exc', default='mash')
@click.argument('sample_name')
def run_sample(email, password, outdir, fasterq_exc, mash_exc, sample_name):
    knex = Knex()
    User(knex, email, password).login()
    org = Organization(knex, 'MegaGenome').get()
    grp = org.sample_group('SRA').get()
    sample = grp.sample(sample_name).get()
    makedirs(outdir, exist_ok=True)
    process_sample(sample, outdir=outdir, fasterq_exc=fasterq_exc, mash_exc=mash_exc)


@main.command('all')
@click.option('-e', '--email')
@click.option('-p', '--password')
@click.option('-o', '--outdir')
@click.option('--fasterq-exc', default='fasterq-dump')
@click.option('--mash-exc', default='mash')
def run_sample(email, password, outdir, fasterq_exc, mash_exc):
    knex = Knex()
    User(knex, email, password).login()
    tag = Tag(knex, 'MegaGenome').get()
    samples = list(tag.get_samples())
    shuffle(samples)
    for sample in samples:
        try:
            click.echo(sample.name, err=True)
            makedirs(outdir, exist_ok=True)
            process_sample(sample, outdir=outdir, fasterq_exc=fasterq_exc, mash_exc=mash_exc)
        except Exception as e:
            if not isinstance(e, HTTPError):
                raise
            click.echo(f'failed {sample.name}', err=True)


if __name__ == '__main__':
    main()
