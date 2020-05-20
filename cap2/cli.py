
import click

from .api import (
    run_db_stage,
    run_qc_stage,
    run_preprocessing_stage,
    run_short_read_stage,
)
from .sample import Sample


@click.group()
def main():
    pass


@main.group()
def run():
    pass


@run.command('db')
@click.option('-t', '--threads', default=1)
@click.argument('config', type=click.Path())
def cap_db(threads, config):
    """Run the CAP2 database pipeline.

    Config is a yaml file specifying these keys:
        out_dir: <directory where output should go>
        db_dir: <directory where databases are currently stored>
        db_mode: "build"|"download" (defaults to download)
    """
    run_db_stage(config, cores=threads)


@run.command('qc')
@click.argument('config', type=click.Path())
@click.argument('manifest', type=click.File('r'))
def cap_qc(config, manifest):
    """Run the CAP2 qc pipeline.

    Config is a yaml file specifying these keys:
        out_dir: <directory where output should go>
        db_dir: <directory where databases are currently stored>

    Manifest is a three column file with rows of form:
    <sample name>   <read1 filepath>    <read2 filepath>
    """
    samples = Sample.samples_from_manifest(manifest)
    run_qc_stage(samples, config)


@run.command('pre')
@click.argument('config', type=click.Path())
@click.argument('manifest', type=click.File('r'))
def cap_pre(config, manifest):
    """Run the CAP2 preprocessing pipeline.

    Config is a yaml file specifying these keys:
        out_dir: <directory where output should go>
        db_dir: <directory where databases are currently stored>

    Manifest is a three column file with rows of form:
    <sample name>   <read1 filepath>    <read2 filepath>
    """
    samples = Sample.samples_from_manifest(manifest)
    run_preprocessing_stage(samples, config)


@run.command('short-reads')
@click.argument('config', type=click.Path())
@click.argument('manifest', type=click.File('r'))
def cap_short_read(config, manifest):
    """Run the CAP2 short read pipeline and preprocessing pipeline.

    Config is a yaml file specifying these keys:
        out_dir: <directory where output should go>
        db_dir: <directory where databases are currently stored>

    Manifest is a three column file with rows of form:
    <sample name>   <read1 filepath>    <read2 filepath>
    """
    samples = Sample.samples_from_manifest(manifest)
    run_short_read_stage(samples, config)


if __name__ == '__main__':
    main()
