
import click

from .pangea.cli import pangea
from .api import (
    run_db_stage,
    run_stage,
)
from .sample import Sample
from .constants import (
    DEFAULT_STAGE,
    STAGES,
)
from .pipeline.full_pipeline import FullPipeline


@click.group()
def main():
    pass


main.add_command(pangea)


@main.command()
@click.option('-t/-n', '--tree/--number', default=False)
@click.option('-h/-n', '--hash/--number', default=False)
def version(tree, hash):
    if tree:
        click.echo(FullPipeline.version_tree())
    elif hash:
        click.echo(FullPipeline.version_hash())
    else:
        click.echo(FullPipeline.version())


@main.group()
def run():
    pass


@run.command('db')
@click.option('-t', '--threads', default=1)
@click.option('-c', '--config', type=click.Path(), default='')
def cap_db(threads, config):
    """Run the CAP2 database pipeline.

    Config is a yaml file specifying these keys:
        out_dir: <directory where output should go>
        db_dir: <directory where databases are currently stored>
        db_mode: "build"|"download" (defaults to download)
    """
    run_db_stage(config_path=config, cores=threads)


@run.command('pipeline')
@click.option('-t', '--threads', default=1)
@click.option('-c', '--config', type=click.Path(), default='')
@click.option('-s', '--stage', type=click.Choice(STAGES.keys()), default=DEFAULT_STAGE)
@click.argument('manifest', type=click.File('r'))
def cap_pipeline(threads, config, stage, manifest):
    """Run  a stage of the CAP2 pipeline.

    Manifest is a three column file with rows of form:
    <sample name>   <read1 filepath>    <read2 filepath>
    """
    samples = Sample.samples_from_manifest(manifest)
    run_stage(samples, stage, config_path=config, cores=threads)


if __name__ == '__main__':
    main()
