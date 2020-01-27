
import click

from .api import run_short_read_stage
from .sample import Sample


@click.group()
def main():
    pass


@click.command('short-reads')
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
