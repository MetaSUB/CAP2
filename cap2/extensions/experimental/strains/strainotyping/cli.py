
import click

from .io import write_graph_to_filepath
from .api import merge_filter_graphs_from_filepath


@click.group('strainotype')
def strainotype_cli():
    pass


@strainotype_cli.command('merge')
@click.option('-m', '--min-weight', default=2)
@click.option('-o', '--outfile', type=click.File('w'), default='-')
@click.argument('filepaths', nargs=-1)
def merge_graphs_cli(min_weight, outfile, filepaths):
    G = merge_filter_graphs_from_filepath(filepaths, min_weight=min_weight)
    write_graph_to_filepath(G, outfile)
