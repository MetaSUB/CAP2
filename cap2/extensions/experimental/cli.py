
import click

from .covid.cli import covid_cli
from .preclassify.cli import preclassify_cli
from .strains.cli import strain_cli


@click.group('experimental')
def experimental_cli():
    pass


experimental_cli.add_command(covid_cli)
experimental_cli.add_command(preclassify_cli)
experimental_cli.add_command(strain_cli)
