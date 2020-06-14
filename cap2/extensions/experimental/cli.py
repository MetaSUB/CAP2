
import click

from covid.cli import covid_cli


@click.group('experimental')
def experimental_cli():
    pass


experimental_cli.add_command(covid_cli)
