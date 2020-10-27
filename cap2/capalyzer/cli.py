
import click

from .pangea.cli import capalyzer_pangea_cli


@click.group('capalyzer')
def capalyzer_cli():
    pass


capalyzer_cli.add_command(capalyzer_pangea_cli)
