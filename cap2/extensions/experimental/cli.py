
import click


@click.group('experimental')
def experimental_cli():
    pass


try:
    from .covid.cli import covid_cli
    experimental_cli.add_command(covid_cli)
except ImportError:
    click.echo('Failed to import extension module: "covid"', err=True)

try:
    from .preclassify.cli import preclassify_cli
    experimental_cli.add_command(preclassify_cli)
except ImportError:
    click.echo('Failed to import extension module: "preclassify"', err=True)

try:
    from .strains.cli import strain_cli
    experimental_cli.add_command(strain_cli)
except ImportError:
    click.echo('Failed to import extension module: "strains"', err=True)
