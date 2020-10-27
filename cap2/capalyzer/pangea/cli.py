
import click
import logging
from .pangea_file_source import PangeaFileSource
from .pangea_file_uploader import PangeaFileUploader
from ..table_builder import CAPTableBuilder
from .utils import get_pangea_group



@click.group('pangea')
def capalyzer_pangea_cli():
    pass


@capalyzer_pangea_cli.command('make-tables')
@click.option('-l', '--log-level', default=30)
@click.option('--endpoint', default='https://pangea.gimmebio.com')
@click.option('-e', '--email', envvar='PANGEA_USER')
@click.option('-p', '--password', envvar='PANGEA_PASS')
@click.argument('org_name')
@click.argument('grp_name')
def cli_make_tables(log_level, endpoint, email, password, org_name, grp_name):
    logging.basicConfig(
        level=log_level,
        format='%(levelname)s:%(message)s',
    )
    pangea_group = get_pangea_group(org_name, grp_name, email=email, password=password, endpoint=endpoint)
    file_source, file_uploader = PangeaFileSource(pangea_group), PangeaFileUploader(pangea_group)
    table_builder = CAPTableBuilder(f'{org_name}::{grp_name}', file_source)
    read_counts = table_builder.taxa_read_counts()
    file_uploader.upload_pandas(read_counts, 'kraken2_taxa', 'read_counts')
