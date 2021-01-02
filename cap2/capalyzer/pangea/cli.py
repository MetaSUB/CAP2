
import click
import logging
from .pangea_file_source import PangeaFileSource
from .pangea_file_uploader import PangeaFileUploader, PangeaFileAlreadyExistsError
from ..table_builder import CAPTableBuilder
from .utils import get_pangea_group

from pangea_api.contrib.tagging import Tag
from pangea_api import (
    Knex,
    User,
)

logger = logging.getLogger(__name__)  # Same name as calling module


@click.group('pangea')
def capalyzer_pangea_cli():
    pass


def process_group(pangea_group):
    module_counts = pangea_group.get_module_counts()
    file_source, file_uploader = PangeaFileSource(pangea_group), PangeaFileUploader(pangea_group)
    table_builder = CAPTableBuilder(f'{pangea_group.org.name}::{pangea_group.name}', file_source)

    try:
        result_name = 'kraken2_taxa'
        module_count = module_counts.get('cap2::kraken2', 0)
        up_to_date = file_uploader.result_is_up_to_date(result_name, module_count)
        if module_count > 0 and not up_to_date:
            read_counts, n_samples = table_builder.taxa_read_counts()
            file_uploader.upload_pandas(read_counts, result_name, 'read_counts', n_samples=n_samples)
            logger.info(f'made kraken2 taxa read count table for group {pangea_group.name}')
        elif up_to_date:
            logger.info(f'kraken2 taxa read count table already exists for group {pangea_group.name}')
        else:
            logger.info(f'source modules not available for kraken2 taxa read count table for group {pangea_group.name}')
    except:
        logger.error(f'failed to make kraken2 taxa read count table for group {pangea_group.name}')
        if strict:
            raise

    try:
        result_name = 'kraken2_covid_fast_detect'
        module_count = module_counts.get('cap2::experimental::covid19_fast_detect', 0)
        up_to_date = file_uploader.result_is_up_to_date(result_name, module_count)
        if module_count > 0 and not up_to_date:
            read_counts, n_samples = table_builder.covid_fast_detect_read_counts()
            file_uploader.upload_pandas(read_counts, result_name, 'read_counts', n_samples=n_samples)
            logger.info(f'made kraken2 covid fast detect read count table for group {pangea_group.name}')
        elif up_to_date:
            logger.info(f'kraken2 covid fast detect read count table already exists for group {pangea_group.name}')
        else:
            logger.info(f'source modules not available for kraken2 covid fast detect read count table for group {pangea_group.name}')
    except:
        logger.error(f'failed to make kraken2 covid fast detect read count table for group {pangea_group.name}')
        if strict:
            raise


@capalyzer_pangea_cli.command('make-tables')
@click.option('-l', '--log-level', default=20)
@click.option('--endpoint', default='https://pangea.gimmebio.com')
@click.option('-e', '--email', envvar='PANGEA_USER')
@click.option('-p', '--password', envvar='PANGEA_PASS')
@click.argument('org_name')
@click.argument('grp_name')
def cli_make_tables(log_level, endpoint, email, password, org_name, grp_name):
    logging.basicConfig(
        level=log_level,
        format='%(levelname)s: %(message)s',
    )
    pangea_group = get_pangea_group(org_name, grp_name, email=email, password=password, endpoint=endpoint)
    process_group(pangea_group)


@capalyzer_pangea_cli.command('tag')
@click.option('-l', '--log-level', default=20)
@click.option('--endpoint', default='https://pangea.gimmebio.com')
@click.option('-e', '--email', envvar='PANGEA_USER')
@click.option('-p', '--password', envvar='PANGEA_PASS')
@click.option('-t', '--tag-name', default='CAPalyzer')
@click.option('--strict/--permissive', default=False)
def cli_make_tables(log_level, endpoint, email, password, tag_name, strict):
    logging.basicConfig(
        level=log_level,
        format='%(levelname)s: %(message)s',
    )
    knex = Knex(endpoint)
    if email and password:
        User(knex, email, password).login()
    tag = Tag(knex, tag_name).get()
    for pangea_group in tag.get_sample_groups():
        logger.info(pangea_group)
        process_group(pangea_group)
