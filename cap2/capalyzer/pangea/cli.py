
import click
import logging
from .utils import get_pangea_group

from pangea_api.contrib.tagging import Tag
from pangea_api import (
    Knex,
    User,
)
from .api import process_group

logger = logging.getLogger(__name__)  # Same name as calling module


@click.group('pangea')
def capalyzer_pangea_cli():
    pass


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
def cli_process_tag(log_level, endpoint, email, password, tag_name, strict):
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
        process_group(pangea_group, strict=strict)
