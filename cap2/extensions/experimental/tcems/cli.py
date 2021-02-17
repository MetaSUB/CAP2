
import click
import luigi
import time
import logging


from .tcem_repertoire import TcemRepertoire

from ....pangea.cli import set_config
from ....pangea.api import (
    wrap_task,
    recursively_wrap_task,
)
from ....pangea.pangea_sample import PangeaGroup, PangeaTag
from ....pipeline.preprocessing import (
    BaseReads,
)
from ....constants import DATA_TYPES
from ....utils import chunks
from ....setup_logging import *

logger = logging.getLogger('tcems')


@click.group('tcems')
def tcems_cli():
    pass


@tcems_cli.group('run')
def run_cli():
    pass


def get_task_list_for_sample(sample, stage, config, **kwargs):
    base_reads = wrap_task(
        sample, BaseReads, config_path=config, requires_reads=True, **kwargs
    )
    base_reads.upload_allowed = False
    tcem_rep = recursively_wrap_task(
        sample,
        TcemRepertoire,
        config_path=config_path,
        module_substitute_tasks={BaseReads: base_reads},
        **kwargs,
    )
    tasks = [tcem_rep]
    return tasks


def _process_one_sample_chunk(chunk, scheduler_url, workers,
                              stage, config, clean_reads, **kwargs):
    tasks = []
    for sample in chunk:
        tasks += get_task_list_for_sample(
            sample, stage, config, **kwargs
        )
    if not scheduler_url:
        luigi.build(tasks, local_scheduler=True, workers=workers)
    else:
        luigi.build(tasks, scheduler_url=scheduler_url, workers=workers)
    return chunk


def _process_samples_in_chunks(samples, scheduler_url, batch_size, timelimit, workers,
                               stage, config, clean_reads, **kwargs):
    start_time, completed = time.time(), []
    logger.info(f'Processing {len(samples)} samples')
    for chunk in chunks(samples, batch_size):
        logger.info(f'Completed processing {len(completed)} samples')
        if timelimit and (time.time() - start_time) > (60 * 60 * timelimit):
            logger.info(f'Timelimit reached. Stopping.')
            return completed
        completed += _process_one_sample_chunk(
            chunk, scheduler_url, workers,
            stage, config, clean_reads, **kwargs
        )
    return completed


@run_cli.command('samples')
@click.option('-c', '--config', type=click.Path(), default='', envvar='CAP2_CONFIG')
@click.option('-l', '--log-level', default=30)
@click.option('--clean-reads/--all-reads', default=False)
@click.option('--upload/--no-upload', default=True)
@click.option('--download-only/--run', default=False)
@click.option('--scheduler-url', default=None, envvar='CAP2_LUIGI_SCHEDULER_URL')
@click.option('--max-attempts', default=2)
@click.option('-k', '--data-kind', default='short_read', type=click.Choice(DATA_TYPES))
@click.option('-b', '--batch-size', default=10, help='Number of samples to process in parallel')
@click.option('-w', '--workers', default=1)
@click.option('-t', '--threads', default=1)
@click.option('--timelimit', default=0, help='Stop adding jobs after N hours')
@click.option('--endpoint', default='https://pangea.gimmebio.com')
@click.option('-e', '--email', envvar='PANGEA_USER')
@click.option('-p', '--password', envvar='PANGEA_PASS')
@click.option('-s', '--stage', type=click.Choice(['all']))
@click.option('--random-seed', type=int, default=None)
@click.argument('org_name')
@click.argument('grp_name')
def cli_run_samples(config, log_level, clean_reads, upload, download_only, scheduler_url,
                    max_attempts, data_kind,
                    batch_size, workers, threads, timelimit,
                    endpoint, email, password, stage, random_seed,
                    org_name, grp_name):
    set_config(endpoint, email, password, org_name, grp_name,
              upload_allowed=upload, download_only=download_only, name_is_uuid=True, data_kind=data_kind)
    group = PangeaGroup(grp_name, email, password, endpoint, org_name)
    samples = [
        samp for samp in group.pangea_samples(randomize=True, seed=random_seed, kind=data_kind)
        if not clean_reads or samp.has_clean_reads()
    ]
    completed = _process_samples_in_chunks(
        samples, scheduler_url, batch_size, timelimit, workers,
        stage, config, clean_reads, cores=threads,
    )
