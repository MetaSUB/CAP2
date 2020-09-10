
import click
import luigi
import time

from .fast_detect import Kraken2FastDetectCovid
from ....pangea.cli import set_config
from ....pangea.api import wrap_task
from ....pangea.pangea_sample import PangeaGroup
from ....pipeline.preprocessing import BaseReads


@click.group('covid')
def covid_cli():
    pass


@run.command('samples')
@click.option('-c', '--config', type=click.Path(), default='', envvar='CAP2_CONFIG')
@click.option('--clean-reads/--all-reads', default=False)
@click.option('--upload/--no-upload', default=True)
@click.option('--download-only/--run', default=False)
@click.option('--scheduler-url', default=None, envvar='CAP2_LUIGI_SCHEDULER_URL')
@click.option('--max-attempts', default=2)
@click.option('-b', '--batch-size', default=10, help='Number of samples to process in parallel')
@click.option('-w', '--workers', default=1)
@click.option('-t', '--threads', default=1)
@click.option('--timelimit', default=0, help='Stop adding jobs after N hours')
@click.option('--endpoint', default='https://pangea.gimmebio.com')
@click.option('-e', '--email', envvar='PANGEA_USER')
@click.option('-p', '--password', envvar='PANGEA_PASS')
@click.option('-s', '--stage', default='reads')
@click.option('--random-seed', type=int, default=None)
@click.argument('org_name')
@click.argument('grp_name')
def cli_run_samples(config, clean_reads, upload, download_only, scheduler_url,
                    max_attempts,
                    batch_size, workers, threads, timelimit,
                    endpoint, email, password, stage, random_seed,
                    org_name, grp_name):
    set_config(endpoint, email, password, org_name, grp_name)
    group = PangeaGroup(grp_name, email, password, endpoint, org_name)
    start_time, completed = time.time(), []
    samples = [
        samp for samp in group.pangea_samples(randomize=True, seed=random_seed)
        if not clean_reads or samp.has_clean_reads()
    ]
    click.echo(f'Processing {len(samples)} samples', err=True)
    for chunk in chunks(samples, batch_size):
        click.echo(f'Completed processing {len(completed)} samples', err=True)
        if timelimit and (time.time() - start_time) > (60 * 60 * timelimit):
            click.echo(f'Timelimit reached. Stopping.', err=True)
            break
        reads = wrap_task(
            sample, BaseReads,
            upload=False, config_path=config, cores=threads, requires_reads=True
        )
        fast_detect = wrap_task(
            sample, Kraken2FastDetectCovid, config_path=config, cores=threads
        )
        fast_detect.wrapped.reads = reads
        tasks = [fast_detect]
        if not scheduler_url:
            luigi.build(tasks, local_scheduler=True, workers=workers)
        else:
            luigi.build(
                tasks, scheduler_url=scheduler_url, workers=workers
            )
        completed += chunk