
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


@covid_cli.command('fast')
@click.option('-c', '--config', type=click.Path(), default='', envvar='CAP2_CONFIG')
@click.option('--scheduler-url', default=None, envvar='CAP2_LUIGI_SCHEDULER_URL')
@click.option('-w', '--workers', default=1)
@click.option('-t', '--threads', default=1)
@click.option('--timelimit', default=0, help='Stop adding jobs after N hours')
@click.option('--endpoint', default='https://pangea.gimmebio.com')
@click.option('--s3-endpoint', default='https://s3.wasabisys.com')
@click.option('--s3-profile', default='default', envvar='CAP2_PANGEA_S3_PROFILE')
@click.option('-e', '--email', envvar='PANGEA_USER')
@click.option('-p', '--password', envvar='PANGEA_PASS')
@click.argument('org_name')
@click.argument('grp_name')
@click.argument('bucket_name')
def covid_fast_cli(config, scheduler_url, workers, threads, timelimit,
                   endpoint, s3_endpoint, s3_profile, email, password,
                   org_name, grp_name, bucket_name):
    set_config(email, password, org_name, grp_name, bucket_name, s3_endpoint, s3_profile)
    group = PangeaGroup(grp_name, email, password, endpoint, org_name)
    start_time = time.time()
    index, completed = -1, set()
    samples = list(group.pangea_samples(randomize=True))
    while len(completed) < len(samples):
        if timelimit and (time.time() - start_time) > (60 * 60 * timelimit):
            break
        index = (index + 1) % len(samples)
        sample = samples[index]
        if index in completed:
            continue
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
        completed.add(index)
