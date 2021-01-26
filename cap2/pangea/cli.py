
import click
import luigi
import time

from os import environ

from .api import get_task_list_for_sample
from .load_task import PangeaLoadTask, PangeaGroupLoadTask
from .pangea_sample import PangeaSample, PangeaGroup, PangeaTag
from ..pipeline.preprocessing import FastQC, MultiQC
from ..utils import chunks


@click.group()
def pangea():
    pass


@pangea.group()
def run():
    pass


def set_config(endpoint, email, password, org_name, grp_name, name_is_uuid=False):
    luigi.configuration.get_config().set('pangea', 'pangea_endpoint', endpoint)
    luigi.configuration.get_config().set('pangea', 'user', email)
    luigi.configuration.get_config().set('pangea', 'password', password)
    luigi.configuration.get_config().set('pangea', 'org_name', org_name if org_name else '')
    luigi.configuration.get_config().set('pangea', 'grp_name', grp_name if grp_name else '')
    luigi.configuration.get_config().set('pangea', 'name_is_uuid', 'name_is_uuid' if name_is_uuid else '')


@run.command('group')
@click.option('--upload/--no-upload', default=True)
@click.option('--scheduler-host', default=None)
@click.option('--scheduler-port', default=8082)
@click.option('--endpoint', default='https://pangea.gimmebio.com')
@click.option('--s3-endpoint', default='https://s3.wasabisys.com')
@click.option('--s3-profile', default='default')
@click.option('-e', '--email', default=environ.get('PANGEA_USER', None))
@click.option('-p', '--password', default=environ.get('PANGEA_PASS', None))
@click.option('-w', '--workers', default=1)
@click.argument('org_name')
@click.argument('grp_name')
@click.argument('bucket_name')
def cli_run_group(upload,
                  scheduler_host, scheduler_port,
                  endpoint, s3_endpoint, s3_profile, email, password, workers,
                  org_name, grp_name, bucket_name):
    set_config(endpoint, email, password, org_name, grp_name, bucket_name, s3_endpoint, s3_profile)
    group = PangeaGroup(grp_name, email, password, endpoint, org_name)
    tasks = []

    mqc_task = PangeaGroupLoadTask.from_samples(grp_name, group.cap_samples())
    mqc_task.wrapped_module = MultiQC
    mqc_task.module_requires_reads[FastQC] = True
    tasks.append(mqc_task)

    if not scheduler_host:
        luigi.build(tasks, local_scheduler=True, workers=workers)
    else:
        luigi.build(
            tasks, scheduler_host=scheduler_host, scheduler_port=scheduler_port, workers=workers
        )


@run.command('sample')
@click.option('-c', '--config', type=click.Path(), default='', envvar='CAP2_CONFIG')
@click.option('--upload/--no-upload', default=True)
@click.option('--scheduler-url', default=None, envvar='CAP2_LUIGI_SCHEDULER_URL')
@click.option('-w', '--workers', default=1)
@click.option('-t', '--threads', default=1)
@click.option('--endpoint', default='https://pangea.gimmebio.com')
@click.option('--s3-endpoint', default='https://s3.wasabisys.com')
@click.option('--s3-profile', default='default', envvar='CAP2_PANGEA_S3_PROFILE')
@click.option('-e', '--email', envvar='PANGEA_USER')
@click.option('-p', '--password', envvar='PANGEA_PASS')
@click.option('-s', '--stage', default='reads')
@click.argument('org_name')
@click.argument('grp_name')
@click.argument('sample_name')
def cli_run_sample(config, upload, scheduler_url, workers, threads,
                   endpoint, s3_endpoint, s3_profile, email, password,
                   stage,
                   org_name, grp_name, sample_name):
    sample = PangeaSample(sample_name, email, password, endpoint, org_name, grp_name)
    set_config(endpoint, email, password, org_name, grp_name)
    tasks = get_task_list_for_sample(
        sample, stage, upload=upload, config_path=config, cores=threads
    )
    if not scheduler_url:
        luigi.build(tasks, local_scheduler=True, workers=workers)
    else:
        luigi.build(
            tasks, scheduler_url=scheduler_url, workers=workers
        )


def _process_one_sample_chunk(chunk,
                              scheduler_url, stage, upload, download_only,
                              config, threads, max_ram, clean_reads, workers):
    tasks = []
    for sample in chunk:
        tasks += get_task_list_for_sample(
            sample, stage,
            upload=upload, download_only=download_only,
            config_path=config, cores=threads, max_ram=max_ram, require_clean_reads=clean_reads
        )
    if not scheduler_url:
        luigi.build(tasks, local_scheduler=True, workers=workers)
    else:
        luigi.build(tasks, scheduler_url=scheduler_url, workers=workers)
    return chunk


def _process_samples_in_chunks(samples,
                               scheduler_url, stage, upload, download_only,
                               config, threads, max_ram, clean_reads, workers,
                               batch_size, timelimit):
    start_time, completed = time.time(), []
    click.echo(f'Processing {len(samples)} samples', err=True)
    for chunk in chunks(samples, batch_size):
        click.echo(f'Completed processing {len(completed)} samples', err=True)
        if timelimit and (time.time() - start_time) > (60 * 60 * timelimit):
            click.echo(f'Timelimit reached. Stopping.', err=True)
            return completed
        completed += _process_one_sample_chunk(
            chunk,
            scheduler_url, stage, upload, download_only,
            config, threads, max_ram, clean_reads, workers
        )
    return completed


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
@click.option('-m', '--max-ram', default=0)
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
                    batch_size, workers, threads, max_ram, timelimit,
                    endpoint, email, password, stage, random_seed,
                    org_name, grp_name):
    set_config(endpoint, email, password, org_name, grp_name)
    group = PangeaGroup(grp_name, email, password, endpoint, org_name)
    samples = [
        samp for samp in group.pangea_samples(randomize=True, seed=random_seed)
        if not clean_reads or samp.has_clean_reads()
    ]
    completed = _process_samples_in_chunks(
        samples,
        scheduler_url, stage, upload, download_only,
        config, threads, max_ram, clean_reads, workers,
        batch_size, timelimit
    )


@run.command('tag')
@click.option('-c', '--config', type=click.Path(), default='', envvar='CAP2_CONFIG')
@click.option('--clean-reads/--all-reads', default=False)
@click.option('--upload/--no-upload', default=True)
@click.option('--download-only/--run', default=False)
@click.option('--scheduler-url', default=None, envvar='CAP2_LUIGI_SCHEDULER_URL')
@click.option('--max-attempts', default=2)
@click.option('-b', '--batch-size', default=10, help='Number of samples to process in parallel')
@click.option('-n', '--num-samples', default=100, help='Max number of samples for this worker to process')
@click.option('-w', '--workers', default=1)
@click.option('-t', '--threads', default=1)
@click.option('-m', '--max-ram', default=0)
@click.option('--timelimit', default=0, help='Stop adding jobs after N hours')
@click.option('--endpoint', default='https://pangea.gimmebio.com')
@click.option('-e', '--email', envvar='PANGEA_USER')
@click.option('-p', '--password', envvar='PANGEA_PASS')
@click.option('-s', '--stage', default='reads')
@click.option('--random-seed', type=int, default=None)
@click.option('--tag-name', default='MetaSUB CAP')
def cli_run_samples_from_tag(config, clean_reads, upload, download_only, scheduler_url,
                             max_attempts,
                             batch_size, num_samples, workers, threads, max_ram, timelimit,
                             endpoint, email, password, stage, random_seed,
                             tag_name):
    set_config(endpoint, email, password, None, None, name_is_uuid=True)
    tag = PangeaTag(tag_name, email, password, endpoint)
    samples = [
        samp for samp in tag.pangea_samples(randomize=True, n=num_samples)
        if not clean_reads or samp.has_clean_reads()
    ]
    completed = _process_samples_in_chunks(
        samples,
        scheduler_url, stage, upload, download_only,
        config, threads, max_ram, clean_reads, workers,
        batch_size, timelimit
    )
