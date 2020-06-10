
import click
import luigi
import time

from os import environ

from .api import get_task_list_for_sample
from .load_task import PangeaLoadTask, PangeaGroupLoadTask
from .pangea_sample import PangeaSample, PangeaGroup
from ..pipeline.preprocessing import FastQC, MultiQC


@click.group()
def pangea():
    pass


@pangea.group()
def run():
    pass


def set_config(email, password, org_name, grp_name, bucket_name, s3_endpoint, s3_profile):
    luigi.configuration.get_config().set('pangea', 'user', email)
    luigi.configuration.get_config().set('pangea', 'password', password)
    luigi.configuration.get_config().set('pangea', 'org_name', org_name)
    luigi.configuration.get_config().set('pangea', 'grp_name', grp_name)
    luigi.configuration.get_config().set('pangea', 's3_bucket_name', bucket_name)
    luigi.configuration.get_config().set('pangea', 's3_endpoint_url', s3_endpoint)
    luigi.configuration.get_config().set('pangea', 's3_profile', s3_profile)


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
    set_config(email, password, org_name, grp_name, bucket_name, s3_endpoint, s3_profile)
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
@click.argument('bucket_name')
@click.argument('sample_name')
def cli_run_sample(config, upload, scheduler_url, workers, threads,
                   endpoint, s3_endpoint, s3_profile, email, password,
                   stage,
                   org_name, grp_name, bucket_name, sample_name):
    sample = PangeaSample(sample_name, email, password, endpoint, org_name, grp_name)
    set_config(email, password, org_name, grp_name, bucket_name, s3_endpoint, s3_profile)
    tasks = get_task_list_for_sample(
        sample, stage, upload=upload, config_path=config, cores=threads
    )
    if not scheduler_url:
        luigi.build(tasks, local_scheduler=True, workers=workers)
    else:
        luigi.build(
            tasks, scheduler_url=scheduler_url, workers=workers
        )


@run.command('samples')
@click.option('-c', '--config', type=click.Path(), default='', envvar='CAP2_CONFIG')
@click.option('--clean-reads/--all-reads', default=False)
@click.option('--upload/--no-upload', default=True)
@click.option('--scheduler-url', default=None, envvar='CAP2_LUIGI_SCHEDULER_URL')
@click.option('-w', '--workers', default=1)
@click.option('-t', '--threads', default=1)
@click.option('--timelimit', default=0, help='Stop adding jobs after N hours')
@click.option('--endpoint', default='https://pangea.gimmebio.com')
@click.option('--s3-endpoint', default='https://s3.wasabisys.com')
@click.option('--s3-profile', default='default', envvar='CAP2_PANGEA_S3_PROFILE')
@click.option('-e', '--email', envvar='PANGEA_USER')
@click.option('-p', '--password', envvar='PANGEA_PASS')
@click.option('-s', '--stage', default='reads')
@click.argument('org_name')
@click.argument('grp_name')
@click.argument('bucket_name')
def cli_run_samples(config, clean_reads, upload, scheduler_url, workers, threads,
                    timelimit,
                    endpoint, s3_endpoint, s3_profile, email, password, stage,
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
        if clean_reads and not sample.has_clean_reads():
            continue
        tasks = get_task_list_for_sample(
            sample, stage, upload=upload, config_path=config, cores=threads
        )
        if not scheduler_url:
            luigi.build(tasks, local_scheduler=True, workers=workers)
        else:
            luigi.build(
                tasks, scheduler_url=scheduler_url, workers=workers
            )
        completed.add(index)
