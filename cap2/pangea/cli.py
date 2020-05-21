
import click
import luigi

from os import environ

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
@click.option('--endpoint', default='https://pangea.gimmebio.com')
@click.option('--s3-endpoint', default='https://s3.wasabisys.com')
@click.option('--s3-profile', default='default')
@click.option('-e', '--email', default=environ.get('PANGEA_USER', None))
@click.option('-p', '--password', default=environ.get('PANGEA_PASS', None))
@click.argument('org_name')
@click.argument('grp_name')
@click.argument('bucket_name')
def cli_run_group(endpoint, s3_endpoint, s3_profile, email, password,
                  org_name, grp_name, bucket_name):
    set_config(email, password, org_name, grp_name, bucket_name, s3_endpoint, s3_profile)
    group = PangeaGroup(grp_name, email, password, endpoint, org_name)

    fqc_tasks = []
    for sample in group.pangea_samples():
        click.echo(f'Processing sample {sample.name}...', err=True)
        task = PangeaLoadTask(
            pe1=sample.r1,
            pe2=sample.r2,
            sample_name=sample.name,
        )
        task.wrapped_module = FastQC
        task.requires_reads = True
        fqc_tasks.append(task)
        click.echo('done.', err=True)

    tasks = []
    mqc_task = PangeaGroupLoadTask.from_samples(grp_name, group.cap_samples())
    mqc_task.wrapped_module = MultiQC
    mqc_task.wrapped.fastqcs = fqc_tasks
    tasks.append(mqc_task)

    luigi.build(tasks, local_scheduler=True)


@run.command('sample')
@click.option('--endpoint', default='https://pangea.gimmebio.com')
@click.option('--s3-endpoint', default='https://s3.wasabisys.com')
@click.option('--s3-profile', default='default')
@click.option('-e', '--email', default=environ.get('PANGEA_USER', None))
@click.option('-p', '--password', default=environ.get('PANGEA_PASS', None))
@click.argument('org_name')
@click.argument('grp_name')
@click.argument('bucket_name')
@click.argument('sample_name')
def cli_run_sample(endpoint, s3_endpoint, s3_profile, email, password,
                   org_name, grp_name, bucket_name, sample_name):
    sample = PangeaSample(sample_name, email, password, endpoint, org_name, grp_name)
    # sample.download()
    set_config(email, password, org_name, grp_name, bucket_name, s3_endpoint, s3_profile)
    task = PangeaLoadTask(
        pe1=sample.r1,
        pe2=sample.r2,
        sample_name=sample.name,
    )
    task.wrapped_module = FastQC
    task.requires_reads = True
    luigi.build([task], local_scheduler=True)
