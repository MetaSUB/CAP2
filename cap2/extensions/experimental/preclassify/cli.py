
import click
import luigi
import time

from .preclassify import PreclassifySample
from .dynamic_pipeline import DynamicPipelineSample
from ....pangea.cli import set_config
from ....pangea.api import wrap_task
from ....pangea.pangea_sample import PangeaGroup
from ....pipeline.preprocessing import BaseReads

from ....sample import Sample
from ....api import run_modules
from ....constants import STAGES, DEFAULT_STAGE


@click.group('preclassify')
def preclassify_cli():
    pass


@preclassify_cli.command('sample')
@click.option('-w', '--workers', default=1)
@click.option('-t', '--threads', default=1)
@click.option('-c', '--config', type=click.Path(), default='')
@click.argument('manifest', type=click.File('r'))
def preclassify_sample_cli(workers, threads, config, manifest):
    samples = Sample.samples_from_manifest(manifest)
    modules = [PreclassifySample]
    run_modules(
        samples, modules,
        config_path=config,
        cores=threads,
        workers=workers,
    )


@preclassify_cli.command('pipeline')
@click.option('-w', '--workers', default=1)
@click.option('-t', '--threads', default=1)
@click.option('-c', '--config', type=click.Path(), default='')
@click.option('-s', '--stage', type=click.Choice(STAGES.keys()), default=DEFAULT_STAGE)
@click.argument('manifest', type=click.File('r'))
def dynamic_sample_cli(workers, threads, config, stage, manifest):
    samples = Sample.samples_from_manifest(manifest)
    instances = []
    for sample in samples:
        instance = DynamicPipelineSample.from_sample(sample, config, cores=threads)
        instance.pipeline_stage = stage
        instances.append(instance)
    luigi.build(instances, local_scheduler=True, workers=workers)


@preclassify_cli.group('pangea')
def preclassify_pangea_cli():
    pass


@preclassify_pangea_cli.command('samples')
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
def preclassify_pangea_samples_cli(config, scheduler_url, workers, threads, timelimit,
                                   endpoint, s3_endpoint, s3_profile, email, password,
                                   org_name, grp_name, bucket_name):
    set_config(endpoint, email, password, org_name, grp_name, bucket_name, s3_endpoint, s3_profile)
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
        sample_type = wrap_task(
            sample, PreclassifySample, config_path=config, cores=threads
        )
        sample_type.wrapped.kraken2.reads = reads
        dynamic_sample_type_pipeline = wrap_task(
            sample, DynamicPipelineSample, config_path=config, cores=threads
        )
        dynamic_sample_type_pipeline.wrapped.pangea = True
        dynamic_sample_type_pipeline.wrapped._sample_type = sample_type
        tasks = [dynamic_sample_type_pipeline]
        if not scheduler_url:
            luigi.build(tasks, local_scheduler=True, workers=workers)
        else:
            luigi.build(
                tasks, scheduler_url=scheduler_url, workers=workers
            )
        completed.add(index)
