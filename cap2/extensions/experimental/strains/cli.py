
import click
import luigi
import time

from .make_pileup import MakePileup
from .align_to_genome import AlignReadsToGenome
from .make_snp_graph import MakeSNPGraph
from .make_snp_table import MakeSNPTable
from .make_snp_clusters import MakeSNPClusters
from .merge_snp_graph import MergeSNPGraph
from .merge_snp_clusters import MergeSNPClusters

from ....pangea.cli import set_config
from ....pangea.api import wrap_task
from ....pangea.pangea_sample import PangeaGroup
from ....pipeline.preprocessing import BaseReads
from ....utils import chunks
from .tasks import StrainPangeaLoadTask, StrainPangeaGroupLoadTask
from .utils import clean_microbe_name
from .strainotyping.cli import strainotype_cli


@click.group('strains')
def strain_cli():
    pass


strain_cli.add_command(strainotype_cli)


@strain_cli.group('run')
def run_cli():
    pass


def strain_wrap_task(sample, module,
              requires_reads=False, upload=True, download_only=False,
              config_path='', cores=1, **kwargs):
    task = StrainPangeaLoadTask(
        pe1=sample.r1,
        pe2=sample.r2,
        sample_name=sample.name,
        wraps=module.module_name(),
        config_filename=config_path,
        cores=cores,
        **kwargs,
    )
    task.upload_allowed = upload
    task.wrapped_module = module
    task.requires_reads = requires_reads
    task.download_only = download_only
    return task


def get_task_list_for_sample(sample, config, threads, genome_name, genome_path):
    base_reads = wrap_task(
        sample, BaseReads,
        upload=False, config_path=config, cores=threads, requires_reads=True
    )
    align_genome = strain_wrap_task(
        sample, AlignReadsToGenome, config_path=config, cores=threads,
        genome_name=genome_name, genome_path=genome_path
    )
    align_genome.wrapped.reads.mouse_removed_reads.reads = base_reads
    make_pileup = strain_wrap_task(
        sample, MakePileup, config_path=config, cores=threads,
        genome_name=genome_name, genome_path=genome_path
    )
    make_pileup.wrapped.bam = align_genome
    make_snp_graph = strain_wrap_task(
        sample, MakeSNPGraph, config_path=config, cores=threads,
        genome_name=genome_name, genome_path=genome_path
    )
    make_snp_graph.wrapped.bam = align_genome
    make_snp_table = strain_wrap_task(
        sample, MakeSNPTable, config_path=config, cores=threads,
        genome_name=genome_name, genome_path=genome_path
    )
    make_snp_table.wrapped.graph = make_snp_graph
    make_snp_clusters = strain_wrap_task(
        sample, MakeSNPClusters, config_path=config, cores=threads,
        genome_name=genome_name, genome_path=genome_path
    )
    make_snp_clusters.wrapped.graph = make_snp_graph
    tasks = [make_pileup, make_snp_graph, make_snp_table, make_snp_clusters]
    return tasks


def get_task_list_for_group(group, config, threads, genome_name, genome_path):
    snp_graph_task = StrainPangeaGroupLoadTask.from_samples(
        group.name, group.cap_samples(), MergeSNPGraph.module_name(),
        genome_name=genome_name,
        genome_path=genome_path,
        config_path=config,
        cores=threads,
    )
    snp_graph_task.wrapped_module = MergeSNPGraph
    snp_cluster_task = StrainPangeaGroupLoadTask(
        group_name = group.name,
        samples=snp_graph_task.samples,
        wraps=MergeSNPClusters.module_name(),
        config_filename=config,
        cores=threads,
        genome_name=genome_name,
        genome_path=genome_path,
    )
    snp_cluster_task.wrapped_module = MergeSNPClusters
    snp_cluster_task.wrapped.graph = snp_graph_task
    tasks = [snp_graph_task, snp_cluster_task]
    return tasks


@run_cli.command('group')
@click.option('-c', '--config', type=click.Path(), default='', envvar='CAP2_CONFIG')
@click.option('--upload/--no-upload', default=True)
@click.option('--download-only/--run', default=False)
@click.option('--scheduler-url', default=None, envvar='CAP2_LUIGI_SCHEDULER_URL')
@click.option('--endpoint', default='https://pangea.gimmebio.com')
@click.option('-e', '--email', envvar='PANGEA_USER')
@click.option('-p', '--password', envvar='PANGEA_PASS')
@click.option('-w', '--workers', default=1)
@click.option('-t', '--threads', default=1)
@click.option('-g', '--genome-path', default='', help='Path to local fastas (instead of downloading from NCBI)')
@click.argument('org_name')
@click.argument('grp_name')
@click.argument('genome_name_list', type=click.File('r'))
def cli_run_group(config, upload, download_only, scheduler_url,
                  endpoint, email, password, workers, threads,
                  genome_path,
                  org_name, grp_name, genome_name_list):
    set_config(endpoint, email, password, org_name, grp_name)
    group = PangeaGroup(grp_name, email, password, endpoint, org_name)
    tasks = []
    genome_names = [line.strip() for line in genome_name_list if line.strip()]
    for genome_name in genome_names:
        genome_name = clean_microbe_name(genome_name)
        tasks += get_task_list_for_group(group, config, threads, genome_name, genome_path)

    if not scheduler_url:
        luigi.build(tasks, local_scheduler=True, workers=workers)
    else:
        luigi.build(tasks, scheduler_url=scheduler_url, workers=workers)


@run_cli.command('samples')
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
@click.option('--random-seed', type=int, default=None)
@click.option('-g', '--genome-path', default='', help='Path to local fastas (instead of downloading from NCBI)')
@click.argument('org_name')
@click.argument('grp_name')
@click.argument('genome_name_list', type=click.File('r'))
def cli_run_samples(config, clean_reads, upload, download_only, scheduler_url,
                    max_attempts,
                    batch_size, workers, threads, timelimit,
                    endpoint, email, password, random_seed,
                    genome_path,
                    org_name, grp_name, genome_name_list):
    set_config(endpoint, email, password, org_name, grp_name)
    group = PangeaGroup(grp_name, email, password, endpoint, org_name)
    start_time, completed = time.time(), []
    samples = [
        samp for samp in group.pangea_samples(randomize=True, seed=random_seed)
        if not clean_reads or samp.has_clean_reads()
    ]
    click.echo(f'Processing {len(samples)} samples', err=True)
    genome_names = [line.strip() for line in genome_name_list if line.strip()]
    for chunk in chunks(samples, batch_size):
        for genome_name in genome_names:
            genome_name = clean_microbe_name(genome_name)
            click.echo(f'Completed processing {len(completed)} samples for {genome_name}', err=True)
            if timelimit and (time.time() - start_time) > (60 * 60 * timelimit):
                click.echo(f'Timelimit reached. Stopping.', err=True)
                break
            tasks = []
            for sample in chunk:
                tasks += get_task_list_for_sample(sample, config, threads, genome_name, genome_path)
            if not scheduler_url:
                luigi.build(tasks, local_scheduler=True, workers=workers)
            else:
                luigi.build(
                    tasks, scheduler_url=scheduler_url, workers=workers
                )
            completed += chunk
