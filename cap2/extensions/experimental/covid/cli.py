
import click
import luigi
import time

from .fast_detect import Kraken2FastDetectCovid
from .align_to_covid_genome import AlignReadsToCovidGenome
from .genome_coverage import CovidGenomeCoverage
from .make_pileup import MakeCovidPileup
from .make_covid_consensus_seq import MakeCovidConsensusSeq
from .call_covid_variants import CallCovidVariants

from ....pangea.cli import set_config
from ....pangea.api import wrap_task
from ....pangea.pangea_sample import PangeaGroup
from ....pipeline.preprocessing import BaseReads
from ....pipeline.preprocessing.map_to_human import RemoveHumanReads
from ....utils import chunks


@click.group('covid')
def covid_cli():
    pass


@covid_cli.group('run')
def run_cli():
    pass


def get_task_list_for_sample(sample, config, threads):
    base_reads = wrap_task(
        sample, BaseReads,
        upload=False, config_path=config, cores=threads, requires_reads=True
    )

    wrapit = lambda x: wrap_task(sample, x, config_path=config, cores=threads)

    fast_detect = wrapit(Kraken2FastDetectCovid)
    fast_detect.wrapped.reads = base_reads

    nonhuman_reads = wrapit(RemoveHumanReads)
    nonhuman_reads.wrapped.adapter_removed_reads.reads = base_reads

    align_to_covid = wrapit(AlignReadsToCovidGenome)
    align_to_covid.wrapped.reads = nonhuman_reads

    covid_genome_coverage = wrapit(CovidGenomeCoverage)
    covid_genome_coverage.wrapped.bam = align_to_covid

    covid_pileup = wrapit(MakeCovidPileup)
    covid_pileup.wrapped.bam = align_to_covid

    covid_consensus = wrapit(MakeCovidConsensusSeq)
    covid_consensus.wrapped.pileup = covid_pileup

    covid_variants = wrapit(CallCovidVariants)
    covid_variants.wrapped.pileup = covid_pileup

    tasks = [
        fast_detect, covid_pileup, covid_genome_coverage, align_to_covid,
        covid_consensus, covid_variants
    ]
    return tasks


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
@click.argument('org_name')
@click.argument('grp_name')
def cli_run_samples(config, clean_reads, upload, download_only, scheduler_url,
                    max_attempts,
                    batch_size, workers, threads, timelimit,
                    endpoint, email, password, random_seed,
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
        tasks = []
        for sample in chunk:
            tasks += get_task_list_for_sample(sample, config, threads)
        if not scheduler_url:
            luigi.build(tasks, local_scheduler=True, workers=workers)
        else:
            luigi.build(
                tasks, scheduler_url=scheduler_url, workers=workers
            )
        completed += chunk
