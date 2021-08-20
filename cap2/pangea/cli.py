
import click
import luigi
import time
import logging

from os import environ

from .api import get_task_list_for_sample, recursive_task_status

from ..pipeline.preprocessing import FastQC, MultiQC
from ..utils import chunks
from ..setup_logging import *
from ..constants import DATA_TYPES
from .constants import WORK_ORDER_PROTOS
from .cli_utils import (
    _process_samples_in_chunks,
    use_common_state,
)


logger = logging.getLogger('cap2')


@click.group()
def pangea():
    pass


@pangea.command('version')
def pangea_version():
    click.echo('v0.1.0')


@pangea.group('list')
def cli_list():
    pass

@cli_list.command('work-order')
@use_common_state
def cli_list_samples_from_work_order(state):
    state.prep_state()
    wop = state.pangea_work_order_proto()
    for sample in state.filter_samples(wop.pangea_samples()):
        print(sample)


@pangea.group('status')
def cli_status():
    pass



@cli_status.command('sample')
@use_common_state
@click.argument('org_name')
@click.argument('grp_name')
@click.argument('sample_name')
def cli_status_sample(state, org_name, grp_name, sample_name):
    """Check the status of a sample."""
    state.set_config(org_name=org_name, grp_name=grp_name, name_is_uuid=True)
    sample = state.pangea_sample(org_name, grp_name, sample_name)
    tasks = get_task_list_for_sample(
        sample, state.stage,
        config_path=state.config, require_clean_reads=state.clean_reads, cores=state.threads, max_ram=state.max_ram
    )
    click.echo(f'Checking status of sample "{sample_name}" for stage "{state.stage}"', err=True)
    sample_status = {}
    for task in tasks:
        for subtask, status in recursive_task_status(sample, type(task), config_path=state.config):
            sample_status[subtask.module_name()] = status
    for key, val in sample_status.items():
        if key == 'raw::base_reads':  # special case
            continue
        print(f'{key}:  {val}')


@cli_status.command('group')
@use_common_state
@click.argument('org_name')
@click.argument('grp_name')
def cli_status_group(state, org_name, grp_name):
    """Check the status of a group."""
    state.set_config(org_name=org_name, grp_name=grp_name, name_is_uuid=True)
    group = state.pangea_group(org_name, grp_name)
    samples = group.pangea_samples(randomize=True, check_for_reads=False, kind='no_reads')
    samples = list(samples)
    group_status = {}
    for sample in samples:
        tasks = get_task_list_for_sample(
            sample, state.stage,
            config_path=state.config, require_clean_reads=state.clean_reads, cores=state.threads, max_ram=state.max_ram
        )
        click.echo(f'Checking status of sample "{sample.name}" for stage "{state.stage}"', err=True)
        sample_status = {}
        for task in tasks:
            for subtask, status in recursive_task_status(sample, type(task), config_path=state.config):
                sample_status[subtask.module_name()] = status
        for key, val in sample_status.items():
            if key not in group_status:
                group_status[key] = {}
            group_status[key][val] = group_status[key].get(val, 0) + 1 
    for key, grp in group_status.items():
        if key == 'raw::base_reads':  # special case
            continue
        for status, count in grp.items():
            percent = 100 * count / len(samples)
            print(f'{key}, {status}:  {percent}')

@pangea.group()
def run():
    pass


@run.command('sample')
@use_common_state
@click.argument('org_name')
@click.argument('grp_name')
@click.argument('sample_name')
def cli_run_sample(state, org_name, grp_name, sample_name):
    state.set_config(org_name=org_name, grp_name=grp_name)
    sample = state.pangea_sample(org_name, grp_name, sample_name)
    tasks = get_task_list_for_sample(
        sample, state.stage,
        config_path=state.config, require_clean_reads=state.clean_reads,
    )
    state.luigi_build(tasks)


@run.command('samples')
@use_common_state
@click.argument('org_name')
@click.argument('grp_name')
def cli_run_samples(state, org_name, grp_name):
    state.set_config(org_name=org_name, grp_name=grp_name, name_is_uuid=True)
    group = state.pangea_group(org_name, grp_name)
    samples = state.filter_samples(group.pangea_samples(randomize=True, seed=state.random_seed))
    completed = _process_samples_in_chunks(samples, state)


@run.command('tag')
@use_common_state
@click.option('--num-samples', default=100)
@click.option('--tag-name', default='MetaSUB CAP')
def cli_run_samples_from_tag(state, num_samples, tag_name):
    state.set_config(name_is_uuid=True)
    tag = state.pangea_tag(tag_name)
    samples = state.filter_samples(tag.pangea_samples(randomize=True, n=num_samples))
    completed = _process_samples_in_chunks(samples, state)


@run.command('work-order')
@use_common_state
def cli_run_samples_from_work_order(state):
    state.set_config(name_is_uuid=True)
    wo = state.pangea_work_order_proto()
    samples = state.filter_samples(wo.pangea_samples())
    completed = _process_samples_in_chunks(samples, state)
