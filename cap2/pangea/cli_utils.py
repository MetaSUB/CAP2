import click
import luigi
import logging
import time
from luigi.configuration import get_config
from os import environ

from .utils import set_config
from ..utils import chunks
from ..setup_logging import *
from ..constants import DATA_TYPES
from .api import get_task_list_for_sample
from .pangea_sample import PangeaSample, PangeaGroup, PangeaTag, PangeaWorkOrder
from .constants import WORK_ORDER_PROTOS
from ..exceptions import CAPSampleError

logger = logging.getLogger('cap2')


def _process_one_sample_chunk(chunk, state):
    tasks = []
    for sample in chunk:
        tasks += get_task_list_for_sample(
            sample, state.stage,
            config_path=state.config, require_clean_reads=state.clean_reads, cores=state.threads, max_ram=state.max_ram
        )
    state.luigi_build(tasks)
    return chunk


def _process_samples_in_chunks(samples, state):
    start_time, completed = time.time(), []
    logging.info(f'Processing {len(samples)} samples')
    for chunk in chunks(samples, state.batch_size):
        logging.info(f'Completed processing {len(completed)} samples')
        if state.timelimit and (time.time() - start_time) > (60 * 60 * state.timelimit):
            logging.info(f'Timelimit reached. Stopping.')
            return completed
        completed += _process_one_sample_chunk(chunk, state)
    return completed


class State(object):

    def __init__(self):
        self.email = None
        self.password = None
        self.endpoint = 'https://pangeabio.io'
        self.config = None
        self.upload = None
        self.download_only = None
        self.scheduler_url = None
        self.data_kind = None
        self.workers = 1
        self.threads = 1
        self.clean_reads = None
        self.batch_size = None
        self.random_seed = None
        self.stage = None
        self.time_limit = None
        self.max_ram = None
        self.work_order = None

    def prep_state(self):
        if self.work_order:
            stage, uuid = WORK_ORDER_PROTOS[self.work_order]
            self.stage = stage
            self.work_order = uuid
            if self.stage in ['pre', 'qc']:
                self.clean_reads = False

    def set_config(self, org_name=None, grp_name=None, name_is_uuid=False):
        self.prep_state()
        set_config(
            self.endpoint, self.email, self.password,
            org_name=org_name, grp_name=grp_name, work_order_name=self.work_order,
            name_is_uuid=name_is_uuid, upload_allowed=self.upload, download_only=self.download_only,
            data_kind=self.data_kind,
        )

    def luigi_build(self, tasks):
        if not self.scheduler_url:
            return luigi.build(tasks, local_scheduler=True, workers=self.workers)
        return luigi.build(tasks, scheduler_url=self.scheduler_url, workers=self.workers)

    def pangea_sample(self, org_name, grp_name, sample_name, name_is_uuid=True):
        return PangeaSample(
            sample_name,
            self.email, self.password, self.endpoint,
            org_name, grp_name,
            kind=self.data_kind,
            name_is_uuid=name_is_uuid,
        )

    def pangea_group(self, org_name, grp_name):
        return PangeaGroup(grp_name, self.email, self.password, self.endpoint, org_name)

    def pangea_tag(self, tag_name):
        return PangeaTag(tag_name, self.email, self.password, self.endpoint)

    def pangea_work_order_proto(self):
        return PangeaWorkOrder(self.work_order, self.email, self.password, self.endpoint)

    def filter_samples(self, samples):
        out = []
        for samp in samples:
            try:
                if samp.has_reads() and ((not self.clean_reads) or samp.has_clean_reads()):
                    out.append(samp)
            except CAPSampleError:
                pass
        return out


pass_state = click.make_pass_decorator(State, ensure=True)


def stateful_option(param_name, *args, **kwargs):

    def callback(ctx, param, value):
        state = ctx.ensure_object(State)
        state.__setattr__(param_name, value)
        return value

    return lambda f: click.option(*args, **kwargs, expose_value=False, callback=callback)(f)


STATEFUL_OPTS = [
    stateful_option('email', '-e', '--email',
                                    envvar='PANGEA_USER',
                                    help='Your Pangea login email.'),
    stateful_option('password', '-p', '--password',
                                        envvar='PANGEA_USER',
                                        help='Your Pangea password.'),
    stateful_option('endpoint', '--endpoint',
                            default='https://pangeabio.io',
                            help='The URL to use for Pangea.'),
    stateful_option('config', '-c', '--config', type=click.Path(), default='', envvar='CAP2_CONFIG'),
    stateful_option('upload', '--upload/--no-upload', default=True),
    stateful_option('download_only', '--download-only/--run', default=False),
    stateful_option('scheduler_url', '--scheduler-url', default=None, envvar='CAP2_LUIGI_SCHEDULER_URL'),
    stateful_option('data_kind', '-k', '--data-kind', default='short_read', type=click.Choice(DATA_TYPES)),
    stateful_option('workers', '-w', '--workers', default=1),
    stateful_option('threads', '-t', '--threads', default=1),
    stateful_option('clean_reads', '--clean-reads/--all-reads', default=False),
    stateful_option('batch_size', '-b', '--batch-size', default=10, help='Number of samples to process in parallel'),
    stateful_option('random_seed', '--random-seed', type=int, default=None),
    stateful_option('stage', '-s', '--stage', default='reads'),
    stateful_option('timelimit', '--timelimit', default=0, help='Stop adding jobs after N hours'),
    stateful_option('max_ram', '-m', '--max-ram', default=0),
    stateful_option('work_order', '--work-order', default=list(WORK_ORDER_PROTOS.keys())[0], type=click.Choice(WORK_ORDER_PROTOS.keys())),
]


def common_options(f):
    for opt in STATEFUL_OPTS:
        f = opt(f)
    return f


def use_common_state(f):
    f = common_options(f)
    f = pass_state(f)
    return f
