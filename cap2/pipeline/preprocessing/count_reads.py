
import luigi
from gzip import open as gopen
from os.path import join

from ..utils.cap_task import CapTask
from ..config import PipelineConfig


class CountRawReads(CapTask):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def output(self):
        return {'read_counts': self.get_target('read_counts', 'csv')}

    def module_name(self):
        return 'count_raw_reads'

    def _run(self):
        count = 0
        with gopen(self.pe1) as i:
            for line in i:
                count += 1
        with open(self.output()['read_counts'].path, 'a') as o:
            print(f'{self.sample_name},raw_reads,{count / 4}', file=o)
