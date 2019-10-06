
import luigi
from gzip import open as gopen
from os.path import join

from ..config import PipelineConfig


class CountRawReads(luigi.Task):
    in_filename = luigi.Parameter()
    sample_name = luigi.Parameter()
    config_filename = luigi.Parameter()

    def output(self):
        target = luigi.LocalTarget(join(
            PipelineConfig(self.config_filename).out_dir,
            f'{self.sample_name}.read_counts.csv'
        ))
        target.makedirs()
        return target

    def run(self):
        count = 0
        with gopen(self.in_filename) as i:
            for line in i:
                count += 1
        with open(self.output().path, 'a') as o:
            print(f'raw_reads,{count / 4}', file=o)
