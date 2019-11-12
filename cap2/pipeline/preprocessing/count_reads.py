
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
        return {'read_counts': target}

    def run(self):
        count = 0
        with gopen(self.in_filename) as i:
            for line in i:
                count += 1
        with open(self.output()['read_counts'].path, 'a') as o:
            print(f'{self.sample_name},raw_reads,{count / 4}', file=o)
