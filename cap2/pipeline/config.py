
from yaml import load
from os.path import join, abspath


class PipelineConfig:

    def __init__(self, filename):
        self.blob = load(open(filename).read())
        self.out_dir = self.blob['out_dir']
        self.db_dir = self.blob['db_dir']
