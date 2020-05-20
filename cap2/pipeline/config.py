
from yaml import load
from os.path import join, abspath


class PipelineConfig:
    DB_MODE_DOWNLOAD = 'download'
    DB_MODE_BUILD = 'build'

    def __init__(self, filename):
        self.blob = load(open(filename).read())
        self.out_dir = self.blob['out_dir']
        self.db_dir = self.blob['db_dir']
        self.db_mode = self.blob.get('db_mode', PipelineConfig.DB_MODE_DOWNLOAD)
        self.conda_spec_dir = self.blob.get('conda_spec_dir', 'config/envs')
        self.conda_base_path = self.blob.get('conda_base_path', 'vendor/conda')
