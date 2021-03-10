
from yaml import load
from os import environ


class PipelineConfig:
    DB_MODE_DOWNLOAD = 'download'
    DB_MODE_BUILD = 'build'

    def __init__(self, filename):
        if filename:
            self.blob = load(open(filename).read())
        else:
            self.blob = {}
        self.out_dir = self.blob.get('out_dir', environ.get('CAP2_OUT_DIR', 'results'))
        self.db_dir = self.blob.get('db_dir', environ.get('CAP2_DB_DIR', 'cap2_dbs'))
        self.db_mode = self.blob.get('db_mode', PipelineConfig.DB_MODE_DOWNLOAD)
        self.conda_spec_dir = self.blob.get(
            'conda_spec_dir', environ.get('CAP2_CONDA_SPEC_DIR', 'config/envs')
        )
        self.conda_base_path = self.blob.get(
            'conda_base_path', environ.get('CAP2_CONDA_BASE_PATH', 'vendor/conda')
        )

        self.exc_metaspades = self.blob.get('EXC_METASPADES', None)
        self.module_versions = self.blob.get('module_version', {})

    def allowed_versions(self, module):
        """Return a list of the allowed versions for the specified module."""
        module_name = module.module_name()
        if module_name not in self.module_versions:
            module_name = module._module_name()
        if module_name not in self.module_versions:
            return []
        versions = self.module_versions[module_name]
        return versions
