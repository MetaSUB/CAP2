
from os import environ, makedirs
from time import time
from ..version import VERSION_URL_SAFE
from os.path import join

TMP_DIR = environ.get('CAP2_TMP_DIR', '/tmp')
CAPALYZER_DIR = join(TMP_DIR, 'capalyzer', 'v1')
makedirs(CAPALYZER_DIR, exist_ok=True)


class PangeaFileUploader:

    def __init__(self, pangea_group):
        self.grp = pangea_group
        self._module_cache = {}

    def _module(self, module_name, replicate):
        try:
            return self._module_cache[(module_name, replicate)]
        except KeyError:
            ar = self.grp.analysis_result(module_name, replicate=replicate).create()
            self._module_cache[(module_name, replicate)] = ar
            return ar

    def replicate(self):
        timestamp, n_samples = int(time()), len(list(self.grp.get_samples()))
        replicate = f'{timestamp}-{n_samples}'
        return replicate

    def module_name(self, raw_name):
        return f'cap2::capalyzer-v{VERSION_URL_SAFE}::{raw_name}'

    def upload_pandas(self, tbl, raw_module_name, field_name, replicate=None):
        replicate = replicate if replicate else self.replicate()
        module_name = self.module_name(raw_module_name)
        tbl_path = self._local_path(module_name, replicate, field_name, 'csv')
        tbl.to_csv(tbl_path)
        self.upload_file(tbl_path, raw_module_name, field_name, replicate=replicate)

    def upload_file(self, local_filepath, raw_module_name, field_name, replicate=None):
        replicate = replicate if replicate else self.replicate()
        module_name = self.module_name(raw_module_name)
        analysis_result = self._module(module_name, replicate)
        field = analysis_result.field(field_name).create()
        field.upload_file(local_filepath)
        field.save()

    def _local_path(self, module_name, replicate, field_name, ext):
        filename = f'{self.grp.name}.{module_name}.{replicate}.{field_name}.{ext}'
        filename = filename.lower()
        filename = filename.replace(' ', '_')
        filepath = join(CAPALYZER_DIR, filename)
        return filepath
