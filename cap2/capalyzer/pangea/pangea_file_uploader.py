
from os import environ, makedirs
from time import time
from ..version import VERSION_URL_SAFE, VERSION
from os.path import join
from requests.exceptions import HTTPError

TMP_DIR = environ.get('CAP2_TMP_DIR', '/tmp')
CAPALYZER_DIR = join(TMP_DIR, 'capalyzer', 'v1')
makedirs(CAPALYZER_DIR, exist_ok=True)


class PangeaFileAlreadyExistsError(Exception):
    pass


class PangeaFileUploader:

    def __init__(self, pangea_group):
        self.grp = pangea_group
        self._module_cache = {}

    def result_is_up_to_date(self, raw_module_name, n_samples):
        """Return True iff the result exists and is up to date."""
        module_name = self.module_name(raw_module_name)
        try:
            ar = self.grp.analysis_result(module_name).get()
            current_n_samples = int(ar.replicate.split('-')[-1].split('_')[0])
            return current_n_samples == n_samples
        except HTTPError:  # occurs if AR does not exist
            return False
        except ValueError:  # occurs if the AR is too old to have a well formed replicate number
            return False
        return False

    def _module(self, module_name, replicate):
        try:
            return self._module_cache[(module_name, replicate)]
        except KeyError:
            ar = self.grp.analysis_result(module_name, replicate=replicate).create()
            self._module_cache[(module_name, replicate)] = ar
            return ar

    def replicate(self, n_samples=None):
        n_samples = n_samples if n_samples else len(list(self.grp.get_samples()))
        timestamp = int(time())
        replicate = f'{VERSION}-{timestamp}-{n_samples}'
        return replicate

    def module_name(self, raw_name):
        return f'cap2::capalyzer::{raw_name}'

    def upload_pandas(self, tbl, raw_module_name, field_name, replicate=None, n_samples=None):
        replicate = replicate if replicate else self.replicate(n_samples=n_samples)
        module_name = self.module_name(raw_module_name)
        tbl_path = self._local_path(module_name, replicate, field_name, 'csv')
        tbl.to_csv(tbl_path)
        self.upload_file(tbl_path, raw_module_name, field_name, replicate=replicate)

    def upload_file(self, local_filepath, raw_module_name, field_name, replicate=None, n_samples=None):
        replicate = replicate if replicate else self.replicate(n_samples=n_samples)
        module_name = self.module_name(raw_module_name)
        try:
            analysis_result = self._module(module_name, replicate)
        except HTTPError:
            raise PangeaFileAlreadyExistsError(f'module "{module_name}" replicate "{replicate}" already exists for group "{self.grp.name}"')
        field = analysis_result.field(field_name).create()
        field.upload_file(local_filepath)
        field.save()

    def _local_path(self, module_name, replicate, field_name, ext):
        filename = f'{self.grp.name}.{module_name}.{replicate}.{field_name}.{ext}'
        filename = filename.lower()
        filename = filename.replace(' ', '_')
        filepath = join(CAPALYZER_DIR, filename)
        return filepath
