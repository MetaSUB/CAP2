
import pandas as pd
from os import environ
from os.path import isfile, join, basename
from requests.exceptions import HTTPError as HTTPError1
from pangea_api import (
    Knex,
    User,
    Organization,
    SampleAnalysisResultField,
)
from ..cap_table_builder import CAPFileSource

TMP_DIR = environ.get('CAP2_TMP_DIR', '/tmp')

class PangeaFileSource(CAPFileSource):

    def __init__(self, org_name, grp_name, email=None, password=None, endpoint='https://pangea.gimmebio.com'):
        knex = Knex()
        if email and password:
            User(knex, email, password).login()
        org = Organization(knex, org_name).get()
        self.grp = org.sample_group(grp_name).get()

    def sample_names(self):
        for sample in self.grp.get_samples():
            yield sample.name

    def metadata(self):
        tbl = {}
        for sample in self.grp.get_samples():
            tbl[sample.name] = sample.metadata
        tbl = pd.DataFrame.from_dict(tbl, orient='index')
        return tbl

    def module_files(self, module_name, field_name):
        """Return an iterable 2-ples of (sample_name, local_path) for modules of specified type."""
        for sample in self.grp.get_samples():
            try:
                ar = sample.analysis_result(module_name).get()
                arf = ar.field(field_name).get()
                local_path = join(TMP_DIR, basename(arf.get_referenced_filename()))
                if not isfile(local_path):
                    local_path = arf.download_file(filename=local_path)
            except HTTPError1:
                continue
            except Exception:
                continue
            yield sample.name, local_path
