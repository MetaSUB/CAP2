
import luigi
import subprocess
import json
import os
from os.path import join, dirname, basename
from tempfile import NamedTemporaryFile

from .fastqc import FastQC
from ..utils.cap_task import CapGroupTask
from ..config import PipelineConfig
from ..utils.conda import CondaPackage


class MultiQC(CapGroupTask):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @property
    def fastqcs(self):
        return self.module_req_list(FastQC)

    def requires(self):
        return self.fastqcs

    @classmethod
    def version(cls):
        return 'v1.0.0'

    @classmethod
    def dependencies(cls):
        return ['multiqc==1.8', FastQC]

    @classmethod
    def _module_name(cls):
        return 'multiqc'

    def output(self):
        return {'report': self.get_target('report', 'html')}

    def _run(self):
        custom_conf = {"sp": {"fastqc/zip": {"fn": "*fastqc.zip_out.zip"}}}
        conf_file = NamedTemporaryFile(delete=False, mode='w')
        conf_file.write(json.dumps(custom_conf))
        conf_file.close()

        file_list = NamedTemporaryFile(delete=False, mode='w')
        for fqc in self.fastqcs:
            out = fqc.output()['zip_output']
            print(out.path, file=file_list)
        file_list.close()

        cmd = ' '.join([
            'multiqc',
            '-f',
            '--no-data-dir',
            f'-i \'{self.group_name}\'',
            f'-n {self.output()["report"].path}',
            f'-c {conf_file.name} ',
            f'-l {file_list.name} ',
        ])
        try:
            self.run_cmd(cmd)
        finally:
            os.remove(conf_file.name)
            os.remove(file_list.name)
