
import luigi
import subprocess
import json
from os.path import join, dirname, basename

from ....pipeline.utils.cap_task import CapTask
from ....pipeline.config import PipelineConfig
from ....pipeline.utils.conda import CondaPackage
from .preclassify_db import PreclassifyKraken2DBDataDown
from ....pipeline.preprocessing.base_reads import BaseReads


class PreclassifyKraken2(CapTask):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.rsync_pkg = CondaPackage(
            package="rsync",
            executable="rsync",
            channel="conda-forge",
            env="CAP_v2_kraken2",
            config_filename=self.config_filename,
        )
        self.pkg = CondaPackage(
            package="kraken2",
            executable="kraken2",
            channel="bioconda",
            env="CAP_v2_kraken2",
            config_filename=self.config_filename,
        )
        self.config = PipelineConfig(self.config_filename)
        self.out_dir = self.config.out_dir
        self.db = PreclassifyKraken2DBDataDown(config_filename=self.config_filename)
        self.reads = BaseReads(
            sample_name=self.sample_name,
            pe1=self.pe1,
            pe2=self.pe2,
            config_filename=self.config_filename
        )

    @classmethod
    def _module_name(cls):
        return 'preclassify_kraken2'

    def requires(self):
        return self.rsync_pkg, self.pkg, self.db, self.reads

    @classmethod
    def version(cls):
        return 'v0.1.0'

    @classmethod
    def dependencies(cls):
        return ['kraken2', PreclassifyKraken2DBDataDown, BaseReads]

    def output(self):
        return {
            'report_16s': self.get_target('report_16s', 'tsv'),
            'report_metagenome': self.get_target('report_metagenome', 'tsv'),
            'read_assignments_16s': self.get_target('read_assignments_16s', 'tsv'),
            'read_assignments_metagenome': self.get_target('read_assignments_metagenome', 'tsv'),
        }

    def _run(self):
        cmd = (
            f'{self.pkg.bin} '
            f'--db {self.db.kraken2_16s_db} '
            f'--db {self.db.kraken2_metagenome_db} '
            f'--threads {self.cores} '
            '--use-mpa-style '
            '--gzip-compressed '
            f'--report {self.output()["report"].path} '
            f'{self.reads.output()["base_reads_1"].path} '
            f'> {self.output()["read_assignments"].path}'
        )
        self.run_cmd(cmd)


class PreclassifySample(CapTask):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.config = PipelineConfig(self.config_filename)
        self.out_dir = self.config.out_dir
        self.kraken2 = PreclassifyKraken2(
            sample_name=self.sample_name,
            pe1=self.pe1,
            pe2=self.pe2,
            config_filename=self.config_filename
        )

    @classmethod
    def _module_name(cls):
        return 'preclassify_sample'

    def requires(self):
        return self.kraken2

    @classmethod
    def version(cls):
        return 'v0.1.0'

    @classmethod
    def dependencies(cls):
        return [PreclassifyKraken2]

    def output(self):
        return {
            'report': self.get_target('report', 'json'),
        }

    def _fraction_classified(self, report):
        n_classified, n_total = 0, 1
        with open(self.kraken2.output()[report].path) as f:
            for line in f:
                class_code = line[0]
                if class_code == 'C':
                    n_classified += 1
                n_total += 1
        return n_classified / n_total

    def _run(self):
        classified_16s = self._fraction_classified("read_assignments_16s")
        classified_metagenome = self._fraction_classified("read_assignments_metagenome")
        sample_type = 'UNKNOWN'
        elif classified_16s < 0.01 and classified_metagenome > 0.01:
            sample_type = 'METAGENOME'
        elif classified_16s > 0.01 and classified_metagenome < 0.01:
            sample_type = 'AMPLICON'
        blob = {
            'classification_rate_16s': classified_16s,
            'classification_rate_metagenome': classified_metagenome,
            'sample_type': sample_type,
        }
        with open(self.output()['report'].path, 'w') as report_file:
            report_file.write(json.dumps(blob))
