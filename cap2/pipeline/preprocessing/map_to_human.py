
import luigi
import subprocess
from os.path import join, dirname, basename

from ..config import PipelineConfig
from ..utils.conda import CondaPackage
from ..databases.human_removal_db import HumanRemovalDB


class RemoveHumanReads(luigi.Task):
    sample_name = luigi.Parameter()
    pe1 = luigi.Parameter()
    pe2 = luigi.Parameter()
    config_filename = luigi.Parameter()
    cores = luigi.IntParameter(default=1)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pkg = CondaPackage(
            package="bowtie2",
            executable="bowtie2",
            channel="bioconda"
        )
        self.config = PipelineConfig(self.config_filename)
        self.out_dir = self.config.out_dir
        self.db = HumanRemovalDB()

    def requires(self):
        return self.pkg, self.db

    def output(self):
        bam = luigi.LocalTarget(join(self.out_dir, f'{self.sample_name}.human_alignment.bam'))
        non_human_1 = luigi.LocalTarget(join(
            self.out_dir, f'{self.sample_name}.nonhuman_reads.R1.fastq.gz'
        ))
        non_human_2 = luigi.LocalTarget(join(
            self.out_dir, f'{self.sample_name}.nonhuman_reads.R2.fastq.gz'
        ))
        return {
            'bam': bam,
            'nonhuman_reads': [non_human_1, non_human_2],
        }

    def run(self):
        subprocess.call(
            [
                self.pkg.bin,
                '-x', self.db.bowtie2_index,
                '-1', self.pe1,
                '-2', self.pe2,
                '--threads', self.cores,
                '--very-sensitive',
                ' | samtools view -F 4 -b > ',
                self.output()['bam'].path,
            ]
        )
