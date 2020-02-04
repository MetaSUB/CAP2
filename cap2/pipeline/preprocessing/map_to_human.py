
import luigi
import subprocess
from os.path import join, dirname, basename

from ..utils.cap_task import CapTask
from ..config import PipelineConfig
from ..utils.conda import CondaPackage
from ..databases.human_removal_db import HumanRemovalDB


class RemoveHumanReads(CapTask):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pkg = CondaPackage(
            package="bowtie2",
            executable="bowtie2",
            channel="bioconda"
        )
        self.samtools = CondaPackage(
            package="samtools",
            executable="samtools",
            channel="bioconda"
        )
        self.config = PipelineConfig(self.config_filename)
        self.out_dir = self.config.out_dir
        self.db = HumanRemovalDB(config_filename=self.config_filename)

    def requires(self):
        return self.pkg, self.samtools, self.db

    def output(self):
        bam = luigi.LocalTarget(join(self.out_dir, f'{self.sample_name}.human_alignment.bam'))
        bam.makedirs()
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
        cmd = ''.join((
                self.pkg.bin,
                ' -x ', self.db.bowtie2_index,
                ' -1 ', self.pe1,
                ' -2 ', self.pe2,
                f' --conc-gz {self.out_dir}/{self.sample_name}.human_reads.R%.fastq.gz ',
                f' --un-conc-gz {self.out_dir}/{self.sample_name}.nonhuman_reads.R%.fastq.gz ',
                ' --threads ', str(self.cores),
                ' --very-sensitive ',
                f' | {self.samtools.bin} view -F 4 -b > ',
                self.output()['bam'].path,
        ))
        self.run_cmd(cmd)
