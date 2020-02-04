
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
        trgt = lambda a, b: self.get_target(self.sample_name, 'remove_human', a, b)
        return {
            'bam': trgt('human_alignment', 'bam'),
            'nonhuman_reads': [
                trgt('nonhuman_reads', 'R1.fastq.gz'),
                trgt('nonhuman_reads', 'R2.fastq.gz')
            ],
            'human_reads': [
                trgt('human_reads', 'R1.fastq.gz'),
                trgt('human_reads', 'R2.fastq.gz')
            ],
        }

    def run(self):
        cmd = ''.join((
                self.pkg.bin,
                ' -x ', self.db.bowtie2_index,
                ' -1 ', self.pe1,
                ' -2 ', self.pe2,
                f' --conc-gz {self.out_dir}/{self.sample_name}.remove_human.human_reads.R%.fastq.gz ',
                f' --un-conc-gz {self.out_dir}/{self.sample_name}.remove_human.nonhuman_reads.R%.fastq.gz ',
                ' --threads ', str(self.cores),
                ' --very-sensitive ',
                f' | {self.samtools.bin} view -F 4 -b > ',
                self.output()['bam'].path,
        ))
        self.run_cmd(cmd)
