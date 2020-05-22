
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
            package="bowtie2==2.4.1",
            executable="bowtie2",
            channel="bioconda",
            config_filename=self.config_filename,
        )
        self.samtools = CondaPackage(
            package="samtools",
            executable="samtools",
            channel="bioconda",
            config_filename=self.config_filename,
        )
        self.config = PipelineConfig(self.config_filename)
        self.out_dir = self.config.out_dir
        self.db = HumanRemovalDB(config_filename=self.config_filename)

    def requires(self):
        return self.pkg, self.samtools, self.db

    @classmethod
    def version(cls):
        return 'v1.0.0'

    @classmethod
    def dependencies(cls):
        return ["bowtie2==2.4.1", HumanRemovalDB]

    @classmethod
    def _module_name(cls):
        return 'remove_human'

    def output(self):
        return {
            'bam': self.get_target('human_alignment', 'bam'),
            'nonhuman_reads': [
                self.get_target('nonhuman_reads', 'R1.fastq.gz'),
                self.get_target('nonhuman_reads', 'R2.fastq.gz')
            ],
            'human_reads': [
                self.get_target('human_reads', 'R1.fastq.gz'),
                self.get_target('human_reads', 'R2.fastq.gz')
            ],
        }

    def _run(self):
        cmd = ''.join((
            self.pkg.bin,
            ' -x ', self.db.bowtie2_index,
            ' -1 ', self.pe1,
            ' -2 ', self.pe2,
            f' --al-conc-gz {self.out_dir}/{self.sample_name}.remove_human.human_reads.R%.fastq.gz ',
            f' --un-conc-gz {self.out_dir}/{self.sample_name}.remove_human.nonhuman_reads.R%.fastq.gz ',
            ' --threads ', str(self.cores),
            ' --very-sensitive ',
            f' | {self.samtools.bin} view -F 4 -b > ',
            self.output()['bam'].path,
        ))
        self.run_cmd(cmd)
