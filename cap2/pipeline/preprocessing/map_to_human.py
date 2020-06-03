
import luigi
import subprocess
from os.path import join, dirname, basename

from .remove_adapters import AdapterRemoval
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
            channel="bioconda",
            config_filename=self.config_filename,
        )
        self.samtools = CondaPackage(
            package="samtools=1.09",
            executable="samtools",
            channel="bioconda",
            config_filename=self.config_filename,
        )
        self.config = PipelineConfig(self.config_filename)
        self.out_dir = self.config.out_dir
        self.db = HumanRemovalDB(config_filename=self.config_filename)
        self.reads = AdapterRemoval(
            pe1=self.pe1,
            pe2=self.pe2,
            sample_name=self.sample_name,
            config_filename=self.config_filename,
        )

    def requires(self):
        return self.samtools, self.pkg, self.db, self.reads

    @classmethod
    def version(cls):
        return 'v1.0.0'

    @classmethod
    def dependencies(cls):
        return ["samtools", "bowtie2", HumanRemovalDB, AdapterRemoval]

    @classmethod
    def _module_name(cls):
        return 'remove_human'

    def output(self):
        return {
            'bam': self.get_target('human_alignment', 'bam'),
            'nonhuman_reads_1': self.get_target('nonhuman_reads', 'R1.fastq.gz'),
            'nonhuman_reads_2': self.get_target('nonhuman_reads', 'R2.fastq.gz'),
        }

    def _run(self):
        fastq_out = self.output()['nonhuman_reads_1'].path.replace('R1', 'R%')
        cmd = ''.join((
            self.pkg.bin,
            ' -x ', self.db.bowtie2_index,
            ' -1 ', self.reads.output()['adapter_removed_reads_1'].path,
            ' -2 ', self.reads.output()['adapter_removed_reads_2'].path,
            f' --un-conc-gz {fastq_out} ',
            ' --threads ', str(self.cores),
            ' --very-sensitive ',
            f' | {self.samtools.bin} view -F 4 -b > ',
            self.output()['bam'].path,
        ))
        self.run_cmd(cmd)
