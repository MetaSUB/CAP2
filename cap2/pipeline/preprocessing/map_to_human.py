
import luigi
import subprocess
from os.path import join, dirname, basename

from .map_to_mouse import RemoveMouseReads
from ..utils.cap_task import CapTask
from ..config import PipelineConfig
from ..utils.conda import CondaPackage
from ..databases.human_removal_db import HumanRemovalDB


class RemoveHumanReads(CapTask):
    module_description = """
    This module removes reads likely to be human.

    Motivation: nearly every metagenomic sample can contain
    human DNA some of which resembles microbial sequences.
    Removing likely human DNA decreases the chance of
    incorrectly mis-identifying human DNA as microbial.

    Negatives: in some samples the sequences that resemble
    human DNA may actually be microbial though in most cases
    part of the microbial genome will not resemble human.
    """
    MODULE_VERSION = 'v0.2.1'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pkg = CondaPackage(
            package="bowtie2",
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
        self.mouse_removed_reads = RemoveMouseReads.from_cap_task(self)

    def requires(self):
        return self.samtools, self.pkg, self.db, self.mouse_removed_reads

    def tool_version(self):
        version = '[BOWTIE2]\n'
        version += self.run_cmd(f'{self.pkg.bin} --version').stderr.decode('utf-8')
        version += '\n[SAMTOOLS]\n'
        version += self.run_cmd(f'{self.samtools.bin} --version').stderr.decode('utf-8')
        return version

    @classmethod
    def dependencies(cls):
        return ["samtools", "bowtie2", HumanRemovalDB, RemoveMouseReads]

    @classmethod
    def _module_name(cls):
        return 'remove_human'

    def output(self):
        out = {
            'bam': self.get_target('human_alignment', 'bam'),
            'nonhuman_reads_1': self.get_target('nonhuman_reads', 'R1.fastq.gz'),
        }
        if self.paired:
            out['nonhuman_reads_2'] = self.get_target('nonhuman_reads', 'R2.fastq.gz')
        return out

    def _run(self):
        if self.paired:
            return self._run_paired()
        return self._run_single()

    def _run_single(self):
        fastq_out = self.output()['nonhuman_reads_1'].path.replace('R1', 'R%')
        cmd = ''.join((
            self.pkg.bin,
            ' -x ', self.db.bowtie2_index,
            ' -U ', self.mouse_removed_reads.output()['nonmouse_reads_1'].path,
            f' --un-gz ', self.output()['nonhuman_reads_1'].path,
            ' --threads ', str(self.cores),
            ' --very-sensitive ',
            f' | {self.samtools.bin} view -F 4 -b > ',
            self.output()['bam'].path,
        ))
        self.run_cmd(cmd)

    def _run_paired(self):
        fastq_out = self.output()['nonhuman_reads_1'].path.replace('R1', 'R%')
        cmd = ''.join((
            self.pkg.bin,
            ' -x ', self.db.bowtie2_index,
            ' -1 ', self.mouse_removed_reads.output()['nonmouse_reads_1'].path,
            ' -2 ', self.mouse_removed_reads.output()['nonmouse_reads_2'].path,
            f' --un-conc-gz {fastq_out} ',
            ' --threads ', str(self.cores),
            ' --very-sensitive ',
            f' | {self.samtools.bin} view -F 4 -b > ',
            self.output()['bam'].path,
        ))
        self.run_cmd(cmd)
