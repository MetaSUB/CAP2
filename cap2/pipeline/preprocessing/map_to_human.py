
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
        self.adapter_removed_reads = AdapterRemoval(
            pe1=self.pe1,
            pe2=self.pe2,
            sample_name=self.sample_name,
            config_filename=self.config_filename,
            cores=self.cores,
            data_type=self.data_type,
        )

    def requires(self):
        return self.samtools, self.pkg, self.db, self.adapter_removed_reads

    @classmethod
    def version(cls):
        return 'v0.2.1'

    def tool_version(self):
        version = '[BOWTIE2]\n'
        version += self.run_cmd(f'{self.pkg.bin} --version').stderr.decode('utf-8')
        version += '\n[SAMTOOLS]\n'
        version += self.run_cmd(f'{self.samtools.bin} --version').stderr.decode('utf-8')
        return version

    @classmethod
    def dependencies(cls):
        return ["samtools", "bowtie2", HumanRemovalDB, AdapterRemoval]

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
            ' -U ', self.adapter_removed_reads.output()['adapter_removed_reads_1'].path,
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
            ' -1 ', self.adapter_removed_reads.output()['adapter_removed_reads_1'].path,
            ' -2 ', self.adapter_removed_reads.output()['adapter_removed_reads_2'].path,
            f' --un-conc-gz {fastq_out} ',
            ' --threads ', str(self.cores),
            ' --very-sensitive ',
            f' | {self.samtools.bin} view -F 4 -b > ',
            self.output()['bam'].path,
        ))
        self.run_cmd(cmd)
