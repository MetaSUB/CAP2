
import luigi
import subprocess
from os.path import join, dirname, basename
from yaml import load
from shutil import rmtree

from ..config import PipelineConfig
from ..utils.conda import CondaPackage
from ..utils.cap_task import CapTask
from .map_to_human import RemoveHumanReads


class ErrorCorrectReads(CapTask):
    module_description = """
    This module error corrects reads using BayesHammer.

    Motivation: reads can contain base errors introduced
    during sequencing. Running error correction reduces
    the total number of errors. This can improve the
    classification rate and decrease the misclassification
    rate.

    Negatives: error correction can remove SNPs present in
    secondary strains, as such error corrected reads should
    not be used to call SNPs.
    """
    MODULE_VERSION = 'v0.2.2'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pkg = CondaPackage(
            package="spades",
            executable="spades.py",
            channel="bioconda",
            config_filename=self.config_filename,
        )
        self.config = PipelineConfig(self.config_filename)
        self.out_dir = self.config.out_dir
        self.nonhuman_reads = RemoveHumanReads.from_cap_task(self)

    def requires(self):
        return self.pkg, self.nonhuman_reads

    def tool_version(self):
        return self.run_cmd(f'{self.pkg.bin} --version').stderr.decode('utf-8')

    @classmethod
    def dependencies(cls):
        return ["spades", RemoveHumanReads]

    @classmethod
    def _module_name(cls):
        return 'error_corrected_reads'

    def output(self):
        out = {
            'error_corrected_reads_1': self.get_target('R1', 'fastq.gz'),
        }
        if self.paired:
            out['error_corrected_reads_2'] = self.get_target('R2', 'fastq.gz')
        return out

    def _run(self):
        if self.paired:
            return self._run_paired()
        return self._run_single()

    def _run_single(self):
        r1 = self.nonhuman_reads.output()['nonhuman_reads_1']
        cmd = self.pkg.bin
        cmd += f' --only-error-correction --meta -U {r1.path} '
        outdir = f'{self.sample_name}.error_correction_out'
        cmd += f' -t {self.cores} -o {outdir} '
        if self.max_ram > 0:
            cmd += f' -m {self.max_ram} '
        self.run_cmd(cmd)  # runs error correction but leaves output in a dir
        config_path = f'{self.sample_name}.error_correction_out/corrected/corrected.yaml'
        spades_out = load(open(config_path).read())
        ec_r1 = spades_out[0]['left reads']
        assert len(ec_r1) == 1
        paths = self.output()['error_corrected_reads_1']
        cmd = f'mv {ec_r1[0]} {paths[0].path} '
        self.run_cmd(cmd)
        rmtree(outdir)

    def _run_paired(self):
        r1 = self.nonhuman_reads.output()['nonhuman_reads_1']
        r2 = self.nonhuman_reads.output()['nonhuman_reads_2']
        cmd = self.pkg.bin
        cmd += f' --only-error-correction --meta -1 {r1.path} -2 {r2.path}'
        outdir = f'{self.sample_name}.error_correction_out'
        cmd += f' -t {self.cores} -o {outdir}'
        self.run_cmd(cmd)  # runs error correction but leaves output in a dir
        config_path = f'{self.sample_name}.error_correction_out/corrected/corrected.yaml'
        spades_out = load(open(config_path).read())
        ec_r1 = spades_out[0]['left reads']
        assert len(ec_r1) == 1
        ec_r2 = spades_out[0]['right reads']
        assert len(ec_r2) == 1
        paths = self.output()['error_corrected_reads_1'], self.output()['error_corrected_reads_2']
        cmd = f'mv {ec_r1[0]} {paths[0].path} && mv {ec_r2[0]} {paths[1].path}'
        self.run_cmd(cmd)
        rmtree(outdir)
