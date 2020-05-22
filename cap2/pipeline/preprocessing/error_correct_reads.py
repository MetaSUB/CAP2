
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

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pkg = CondaPackage(
            package="spades==3.14.0",
            executable="spades.py",
            channel="bioconda",
            config_filename=self.config_filename,
        )
        self.config = PipelineConfig(self.config_filename)
        self.out_dir = self.config.out_dir
        self.nonhuman_reads = RemoveHumanReads(
            pe1=self.pe1,
            pe2=self.pe2,
            sample_name=self.sample_name,
            config_filename=self.config_filename,
        )

    def requires(self):
        return self.pkg, self.nonhuman_reads

    @classmethod
    def version(cls):
        return 'v1.0.0'

    @classmethod
    def dependencies(cls):
        return ["spades==3.14.0", RemoveHumanReads]

    @classmethod
    def _module_name(cls):
        return 'error_corrected_reads'

    def output(self):
        return {
            'error_corrected_reads': [
                self.get_target('R1', 'fastq.gz'),
                self.get_target('R2', 'fastq.gz'),
            ],
        }

    def _run(self):
        r1, r2 = self.nonhuman_reads.output()['nonhuman_reads']
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
        paths = self.output()['error_corrected_reads']
        cmd = f'mv {ec_r1[0]} {paths[0].path} && mv {ec_r2[0]} {paths[1].path}'
        self.run_cmd(cmd)
        rmtree(outdir)
