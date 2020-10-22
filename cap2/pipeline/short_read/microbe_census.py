
import luigi
import subprocess
from os.path import join, dirname, basename

from ..utils.cap_task import CapTask
from ..config import PipelineConfig
from ..utils.conda import CondaPackage
from ..preprocessing.clean_reads import CleanReads


class MicrobeCensus(CapTask):
    module_description = """
    This module provides an estimate of the average genome size in a microbiome.

    Motivation: AGS estimate can help uncover ecological relationships
    and adaptation.

    About: AGS is estimated by aligning reads to ~30 Universal Single Copy
    Genes and comparing the number of copies of USiCGs to the total DNA in
    a sample.

    Negatives: AGS estiamtion sometimes produces implausible estimates.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pkg = CondaPackage(
            package="microbecensus==1.1.1",
            executable="microbe_census",
            channel="bioconda",
            config_filename=self.config_filename,
        )
        self.config = PipelineConfig(self.config_filename)
        self.out_dir = self.config.out_dir
        self.reads = CleanReads(
            sample_name=self.sample_name,
            pe1=self.pe1,
            pe2=self.pe2,
            config_filename=self.config_filename
        )

    def tool_version(self):
        return self.run_cmd(f'{self.pkg.bin} --version').stderr.decode('utf-8')

    @classmethod
    def _module_name(self):
        return 'microbe_census'

    def requires(self):
        return self.pkg, self.reads

    @classmethod
    def version(self):
        return 'v0.1.0'

    @classmethod
    def dependencies(self):
        return ["microbecensus==1.1.1", CleanReads]

    def output(self):
        return {
            'report': self.get_target('report', 'tsv'),
        }

    def _run(self):
        cmd = (
            f'{self.pkg.bin} '
            f'-t {self.cores} '
            f'{self.reads.output()["clean_reads"][0].path},'
            f'{self.reads.output()["clean_reads"][1].path} '
            f'{self.output()["report"].path}'
        )
        self.run_cmd(cmd)
