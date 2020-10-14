import luigi
import logging
import subprocess
from os.path import join, dirname, basename

from .make_pileup import MakeCovidPileup

from ....pipeline.utils.cap_task import CapTask
from ....pipeline.config import PipelineConfig
from ....pipeline.utils.conda import CondaPackage
from ....pipeline.preprocessing.map_to_human import RemoveHumanReads

logger = logging.getLogger('experimental::covid')


class MakeCovidConsensusSeq(CapTask):
    module_description = """
    This module 

    Motivation: 

    Negatives: 
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.ivar = CondaPackage(
            package="ivar",
            executable="ivar",
            channel="bioconda",
            config_filename=self.config_filename,
        )
        self.config = PipelineConfig(self.config_filename)
        self.out_dir = self.config.out_dir
        self.pileup = MakeCovidPileup(
            pe1=self.pe1,
            pe2=self.pe2,
            sample_name=self.sample_name,
            config_filename=self.config_filename,
            cores=self.cores,
            data_type=self.data_type,
        )

    def requires(self):
        return self.ivar, self.pileup

    @classmethod
    def version(cls):
        return 'v0.1.0'

    def tool_version(self):
        version = self.run_cmd(f'{self.ivar.bin} --version').stdout.decode('utf-8')
        return version

    @classmethod
    def dependencies(cls):
        return ["ivar", MakeCovidPileup]

    @classmethod
    def _module_name(cls):
        return 'experimental::make_covid_consensus_seq'

    def output(self):
        out = {
            f'fasta': self.get_target(f'consensus_seq', 'fa'),
        }
        return out

    @property
    def fasta_path(self):
        return self.output()[f'fasta'].path

    def _run(self):
        out_prefix = self.fasta_path.replace('.fa', '')
        cmd = (
            f'{self.ivar.bin} '
            'consensus '
            f'-i {self.pileup.pileup_path} '
            f'-p {out_prefix} '
            '-m 1 -t 0.6 -n N '
        )
        self.run_cmd(cmd)
