
import luigi
import logging
import subprocess
import logging
from os.path import join, dirname, basename

from ....pipeline.utils.cap_task import CapTask
from ....pipeline.config import PipelineConfig
from ....pipeline.utils.conda import CondaPackage
from ....pipeline.preprocessing import BaseReads

logger = logging.getLogger('tcems')


class MixcrAlign(CapTask):
    module_description = """
    This module 

    Motivation: 

    Negatives: 
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pkg = CondaPackage(
            package="mixcr",
            executable="mixcr",
            channel="imperial-college-research-computing",
            config_filename=self.config_filename,
        )
        self.config = PipelineConfig(self.config_filename)
        self.reads = BaseReads.from_cap_task(self)

    def requires(self):
        return self.pkg, self.reads

    @classmethod
    def version(cls):
        return 'v0.1.0'

    def tool_version(self):
        version = self.run_cmd(f'{self.pkg.bin} --version').stdout.decode('utf-8')
        return version

    @classmethod
    def dependencies(cls):
        return ["mixcr", BaseReads]

    @classmethod
    def _module_name(cls):
        return 'tcems::mixcr_align'

    def output(self):
        out = {
            'alignments': self.get_target(f'alignments', 'vdjca'),
        }
        return out

    @property
    def alignments_path(self):
        return self.output()[f'alignments'].path

    def _run(self):
        align_cmd = f'{self.pkg.bin} align -p rna-seq -s hsa -OallowPartialAlignments=true {self.reads.read_1} {self.reads.read_1} {self.alignments_path}'
        self.run_cmd(align_cmd)


class MixcrAssemble(CapTask):
    module_description = """
    This module 

    Motivation: 

    Negatives: 
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.align = MixcrAlign.from_cap_task(self)
        self.pkg = self.align.pkg

    def requires(self):
        return self.pkg, self.align

    @classmethod
    def version(cls):
        return 'v0.1.0'

    def tool_version(self):
        version = self.run_cmd(f'{self.pkg.bin} --version').stdout.decode('utf-8')
        return version

    @classmethod
    def dependencies(cls):
        return ["mixcr", MixcrAlign]

    @classmethod
    def _module_name(cls):
        return 'tcems::mixcr_assemble'

    def output(self):
        out = {
            'partial_1': self.get_target(f'partial_1', 'vdjca'),
            'partial_2': self.get_target(f'partial_2', 'vdjca'),
            'extended': self.get_target(f'extended', 'vdjca'),
            'assembled': self.get_target(f'assembled_clones', 'clna'),
            'report': self.get_target(f'assembly_report', 'txt'),
            'contigs': self.get_target(f'full_clones', 'clns'),
        }
        return out

    @property
    def partial_1_path(self):
        return self.output()[f'partial_1'].path

    @property
    def partial_2_path(self):
        return self.output()[f'partial_2'].path

    @property
    def extended_path(self):
        return self.output()[f'extended'].path

    @property
    def assembled_path(self):
        return self.output()[f'assembled'].path

    @property
    def report_path(self):
        return self.output()[f'report'].path

    @property
    def contigs_path(self):
        return self.output()[f'contigs'].path

    def _run(self):
        cmds = [
            f'assemblePartial {self.align.alignments_path} {self.partial_1_path}',
            f'assemblePartial {self.partial_1_path} {self.partial_2_path}',
            f'extend {self.partial_2_path} {self.extended_path}',
            f'assemble --write-alignments --report {self.report_path} {self.extended_path} {self.assembled_path}',
            f'assembleContigs --report {self.report_path} {self.assembled_path} {self.contigs_path}',
        ]
        for cmd in cmds:
            cmd = f'{self.pkg.bin} ' + cmd
            self.run_cmd(cmd)


class MixcrClones(CapTask):
    module_description = """
    This module 

    Motivation: 

    Negatives: 
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.assemble = MixcrAssemble.from_cap_task(self)
        self.pkg = self.align.pkg

    def requires(self):
        return self.pkg, self.assemble

    @classmethod
    def version(cls):
        return 'v0.1.0'

    def tool_version(self):
        version = self.run_cmd(f'{self.pkg.bin} --version').stdout.decode('utf-8')
        return version

    @classmethod
    def dependencies(cls):
        return ["mixcr", MixcrAssemble]

    @classmethod
    def _module_name(cls):
        return 'tcems::mixcr_clones'

    def output(self):
        out = {
            'igh': self.get_target(f'full_clones_IGH', 'txt'),
        }
        return out

    @property
    def igh_path(self):
        return self.output()[f'igh'].path

    def _run(self):
        cmds = [
            f'exportClones -c IGH -p fullImputed {self.assemble.contigs_path} {self.igh_path}',
        ]
        for cmd in cmds:
            cmd = f'{self.pkg.bin} ' + cmd
            self.run_cmd(cmd)
