
import luigi
import subprocess
from os.path import join, dirname, basename

from ..utils.cap_task import CapTask
from ..config import PipelineConfig
from ..utils.conda import CondaPackage
from ..preprocessing.clean_reads import CleanReads
from ..databases.amr_db import GrootDB, MegaResDB, CardDB


class GrootAMR(CapTask):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pkg = CondaPackage(
            package="groot==1.1.2",
            executable="groot",
            channel="bioconda",
            config_filename=self.config_filename,
        )
        self.config = PipelineConfig(self.config_filename)
        self.out_dir = self.config.out_dir
        self.db = GrootDB(config_filename=self.config_filename)
        self.reads = CleanReads(
            sample_name=self.sample_name,
            pe1=self.pe1,
            pe2=self.pe2,
            config_filename=self.config_filename
        )

    @classmethod
    def _module_name(cls):
        return 'groot'

    def requires(self):
        return self.pkg, self.db, self.reads

    @classmethod
    def version(cls):
        return 'v1.0.0'

    @classmethod
    def dependencies(cls):
        return ['groot==1.1.2', GrootDB, CleanReads]

    def output(self):
        return {
            'alignment': self.get_target('alignment', 'bam'),
        }

    def _run(self):
        align_cmd = f'{self.pkg.bin} align '
        align_cmd += f'-i {self.db.groot_index} -f {self.reads.reads[0]},{self.reads.reads[1]} '
        align_cmd += f'-p {self.cores} > {self.output()["alignment"].path}'
        report_cmd = f'{self.pkg.bin} report -i {self.output()["alignment"].path} '
        report_cmd += '--lowCov --plotCov'
        rm_cmd = f'rm {self.output()["alignment"].path}'
        subprocess.check_call(align_cmd + ' && ' + report_cmd + ' | ' + rm_cmd, shell=True)


class MegaRes(CapTask):
    thresh = luigi.FloatParameter(default=80.0)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pkg = CondaPackage(
            package="resistome_analyzer",
            executable="resistome_analyzer",
            channel="bioconda",
            config_filename=self.config_filename,
        )
        self.aligner = CondaPackage(
            package="bowtie2",
            executable="bowtie2",
            channel="bioconda",
            config_filename=self.config_filename,
        )
        self.config = PipelineConfig(self.config_filename)
        self.out_dir = self.config.out_dir
        self.db = MegaResDB(config_filename=self.config_filename)
        self.reads = CleanReads(
            sample_name=self.sample_name,
            pe1=self.pe1,
            pe2=self.pe2,
            config_filename=self.config_filename
        )

    def module_name(self):
        return 'megares'

    def requires(self):
        return self.pkg, self.aligner, self.db, self.reads

    def output(self):
        mytarget = lambda el: luigi.LocalTarget(join(self.out_dir, el))
        return {
            'sam': self.get_target('sam', 'sam'),
            'gene': self.get_target('gene', 'tsv'),
            'group': self.get_target('group', 'tsv'),
            'classus': self.get_target('classus', 'tsv'),
            'mech': self.get_target('mech', 'tsv'),
        }

    def _run(self):
        sam_file = self.output()["sam"].path
        cmd1 = (
            f'{self.aligner.bin} '
            f'-p {threads} '
            '--very-sensitive '
            f' -x {self.db.bowtie2_index} '
            f' -1 {self.reads.reads[0]} '
            f' -2 {self.reads.reads[1]} '
            '| samtools view -F 4  '
            f'> {sam_file} '
        )
        cmd2 = (
            f'{self.pkg.bin} '
            f'-ref_fp {self.db.fasta} '
            f'-sam_fp {sam_file} '
            f'-annot_fp {self.db.annotations} '
            f'-gene_fp {self.output()["gene"].path} '
            f'-group_fp {self.output()["group"].path} '
            f'-class_fp {self.output()["classus"].path} '
            f'-mech_fp {self.output()["mech"].path} '
            f'-t {self.thresh}'
        )
        subprocess.call(cmd1 + ' && ' + cmd2, shell=True)


class CARD(CapTask):
    sample_name = luigi.Parameter()
    pe1 = luigi.Parameter()
    pe2 = luigi.Parameter()
    config_filename = luigi.Parameter()
    cores = luigi.IntParameter(default=1)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pkg = CondaPackage(
            package="megares",
            executable="megares",
            channel="bioconda",
            config_filename=self.config_filename,
        )
        self.config = PipelineConfig(self.config_filename)
        self.out_dir = self.config.out_dir
        self.db = CardDB(config_filename=self.config_filename)
        self.reads = CleanReads(
            sample_name=self.sample_name,
            pe1=self.pe1,
            pe2=self.pe2,
            config_filename=self.config_filename
        )

    def requires(self):
        return self.pkg, self.db, self.reads

    def output(self):
        pass

    def run(self):
        cmd = (

        )
        subprocess.call(cmd, shell=True)
