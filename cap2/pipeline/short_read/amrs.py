
import luigi
import subprocess
from os.path import join, dirname, basename

from ..config import PipelineConfig
from ..utils.conda import CondaPackage
from ..preprocessing.clean_reads import CleanReads
from ..databases.amr_db import MegaResDB, CardDB


class MegaRes(luigi.Task):
    sample_name = luigi.Parameter()
    pe1 = luigi.Parameter()
    pe2 = luigi.Parameter()
    config_filename = luigi.Parameter()
    cores = luigi.IntParameter(default=1)
    thresh = luigi.FloatParameter(default=80.0)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pkg = CondaPackage(
            package="resistome_analyzer",
            executable="resistome_analyzer",
            channel="bioconda"
        )
        self.aligner = CondaPackage(
            package="bowtie2",
            executable="bowtie2",
            channel="bioconda"
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

    def requires(self):
        return self.pkg, self.aligner, self.db, self.reads

    def output(self):
        mytarget = lambda el: luigi.LocalTarget(join(self.out_dir, el))
        return {
            'sam': mytarget(f'{self.sample_name}.megares.sam.sam'),
            'gene': mytarget(f'{self.sample_name}.megares.gene.tsv'),
            'group': mytarget(f'{self.sample_name}.megares.group.tsv'),
            'classus': mytarget(f'{self.sample_name}.megares.classus.tsv'),
            'mech': mytarget(f'{self.sample_name}.megares.mech.tsv'),
        }

    def run(self):
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


class CARD(luigi.Task):
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
            channel="bioconda"
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
