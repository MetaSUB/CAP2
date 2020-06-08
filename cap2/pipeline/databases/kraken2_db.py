
import luigi

from os.path import join, abspath, dirname
from glob import glob
import subprocess

from ..config import PipelineConfig
from ..utils.conda import CondaPackage
from ..utils.cap_task import CapDbTask

DB_DATE = '2020-06-01'


'cap2/databases/2020-06-08/taxa_kraken2/db_download_flag',


class Kraken2DBDataDown(CapDbTask):
    config_filename = luigi.Parameter()
    cores = luigi.IntParameter(default=1)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pkg = CondaPackage(
            package="kraken2",
            executable="kraken2",
            channel="bioconda",
            env="CAP_v2_kraken2",
            config_filename=self.config_filename,
        )
        self.config = PipelineConfig(self.config_filename)
        self.libraries = [
            'archaea', 'bacteria', 'plasmid', 'viral', 'fungi', 'protozoa', 'human'
        ]
        self.kraken_db_dir = 'taxa_kraken2'
        self.download_libs = True

    def requires(self):
        return [self.pkg]

    @classmethod
    def _module_name(cls):
        return 'kraken2_taxa_db_down'

    @classmethod
    def version(cls):
        return 'v0.1.0'

    @classmethod
    def dependencies(cls):
        return ['kraken2', DB_DATE]

    @property
    def kraken2_db(self):
        return join(self.config.db_dir, self.kraken_db_dir)

    def output(self):
        download_flag = luigi.LocalTarget(join(self.kraken2_db, 'db_download_flag'))
        download_flag.makedirs()
        return {'flag': download_flag}

    def run(self):
        if self.config.db_mode == PipelineConfig.DB_MODE_BUILD:
            if self.download_libs:
                self.download_kraken2_db()
        open(self.output()['flag'].path, 'w').close()

    def download_kraken2_db(self):
        cmd = f'{self.pkg.bin}-build --use-ftp --download-taxonomy --db {self.kraken2_db}'
        self.run_cmd(cmd)
        for library in self.libraries:
            cmd = (
                f'PATH=$PATH:{dirname(abspath(self.pkg.bin))} '
                f'{self.pkg.bin}-build '
                f'--use-ftp '
                f'--download-library {library} '
                f'--db {self.kraken2_db}'
            )
            self.run_cmd(cmd)


class Kraken2DB(CapDbTask):
    config_filename = luigi.Parameter()
    cores = luigi.IntParameter(default=1)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pkg = CondaPackage(
            package="kraken2",
            executable="kraken2",
            channel="bioconda",
            env="CAP_v2_kraken2",
            config_filename=self.config_filename,
        )
        self.download_task = Kraken2DBDataDown(
            config_filename=self.config_filename,
            cores=self.cores,
        )
        self.config = PipelineConfig(self.config_filename)
        self.db_size = 120 * (1000 ** 3)  # 120 GB

    def requires(self):
        return self.pkg, self.download_task

    @classmethod
    def _module_name(cls):
        return 'kraken2_taxa_db'

    @classmethod
    def version(cls):
        return 'v0.1.0'

    @classmethod
    def dependencies(cls):
        return ['kraken2', DB_DATE, self.download_task]

    @property
    def kraken2_db(self):
        return self.download_task.kraken2_db

    def output(self):
        db_taxa = luigi.LocalTarget(join(self.kraken2_db, 'hash.k2d'))
        db_taxa.makedirs()
        return {'kraken2_db_taxa': db_taxa}

    def run(self):
        if self.config.db_mode == PipelineConfig.DB_MODE_BUILD:
            self.build_kraken2_db()
        else:
            self.download_kraken2_db_from_s3()

    def build_kraken2_db(self):
        cmd = (
            f'{self.pkg.bin}-build '
            '--build '
            f'--max-db-size {self.db_size} '
            f'--db {self.kraken2_db}'
        )
        self.run_cmd(cmd)

    def download_kraken2_db_from_s3(self):
        paths = [
            'cap2/databases/2020-06-08/taxa_kraken2/hash.k2d',
            'cap2/databases/2020-06-08/taxa_kraken2/opts.k2d',
            'cap2/databases/2020-06-08/taxa_kraken2/seqid2taxid.map',
            'cap2/databases/2020-06-08/taxa_kraken2/taxo.k2d',
            'cap2/databases/2020-06-08/taxa_kraken2/unmapped.txt',
        ]
        for path in paths:
            cmd = (
                'wget '
                f'--directory-prefix={dirname(self.output()["kraken2_db_taxa"].path)} '
                f'https://s3.wasabisys.com/metasub-microbiome/{path} '

            )
            self.run_cmd(cmd)


class BrakenKraken2DB(CapDbTask):
    config_filename = luigi.Parameter()
    cores = luigi.IntParameter(default=1)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pkg = CondaPackage(
            package="bracken==2.6.0",
            executable="bracken",
            channel="bioconda",
            env="CAP_v2_bracken_kraken2",
            config_filename=self.config_filename,
        )
        self.kraken2_db_task = Kraken2DB(
            config_filename=self.config_filename,
            cores=self.cores,
        )
        self.config = PipelineConfig(self.config_filename)
        self.read_lengths = [75, 100, 125, 150, 175, 200, 225, 250]

    def requires(self):
        return self.pkg, self.kraken2_db_task

    @classmethod
    def _module_name(cls):
        return 'bracken_kraken2_taxa_db'

    @classmethod
    def version(cls):
        return 'v0.1.0'

    @classmethod
    def dependencies(cls):
        return ['bracken', DB_DATE, Kraken2DB]

    @property
    def kraken2_db(self):
        return self.kraken2_db_task.kraken2_db

    def output(self):
        out = {}
        for rlen in self.read_lengths:
            out[f'bracken_kraken2_db_{rlen}'] = luigi.LocalTarget(join(
                self.kraken2_db, f'database{rlen}mers.kmer_distrib'
            ))
        return out

    def get_index(self, length):
        for index_len in sorted(self.read_lengths, reverse=True):
            if length > index_len:
                break
        # index_len is now the largest index shorter than length or the smallest
        return index_len, self.output()[f'bracken_kraken2_db_{length}']

    def run(self):
        if self.config.db_mode == PipelineConfig.DB_MODE_BUILD:
            for rlen in self.read_lengths:
                self.build_bracken_db(rlen)
        else:
            self.download_bracken_db_from_s3()

    def build_bracken_db(self, read_len):
        cmd = (
            f'PATH=${{PATH}}:{dirname(abspath(self.pkg.bin))} '
            f'{self.pkg.bin}-build '
            f'-d {self.kraken2_db} '
            f'-t {self.cores} '
            f'-k 35 '
            f'-l {read_len} '
            f'-x {dirname(abspath(self.kraken2_db_task.pkg.bin))}/ '
            '; '
            f'test -e {self.kraken2_db}/database{read_len}mers.kmer_distrib'
        )
        self.run_cmd(cmd)

    def download_bracken_db_from_s3(self):
        paths = [
            'cap2/databases/2020-06-08/taxa_kraken2/database100mers.kmer_distrib',
            'cap2/databases/2020-06-08/taxa_kraken2/database100mers.kraken',
            'cap2/databases/2020-06-08/taxa_kraken2/database125mers.kmer_distrib',
            'cap2/databases/2020-06-08/taxa_kraken2/database125mers.kraken',
            'cap2/databases/2020-06-08/taxa_kraken2/database150mers.kmer_distrib',
            'cap2/databases/2020-06-08/taxa_kraken2/database150mers.kraken',
            'cap2/databases/2020-06-08/taxa_kraken2/database175mers.kmer_distrib',
            'cap2/databases/2020-06-08/taxa_kraken2/database175mers.kraken',
            'cap2/databases/2020-06-08/taxa_kraken2/database200mers.kmer_distrib',
            'cap2/databases/2020-06-08/taxa_kraken2/database200mers.kraken',
            'cap2/databases/2020-06-08/taxa_kraken2/database225mers.kmer_distrib',
            'cap2/databases/2020-06-08/taxa_kraken2/database225mers.kraken',
            'cap2/databases/2020-06-08/taxa_kraken2/database250mers.kmer_distrib',
            'cap2/databases/2020-06-08/taxa_kraken2/database250mers.kraken',
            'cap2/databases/2020-06-08/taxa_kraken2/database75mers.kmer_distrib',
            'cap2/databases/2020-06-08/taxa_kraken2/database75mers.kraken',
        ]
        for path in paths:
            cmd = (
                'wget '
                f'--directory-prefix={dirname(self.output()["bracken_kraken2_db_150"].path)} '
                f'https://s3.wasabisys.com/metasub-microbiome/{path} '

            )
            self.run_cmd(cmd)
