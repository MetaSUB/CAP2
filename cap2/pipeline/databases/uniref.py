
import luigi


class Uniref90(luigi.Task):
    config_filename = luigi.Parameter()
    cores = luigi.IntParameter(default=1)

    @property
    def diamond_index(self):
        pass

    def output(self):
        pass

    def run(self):
        pass
