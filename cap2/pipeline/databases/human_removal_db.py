
import luigi


class HumanRemovalDB(luigi.Task):
    """This class is responsible for building and/or retriveing
    validating the database which will be sued to remove human
    reads from the sample.
    """

    @property
    def bowtie2_index(self):
        return self._bowtie2_index

    def output(self):
        pass

    def run(self):
        pass
