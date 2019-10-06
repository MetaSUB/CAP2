
import luigi


class CondaPackage(luigi.Task):
    package = luigi.Parameter()

    executable = luigi.Parameter()

    channel = luigi.Parameter(default="anaconda")
    env = luigi.Parameter(default="AMEN")
    version = luigi.Parameter(default="None")
    python = luigi.IntParameter(default=3)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._env = CondaEnv(name=self.env, python=self.python)
        self.bin = os.path.join(
            self._env.bin, self.executable
        )

    def requires(self):
        return self._env

    def output(self):
        return luigi.LocalTarget(
            self.bin
        )

    def complete(self):
        return self.output().exists()

    def related_tool(self, name):
        return self._env.get_path(name)

    def run(self):
        if not self._env.contains(self.package):
            self._env.install(self.package, self.channel)
        
        if not self.output().exists():
            raise SpecificationError(
                'Tool {} was not correctly installed'.format(self.package)
            )