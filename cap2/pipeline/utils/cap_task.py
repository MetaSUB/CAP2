
import luigi

from os.path import join


class CapTask(luigi.Task):
    """Base class for CAP2 tasks.

    Currently implements some basic shared logic.
    """

    def get_target(self, obj_name, module_name, field_name, ext):
        filename = f'{obj_name}.{module_name}.{field_name}.{ext}'
        filepath = join(self.out_dir, filename)
        target = luigi.LocalTarget(filepath)
        target.makedirs()
        return target
