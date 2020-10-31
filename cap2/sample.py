
from .exceptions import CAPSampleError
from .constants import DATA_TYPES


class Sample:
    """Thin data collector that represents a sample."""

    def __init__(self, sample_name, read1, read2=None, kind='short_read'):
        self.name = sample_name
        self.r1 = read1
        self.r2 = read2
        self.kind = kind
        if self.kind not in DATA_TYPES:
            raise CAPSampleError(f'kind {self.kind} is not one of {" ".join(DATA_TYPES)}')

    @property
    def paired(self):
        """Return True iff this sample contains paired end data."""
        return self.read2 and self.kind == 'short_read'

    def as_tuple(self):
        """Return a 3-ple of strigns with (sample_name, read_1_path, read_2_path)."""
        return self.name, self.r1, self.r2

    @classmethod
    def samples_from_manifest(cls, manifest):
        """Return a list of samples from a manifest file handle."""
        samples = []
        for line in manifest:
            tkns = line.strip().split()
            samples.append(cls(*tkns))
        return samples
