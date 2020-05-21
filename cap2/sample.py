

class Sample:
    """Thin data collector that represents a sample."""

    def __init__(self, sample_name, read1, read2):
        self.name = sample_name
        self.r1 = read1
        self.r2 = read2

    def as_tuple(self):
        return self.name, self.r1, self.r2

    @classmethod
    def samples_from_manifest(cls, manifest):
        samples = []
        for line in manifest:
            tkns = line.strip().split()
            samples.append(cls(*tkns))
        return samples
