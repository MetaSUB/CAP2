
import json
import pandas as pd
from scipy.spatial.distance import cdist

from ..utils.cap_task import CapTask
from ..utils.utils import estimate_read_length
from ..config import PipelineConfig
from .fast_taxa import FastKraken2
from .base_reads import BaseReads

from .control_sample_abundances import CONTROL_SAMPLES_TAXONOMIC_PROFILES


def parse_taxa_report(local_path):
    """Return a Series of taxa_name to read_counts."""
    out, abundance_sum = {}, 0
    with open(local_path) as taxa_file:
        for line_num, line in enumerate(taxa_file):
            line = line.strip()
            tkns = line.split('\t')
            if not line or len(tkns) < 2:
                continue
            if len(tkns) == 2:
                out[tkns[0]] = float(tkns[1])
                abundance_sum += float(tkns[1])
            else:
                if line_num == 0:
                    continue
                out[tkns[1]] = float(tkns[3])
                abundance_sum += float(tkns[3])
    # out = {k: v for k, v in out.items() if 's__' in k and 't__' not in k}
    out = pd.Series(out)
    return out


def control_distances(taxa, metric):
    dists = cdist(taxa, CONTROL_SAMPLES_TAXONOMIC_PROFILES, metric=metric)
    dists = pd.Series(dists)
    dists.index = taxa.colummns
    dists = dists.to_dict()
    return dists


class BasicSampleStats(CapTask):
    module_description = """
    Summarize some basic statistics about a sample
    based on the taxonomic profiles.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.config = PipelineConfig(self.config_filename)
        self.taxa = FastKraken2.from_cap_task(self)
        self.reads = BaseReads.from_cap_task(self)
        self._taxa_report = None

    @classmethod
    def _module_name(cls):
        return 'basic_sample_stats'

    def tool_version(self):
        return self.version

    def requires(self):
        return self.taxa, self.reads

    @classmethod
    def version(cls):
        return 'v0.1.0'

    @classmethod
    def dependencies(cls):
        return [FastKraken2, BaseReads]

    @property
    def taxa_report(self):
        if not self._taxa_report:
            self._taxa_report = parse_taxa_report(self.taxa.output()['report'].path)
        return self._taxa_report

    @property
    def species_report(self):
        return self.taxa_report

    def output(self):
        return {'report': self.get_target('report', 'json')}

    def _run(self):
        blob = {
            'read_1_length': estimate_read_length(self.reads.read_1),
            'read_2_length': estimate_read_length(self.reads.read_2),
            'control_distances_jaccard': control_distances(self.species_report, 'jaccard'),
            'control_distances_manhattan': control_distances(self.species_report, 'manhattan'),
            'control_distances_cosine': control_distances(self.species_report, 'cosine'),
            'human_fraction': self.species_report.get('Homo sapiens', 0),
            'mouse_fraction': self.species_report.get('Mus musculus', 0),
        }
        with open(self.report, 'w') as f:
            f.write(json.dumps(blob))
