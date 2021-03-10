
import json
import pandas as pd
from scipy.spatial.distance import pdist, squareform

from ...capalyzer.table_builder.parsers import parse_taxa_report

from ..utils.cap_task import CapTask
from ..utils.utils import stats_one_fastq
from ..config import PipelineConfig
from .fast_taxa import FastKraken2
from .base_reads import BaseReads

from .control_sample_abundances import CONTROL_SAMPLES_TAXONOMIC_PROFILES


def control_distances(taxa, metric):
    taxa = pd.DataFrame(taxa, index=['sample'])
    taxa = pd.concat([taxa, CONTROL_SAMPLES_TAXONOMIC_PROFILES])
    taxa = taxa.fillna(0)
    if metric in ['jaccard']:
        taxa = taxa > 0
    dists = pdist(taxa, metric=metric)
    dists = pd.DataFrame(squareform(dists), index=taxa.index, columns=taxa.index)
    dists = dists['sample']
    dists = dists.to_dict()
    del dists['sample']
    return dists


class BasicSampleStats(CapTask):
    module_description = """
    Summarize some basic statistics about a sample
    based on the taxonomic profiles.
    """
    READ_STATS_DROPOUT = 1 / 1000
    MODULE_VERSION = 'v0.1.0'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.config = PipelineConfig(self.config_filename)
        self.taxa = FastKraken2.from_cap_task(self)
        self.reads = BaseReads.from_cap_task(self)
        self._taxa_report = None
        self._species_report = None

    @classmethod
    def _module_name(cls):
        return 'basic_sample_stats'

    def tool_version(self):
        return self.version()

    def requires(self):
        return self.taxa, self.reads

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
        if not self._species_report:
            sr, total_reads = {}, 0
            for taxon, reads in self.taxa_report.items():
                if 's__' in taxon and 't__' not in taxon:
                    total_reads += reads
                    sr[taxon.split('__')[1]] = reads
            self._species_report = {t: r / total_reads for t, r in sr.items()}
        return self._species_report

    def output(self):
        return {'report': self.get_target('report', 'json')}

    @property
    def stats_report(self):
        return self.output()['report'].path

    def _run(self):
        blob = {
            'control_distances_jaccard': control_distances(self.species_report, 'jaccard'),
            'control_distances_manhattan': control_distances(self.species_report, 'cityblock'),
            'control_distances_cosine': control_distances(self.species_report, 'cosine'),
            'human_fraction': self.species_report.get('Homo sapiens', 0),
            'mouse_fraction': self.species_report.get('Mus musculus', 0),
            'read_1_stats': stats_one_fastq(self.reads.read_1, self.READ_STATS_DROPOUT),
            'read_2_stats': stats_one_fastq(self.reads.read_2, self.READ_STATS_DROPOUT),
        }
        with open(self.stats_report, 'w') as f:
            f.write(json.dumps(blob))
