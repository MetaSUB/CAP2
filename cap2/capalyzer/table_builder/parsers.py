import pandas as pd
import logging

logger = logging.getLogger(__name__)  # Same name as calling module


def parse_pileup(local_path, sparse=1):
    """Return a pandas dataframe with info from a pileup file.

    `sparse` is an int >= 1 if `sparse` is > 1 values will be averaged
    making the table more smaller.
    """
    compression = 'gzip'
    tbl = pd.read_csv(
        local_path,
        sep='\t',
        names=['seq', 'pos', 'ref_base', 'read_count', 'read_results', 'quality'],
        compression=compression,
    )
    if sparse > 1:
        tbl = tbl.set_index(['seq', 'pos']).rolling(sparse, center=True).mean()
        tbl = tbl.dropna()
        tbl = tbl.reset_index()
        tbl = tbl.query('pos % @sparse == 0')
    return tbl


def parse_taxa_report(local_path, **kwargs):
    try:
        return _parse_taxa_report(local_path, **kwargs)
    except Exception:
        logger.debug(f'[ParseTaxaReport] failed to parse {local_path}')
        raise


def _parse_taxa_report(local_path, **kwargs):
    """Return a dict of taxa_name to read_counts."""
    out, abundance_sum = {}, 0
    with open(local_path) as taxa_file:
        for line_num, line in enumerate(taxa_file):
            line = line.strip()
            tkns = line.split('\t')
            if not line or len(tkns) < 2:
                continue
            if len(tkns) == 2:
                taxon = tkns[0]
                taxon = taxon.split('|')[-1]
                abundance = float(tkns[1])
            elif len(tkns) == 6:
                taxon = tkns[5].strip()
                taxon_rank = tkns[3].strip().lower()
                if len(taxon_rank) > 1:
                    continue
                taxon = f'{taxon_rank}__{taxon}'
                abundance = float(tkns[1])
            else:
                if line_num == 0:
                    continue
                taxon = tkns[1]
                abundance = float(tkns[3])
            if (not kwargs.get('species_only', False)) or ('s__' in taxon):
                out[taxon] = abundance
                abundance_sum += abundance
    if kwargs.get('normalize', False):
        out = {k: v / abundance_sum for k, v in out.items()}
    if kwargs.get('minimum_abundance', 0):
        out = {k: v for k, v in out.items() if v >= kwargs['minimum_abundance']}
    return out
