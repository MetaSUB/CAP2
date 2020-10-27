import pandas as pd


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


def parse_taxa_report(local_path):
    """Return a dict of taxa_name to read_counts."""
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
    return out
