
import math
import pandas as pd
import numpy as np

from scipy.stats import gmean, entropy
from numpy.linalg import norm
from random import random, sample
from umap import UMAP
from sklearn.decomposition import PCA
from numpy.random import choice


MIL = 1000 * 1000

# ALPHA Diversity`


def shannon_entropy(row, rarefy=0):
    """Return the shannon entropy of an iterable.
    Shannon entropy is robust to rarefaction but we keep
    the param for consistency.
    """
    row_sum, H = sum(row), 0
    for val in row:
        val = val / row_sum
        if val == 0:
            continue
        H += val * math.log2(val)
    if H < 0:
        H *= -1
    return H


def richness(row, rarefy=0, count=False):
    """Return the richness of an iterable."""
    if count:
        return sum(row > 0)
    row_sum, R = sum(row), 0
    for val in row:
        prob_success = val / row_sum
        prob_fail = 1 - prob_success
        prob_detect = 1 - (prob_fail ** rarefy)
        if val and rarefy <= 0:
            R += 1
        else:
            R += prob_detect
    return int(R + 0.5)


def chao1(row, rarefy=0):
    """Return richnes of an iterable"""
    row_sum, R, S, D = sum(row), 0, 0, 0.0000001
    num_reads = MIL if math.isclose(row_sum, 1) else row_sum  # default to 1M reads if compositional
    num_reads = rarefy if rarefy > 0 else num_reads  # if rarefy is set use that as read count

    for val in row:
        prob_success = val / row_sum
        prob_fail = 1 - prob_success
        prob_detect = 1 - (prob_fail ** num_reads)

        if rarefy:
            R += prob_detect
        elif val:
            R += 1
        S += 1 if val == 1 else 0
        D += 1 if val == 2 else 0
    return R + (S ** 2) / (2 * D)


# Beta Diversity


def clr(X):
    _X = X + 0.0000001
    _X = _X / norm(_X, ord=1)
    g = gmean(_X)
    _X = np.divide(_X, g)
    _X = np.log(_X)
    return _X


def rho_proportionality(P, Q):
    _P, _Q = clr(P), clr(Q)
    N = np.var(_P - _Q)
    D = np.var(_P) + np.var(_Q)
    return 1 - (N / D)


def jensen_shannon_dist(P, Q):
    _P = P / norm(P, ord=1)
    _Q = Q / norm(Q, ord=1)
    _M = 0.5 * (_P + _Q)
    J = 0.5 * (entropy(_P, _M) + entropy(_Q, _M))
    return math.sqrt(J)


# Rarefaction

def single_rarefaction(tbl, n=0):
    """Return the number of nonzero columns in tbl.
    Select n rows at random if specified.
    """
    if n and n > 0 and n < tbl.shape[0]:
        tbl = tbl.loc[sample(list(tbl.index), n)]
    return sum(tbl.sum(axis=0) > 0)


def rarefaction_analysis(tbl, ns=[], nsample=16, include_all=True):
    """Return a dataframe with two columns.
    N, the number of samples and Taxa, the number of nonzero elements.
    """
    result = []
    if not ns:
        ns = range(tbl.shape[0])
    if include_all:
        ns = list(ns) + [tbl.shape[0]]
    for n in ns:
        for _ in range(nsample):
            result.append((n, single_rarefaction(tbl, n=n)))
    return pd.DataFrame(result, columns=['N', 'Taxa'])


# Normalization


def proportions(tbl):
    tbl = (tbl.T / tbl.T.sum()).T
    return tbl


def subsample_row(row, n, drop=True):
    pvals = row.values
    pvals /= sum(pvals)
    vals = choice(row.index, p=pvals, size=(n,))
    tbl = {}
    if not drop:
        for val in row.index:
            tbl[val] = 0
    for val in vals:
        tbl[val] = 1 + tbl.get(val, 0)
    tbl = pd.Series(tbl)
    return tbl


def subsample(tbl, n=-1, niter=1):
    if n <= 0:
        n = int(tbl.T.sum().min())
    tbl = pd.concat([
        tbl.apply(lambda row: subsample_row(row, n), axis=1).fillna(0)
        for _ in range(niter)
    ])
    tbl = proportions(tbl)
    return tbl


# Experimental


def umap(mytbl, **kwargs):
    """Retrun a Pandas dataframe with UMAP, make a few basic default decisions."""
    metric = 'manhattan'
    if mytbl.shape[0] == mytbl.shape[1]:
        metric = 'precomputed'
    n_comp = kwargs.get('n_components', 2)
    umap_tbl = pd.DataFrame(UMAP(
        n_neighbors=kwargs.get('n_neighbors', min(100, int(mytbl.shape[0] / 4))),
        n_components=n_comp,
        metric=kwargs.get('metric', metric),
        random_state=kwargs.get('random_state', 42)
    ).fit_transform(mytbl))
    umap_tbl.index = mytbl.index
    umap_tbl = umap_tbl.rename(columns={i: f'UMAP-C{i}' for i in range(n_comp)})
    return umap_tbl


def fractal_dimension(tbl, scales=range(1, 10)):
    """Return a box-method curve used to estimate fractal dimension."""
    ranges = pd.DataFrame({
        'min': tbl.min(),
        'max': tbl.max(),
        'width': tbl.max() - tbl.min()
    })
    ranges = ranges.query('width > 0').T
    tbl = tbl[ranges.columns]
    print(ranges)
    Ns = []
    for scale in scales:
        bins = [
            np.arange(
                ranges[col_name]['min'],
                ranges[col_name]['max'] + 0.00001,
                ranges[col_name]['width'] / scale
            )
            for col_name in tbl.columns
        ]
        try:
            H, _ = np.histogramdd(tbl.values, bins=bins)
            Ns.append(np.sum(H > 0))
        except MemoryError:
            break
    return list(zip(scales, Ns))


def subsample_row(row, n):
    pvals = row.values
    pvals /= sum(pvals)
    vals = np.random.choice(row.index, p=pvals, size=(n,))
    tbl = {val: 0 for val in row.index}
    for val in vals:
        tbl[val] += 1
    tbl = pd.Series(tbl)
    return tbl


def train_validate_split(tbl, train_size):
    train_tbl = tbl.copy(deep=True).apply(
        lambda row: subsample_row(row, int(row.sum() * train_size)),
        axis=1
    ).fillna(0)
    val_tbl = (tbl - train_tbl).copy(deep=True)
    val_tbl = val_tbl.applymap(lambda el: 0 if el < 0 else el)
    return train_tbl, val_tbl


def run_pca(tbl, n_comp, zero_thresh):
    pca = PCA(n_components=n_comp)
    tbl_pca = pca.fit_transform(tbl)
    tbl_rev = pd.DataFrame(pca.inverse_transform(tbl_pca))
    tbl_rev.index = tbl.index
    tbl_rev.columns = tbl.columns
    tbl_rev = tbl_rev.applymap(lambda el: el if el > zero_thresh else 0)
    return tbl_rev


def pca_sample_cross_val(tbl,
                         train_size=0.9, min_comp=1, max_comp=100,
                         comp_step=1, decimals=5, zero_thresh=0.001,
                         transformer=lambda el: el, losses=[]):
    """Return a PCA normalized table based on molecular cross validation.
    Works by splitting each sample into train/validate subsets. Performs PCA
    on train then measures difference of INVPCA to validate. Returns the
    table normalized with the number of components minimizing loss.
    """
    max_comp = min(max_comp, tbl.shape[0])
    train_tbl, val_tbl = train_validate_split(tbl, train_size)
    val_prop = transformer(proportions(val_tbl))
    for i in range(min_comp, max_comp + 1, comp_step):
        predict = transformer(proportions(run_pca(train_tbl, i, zero_thresh)))
        loss = np.linalg.norm(val_prop.values - predict)
        loss = np.round(loss, decimals=decimals)
        losses.append((i, loss))
    losses.sort(key=lambda el: el[0])
    losses.sort(key=lambda el: el[1])
    return run_pca(tbl, losses[0][0], zero_thresh)


# Data utils

def group_small_cols(tbl, top=9, other_name='other', stat='mean'):
    """Return a table where all columns not in the <top> most abundant are pooled.
    Useful for visualization.
    """
    if stat in ['mean', 'ave', 'average']:
        vals = tbl.mean()
    elif stat in ['median']:
        vals = tbl.median()
    top_cols = list(vals.sort_values(ascending=False)[:top].index)
    other_cols = [col for col in tbl.columns if col not in top_cols]
    tbl[other_name] = tbl[other_cols].sum(axis=1)
    tbl = tbl.drop(columns=other_cols)
    return tbl
