"""Byte-weighted mtime histograms per child of a drill directory (spec: viz-widgets.md §4/V.4b).

A mean tells you where a directory's bytes sit on average; it can't tell you
that half of them are ancient and half are from this morning. This computes
the actual distribution: for each child of a drill dir, how many *bytes* of
descendant files fall in each mtime bin.

The weighting is the point. Each child's histogram area is its byte total, so
summing the bins older than a threshold gives exactly the bytes reclaimable at
that threshold — the number a "delete everything older than X" decision needs.
All children share one set of bin edges, so their shapes are comparable and a
single threshold line reads across the whole chart.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import numpy as np
import pandas as pd


@dataclass
class ChildHistogram:
    path: str
    """Child name, relative to the drill dir."""
    kind: str
    """'dir' or 'file' — direct-child files are their own single-file group."""
    bytes: list[int]
    """Bytes per bin; `len == len(edges) - 1`."""
    total_bytes: int
    n_files: int

    def to_dict(self) -> dict:
        return {
            'path': self.path,
            'kind': self.kind,
            'bytes': self.bytes,
            'total_bytes': self.total_bytes,
            'n_files': self.n_files,
        }


@dataclass
class AgeHistograms:
    edges: list[int]
    """Bin edges in epoch seconds, ascending; `len == bins + 1`."""
    children: list[ChildHistogram] = field(default_factory=list)
    """Ordered by `total_bytes` descending."""
    omitted: int = 0
    """Children dropped by `limit` — never dropped silently."""
    omitted_bytes: int = 0

    def to_dict(self) -> dict:
        return {
            'edges': self.edges,
            'children': [c.to_dict() for c in self.children],
            'omitted': self.omitted,
            'omitted_bytes': self.omitted_bytes,
        }


def age_histograms(
    df: pd.DataFrame,
    rel_path: str = '.',
    bins: int = 24,
    limit: int | None = 20,
    edges: list[int] | None = None,
) -> AgeHistograms:
    """Byte-weighted mtime histograms for each child of `rel_path` in `df`.

    `df` is a layer-2 frame (`path`, `kind`, `size`, `mtime`) whose paths are
    relative to the scan root. Only file rows contribute: directory rows carry
    rolled-up sizes, so counting them too would double-count every byte.

    Each descendant file is attributed to the child of `rel_path` it lives
    under (a file sitting directly in the dir is its own group). `edges` may be
    supplied to compare across dirs — or to bin by log-age rather than the
    default equal-width-in-time bins.
    """
    if bins < 1:
        raise ValueError(f"bins must be >= 1; got {bins}")

    files = df[df['kind'] == 'file']
    if rel_path not in ('.', ''):
        prefix = rel_path.rstrip('/') + '/'
        files = files[files['path'].str.startswith(prefix)]
        rel = files['path'].str[len(prefix):]
    else:
        rel = files['path']

    files = files[files['mtime'].notna()]
    rel = rel.loc[files.index]
    if files.empty:
        lo = 0 if edges is None else edges[0]
        hi = 0 if edges is None else edges[-1]
        return AgeHistograms(edges=list(edges) if edges else [lo, hi])

    mtimes = files['mtime'].to_numpy(dtype='float64')
    if edges is not None:
        edge_arr = np.asarray(edges, dtype='float64')
        if len(edge_arr) < 2:
            raise ValueError(f"edges must have >= 2 entries; got {len(edge_arr)}")
    else:
        lo = float(mtimes.min())
        hi = float(mtimes.max())
        # A single distinct mtime has no width to bin; give it one nominal
        # second so the lone bar still has an interval to live in.
        if hi <= lo:
            hi = lo + 1
        edge_arr = np.linspace(lo, hi, bins + 1)

    n_bins = len(edge_arr) - 1
    # `digitize` puts values below the first edge in bin 0 and above the last
    # in bin n-1 after clipping — caller-supplied edges may not span the data,
    # and dropping those bytes would silently understate a child's total.
    idx = np.clip(np.digitize(mtimes, edge_arr[1:-1], right=False), 0, n_bins - 1)

    child = rel.str.split('/', n=1).str[0].to_numpy()
    sizes = files['size'].fillna(0).to_numpy(dtype='int64')

    grouped = pd.DataFrame({'child': child, 'bin': idx, 'size': sizes})
    sums = grouped.groupby(['child', 'bin'], sort=False)['size'].sum()
    counts = grouped.groupby('child', sort=False)['size'].size()
    totals = grouped.groupby('child', sort=False)['size'].sum().sort_values(ascending=False)

    # A child is a dir iff it has descendants below it (i.e. any file under it
    # has a deeper relative path); direct-child files are single-file groups.
    is_dir = rel.str.contains('/').groupby(child).any()

    kept = list(totals.index)
    omitted = 0
    omitted_bytes = 0
    if limit is not None and len(kept) > limit:
        dropped = kept[limit:]
        omitted = len(dropped)
        omitted_bytes = int(totals.loc[dropped].sum())
        kept = kept[:limit]

    children = []
    for name in kept:
        row = np.zeros(n_bins, dtype='int64')
        per_bin = sums.loc[name]
        row[per_bin.index.to_numpy()] = per_bin.to_numpy()
        children.append(ChildHistogram(
            path=str(name),
            kind='dir' if bool(is_dir.loc[name]) else 'file',
            bytes=[int(v) for v in row],
            total_bytes=int(totals.loc[name]),
            n_files=int(counts.loc[name]),
        ))

    return AgeHistograms(
        edges=[int(e) for e in edge_arr],
        children=children,
        omitted=omitted,
        omitted_bytes=omitted_bytes,
    )
