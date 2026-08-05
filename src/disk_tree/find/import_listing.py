"""Import a pre-made object listing → canonical layer-2 scan.

Alternative producer to :func:`disk_tree.find.index` (the live walker): reads
one or more parquet globs conforming to the layer-1 listing schema (raw
object listing, GCS Storage Insights, or S3 Inventory — schema-normalized by
:mod:`disk_tree.listing`), filters to a single bucket, and runs the same
bottom-up aggregation as ``index()`` to produce a canonical per-path
DataFrame.

Phase-0 aggregation is in-memory pandas — fine for laptop-scale or
subsampled cloud listings; the multi-PB regime needs the out-of-core DuckDB
aggregation path (spec item B).
"""

from __future__ import annotations

from os.path import dirname
from typing import TYPE_CHECKING

import pandas as pd

from disk_tree.listing import prepare_listing
from .index import aggregate, IndexResult

if TYPE_CHECKING:
    import duckdb


def import_listing(
    listings: tuple[str, ...],
    bucket: str,
    scheme: str = 'gcs',
    con: "duckdb.DuckDBPyConnection | None" = None,
) -> IndexResult:
    """Read `bucket`'s rows from `listings` and aggregate to a canonical scan.

    `listings` is a tuple of parquet globs; multiple sources are merged with
    earlier-source-wins-per-bucket semantics (see :func:`prepare_listing`).
    `scheme` sets the URI prefix (e.g. ``gcs`` → ``gcs://<bucket>/<name>``).
    """
    import duckdb as _duckdb
    if con is None:
        con = _duckdb.connect()
    src = prepare_listing(con, listings)
    df = con.execute(
        f"SELECT name, size_bytes, epoch(created)::BIGINT AS created FROM {src} WHERE bucket = ? ORDER BY name",
        [bucket],
    ).df()
    if df.empty:
        raise ValueError(f"no rows for bucket {bucket!r} in listings {listings}")
    scan_root = f'{scheme}://{bucket}'
    names = df['name'].astype(str)
    files = pd.DataFrame({
        'path': names,
        'size': df['size_bytes'].fillna(0).astype('int64'),
        'mtime': df['created'].fillna(0).astype('int64'),
        'kind': 'file',
        'parent': names.apply(dirname),
        'uri': [f'{scan_root}/{n}' for n in names],
    })
    # Synthesize dir rows for every unique parent path in the listing — walk
    # backends emit these inline (see backends/s3.py:84-92), and their absence
    # would leave `n_children` under-counted (it counts input rows per parent,
    # so missing sub-dir rows means the sub-dir wouldn't count toward its
    # parent's direct-child tally).
    # Include '' (scan root) so root's n_desc includes-self, matching walk
    # backends (gfind emits `path='' kind='dir'` for the scan root).
    dir_paths = {''}
    for p in files['parent']:
        while p and p not in dir_paths:
            dir_paths.add(p)
            p = dirname(p)
    dir_names = sorted(dir_paths)
    dirs = pd.DataFrame({
        'path': dir_names,
        'size': 0,
        'mtime': 0,
        'kind': 'dir',
        'parent': [dirname(p) for p in dir_names],
        'uri': [f'{scan_root}/{p}' for p in dir_names],
    })
    rows = pd.concat([files, dirs], ignore_index=True)
    return IndexResult(df=aggregate(rows, scan_root=scan_root), error_count=0, error_paths=[])


def list_buckets(listings: tuple[str, ...], con: "duckdb.DuckDBPyConnection | None" = None) -> list[str]:
    """Distinct bucket names present in `listings`."""
    import duckdb as _duckdb
    if con is None:
        con = _duckdb.connect()
    src = prepare_listing(con, listings)
    return [r[0] for r in con.execute(f"SELECT DISTINCT bucket FROM {src} ORDER BY bucket").fetchall()]
