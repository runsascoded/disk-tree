"""Vocabulary sidecar + name→row-group block index (spec: diff-and-search.md, index tiers 1–2).

A sidecar is a second parquet next to a scan's layer-2 blob:

    <blob-stem>.vocab.parquet
    name        str          distinct basename (path segment), sorted
    n_dirs      int64        rows with this basename that are dirs
    n_files     int64        rows with this basename that are files
    row_groups  list<int32>  layer-2 row-group ordinals containing the name

`name` is *basenames only* — every directory is itself a layer-2 row, so the
dir-segment vocabulary is exactly the basenames of dir rows; no path needs
re-segmenting. Sorted `name` + bounded row groups make the artifact
range-prunable for static consumers (DuckDB-wasm / HTTP range reads); the
server and CLI use it to answer segment-local filter queries by scanning the
vocab (mgu probe: 38× fewer strings than rows) and reading only the candidate
row groups of the layer-2 blob.

Freshness: `/api/delete` rewrites scan parquets in place, so a sidecar older
than its blob is stale and must be ignored (`sidecar_is_fresh`). Blobs whose
rows reference chunk scans (`child_scan_id`) are refused at build time — the
index cannot see inside chunks, and silently missing matches is worse than no
index.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from os.path import exists, splitext
from typing import Callable

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from disk_tree.filter import basenames, parse_query, query_mode, rebase_frame

SIDECAR_SUFFIX = '.vocab.parquet'
# Bounded so name-sorted min/max stats let readers (including static ones)
# prune to the row groups covering a query's name range.
VOCAB_ROW_GROUP_SIZE = 65_536


def sidecar_path_for(blob_path: str) -> str:
    stem, ext = splitext(blob_path)
    return (stem if ext == '.parquet' else blob_path) + SIDECAR_SUFFIX


def sidecar_is_fresh(blob_path: str, sc_path: str | None = None) -> bool:
    """A usable sidecar exists: present, and not older than its blob."""
    sc_path = sc_path or sidecar_path_for(blob_path)
    return exists(sc_path) and os.path.getmtime(sc_path) >= os.path.getmtime(blob_path)


@dataclass
class SidecarStats:
    path: str
    n_names: int
    n_rows: int          # layer-2 rows covered
    n_row_groups: int    # layer-2 row groups
    size_bytes: int


def build_vocab_sidecar(blob_path: str, out_path: str | None = None, force: bool = False) -> SidecarStats:
    """Build (or rebuild) the vocab sidecar for a layer-2 parquet blob.

    Streams the blob row-group-by-row-group, so peak memory is one row group
    of paths plus the accumulating (name, rg) aggregate — not the whole scan.
    """
    out_path = out_path or sidecar_path_for(blob_path)
    if not force and sidecar_is_fresh(blob_path, out_path):
        st = os.stat(out_path)
        pf = pq.ParquetFile(out_path)
        blob_pf = pq.ParquetFile(blob_path)
        return SidecarStats(
            path=out_path,
            n_names=pf.metadata.num_rows,
            n_rows=blob_pf.metadata.num_rows,
            n_row_groups=blob_pf.num_row_groups,
            size_bytes=st.st_size,
        )

    pf = pq.ParquetFile(blob_path)
    names_set = set(pf.schema_arrow.names)
    ref_col = 'child_scan_id' if 'child_scan_id' in names_set else None
    cols = ['path', 'kind'] + ([ref_col] if ref_col else [])

    parts: list[pd.DataFrame] = []
    n_rows = 0
    for rg in range(pf.num_row_groups):
        tbl = pf.read_row_group(rg, columns=cols)
        df = tbl.to_pandas()
        if ref_col and df[ref_col].notna().any():
            raise ValueError(
                f"{blob_path}: rows reference chunk scans ({ref_col}); a vocab "
                "sidecar over the root blob would silently miss matches inside "
                "chunks — not building one"
            )
        n_rows += len(df)
        df = df[df['path'] != '.']
        if df.empty:
            continue
        part = (
            pd.DataFrame({
                'name': basenames(df['path']),
                'is_dir': df['kind'] == 'dir',
            })
            .groupby('name', sort=False)['is_dir']
            .agg(n_dirs='sum', n='size')
            .reset_index()
        )
        part['rg'] = rg
        parts.append(part)

    if parts:
        allp = pd.concat(parts, ignore_index=True)
        vocab = (
            allp.groupby('name', sort=True)
            .agg(n_dirs=('n_dirs', 'sum'), n=('n', 'sum'), row_groups=('rg', list))
            .reset_index()
        )
        vocab['n_files'] = vocab['n'] - vocab['n_dirs']
    else:
        vocab = pd.DataFrame({'name': [], 'n_dirs': [], 'n_files': [], 'row_groups': []})

    schema = pa.schema([
        ('name', pa.string()),
        ('n_dirs', pa.int64()),
        ('n_files', pa.int64()),
        ('row_groups', pa.list_(pa.int32())),
    ])
    table = pa.Table.from_pydict(
        {
            'name': vocab['name'].tolist(),
            'n_dirs': vocab['n_dirs'].astype('int64').tolist() if len(vocab) else [],
            'n_files': vocab['n_files'].astype('int64').tolist() if len(vocab) else [],
            'row_groups': vocab['row_groups'].tolist(),
        },
        schema=schema,
    )
    pq.write_table(table, out_path, row_group_size=VOCAB_ROW_GROUP_SIZE)
    return SidecarStats(
        path=out_path,
        n_names=len(vocab),
        n_rows=n_rows,
        n_row_groups=pf.num_row_groups,
        size_bytes=os.path.getsize(out_path),
    )


def candidate_row_groups(
    sc_path: str,
    predicate: Callable[[pd.Series], pd.Series],
) -> tuple[list[int], int]:
    """Match the query predicate over the vocab; return (sorted row-group
    ordinals to read from the layer-2 blob, number of matched names)."""
    tbl = pq.read_table(sc_path, columns=['name', 'row_groups'])
    names = tbl['name'].to_pandas()
    mask = predicate(names).to_numpy()
    n_matched = int(mask.sum())
    if not n_matched:
        return [], 0
    lists = tbl['row_groups'].to_pandas()[mask]
    rgs = np.unique(np.concatenate([np.asarray(x) for x in lists]))
    return [int(r) for r in rgs], n_matched


def load_matched_rows(
    blob_path: str,
    predicate: Callable[[pd.Series], pd.Series],
    sc_path: str | None = None,
    columns: list[str] | None = None,
) -> pd.DataFrame | None:
    """Indexed fast path: rows of the blob whose *basename* matches.

    Returns None when no usable sidecar exists (caller falls back to brute).
    An exact basename re-match runs after the row-group read — candidate row
    groups contain non-matching rows too, and index fidelity is best-effort by
    contract everywhere in this codebase.
    """
    sc_path = sc_path or sidecar_path_for(blob_path)
    if not sidecar_is_fresh(blob_path, sc_path):
        return None
    rgs, n_names = candidate_row_groups(sc_path, predicate)
    cols = columns or ['path', 'size', 'kind']
    if not rgs:
        # Explicit str dtype: an all-float empty frame breaks `.str` accessors
        # downstream (rebase_frame, iter_filter_scan's depth derivation).
        return pd.DataFrame({c: pd.Series([], dtype=str if c in ('path', 'kind') else float) for c in cols})
    pf = pq.ParquetFile(blob_path)
    df = pf.read_row_groups(rgs, columns=cols).to_pandas()
    df = df[df['path'] != '.']
    return df[predicate(basenames(df['path']))]


def indexed_filter_frame(
    blob_path: str,
    query: str,
    case_sensitive: bool = False,
    rel_path: str = '.',
) -> pd.DataFrame | None:
    """The indexed fast path's front door: the frame of matched rows, rebased
    to `rel_path`, ready for `filter_scan(frame, '')` / `iter_filter_scan(frame, '')`
    (an empty query matches every row, so the existing dedup + rollup + SSE
    machinery runs unchanged over just the matches).

    Returns None whenever the caller should brute-force instead: path-mode or
    empty query (the index is name-keyed), or no fresh sidecar.
    """
    q = query.strip()
    if not q or query_mode(query) != 'segment':
        return None
    df = load_matched_rows(blob_path, parse_query(query, case_sensitive=case_sensitive))
    if df is None:
        return None
    if rel_path not in ('.', ''):
        df = rebase_frame(df, rel_path)
    return df
