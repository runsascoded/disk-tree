"""Tests for the DuckDB out-of-core aggregation path (spec Item B).

Parity target: byte-identical output to `disk_tree.find.index.aggregate`
(pandas) on the same input, and to `import_listing` on the same listing.
"""

import datetime as dt
import os
import subprocess
import sqlite3
import sys
import tempfile
from os.path import dirname
from pathlib import Path

import duckdb
import pandas as pd
import pytest

from disk_tree.find import aggregate as aggregate_pandas
from disk_tree.find.aggregate_duckdb import aggregate_duckdb, aggregate_listing_to_parquet
from disk_tree.find.import_listing import import_listing
from disk_tree.listing import prepare_listing


TS = dt.datetime(2026, 7, 28, tzinfo=dt.timezone.utc)
NUMERIC = ['size', 'mtime', 'n_desc', 'n_files', 'n_children', 'depth']
COLS = ['path', 'size', 'mtime', 'kind', 'parent', 'uri', 'n_desc', 'n_files', 'n_children', 'depth']


def _normalize(df: pd.DataFrame) -> pd.DataFrame:
    """Coerce to matching dtypes + row order for equality assertion."""
    d = df[COLS].astype({c: 'int64' for c in NUMERIC}).copy()
    return d.sort_values(['depth', 'path']).reset_index(drop=True)


def _build_inputs(rows: list[tuple[str, int]], scan_root: str) -> pd.DataFrame:
    """Compose the (files + synthesized dirs) input frame the way `import_listing` does."""
    files = pd.DataFrame({
        'path': [n for n, _ in rows],
        'size': [s for _, s in rows],
        'mtime': [int(TS.timestamp())] * len(rows),
        'kind': 'file',
        'parent': [dirname(n) for n, _ in rows],
        'uri': [f'{scan_root}/{n}' for n, _ in rows],
    })
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
    return pd.concat([files, dirs], ignore_index=True)


# ---------- Byte-parity: pandas aggregate() vs. aggregate_duckdb() ----------

@pytest.mark.parametrize('rows', [
    # Small nested tree with a sibling top-level dir
    [('a.txt', 100), ('sub/b.txt', 200), ('sub/c.txt', 300), ('sub/deep/d.txt', 400), ('other/e.txt', 50)],
    # Single-file bucket
    [('lonely.bin', 42)],
    # All at root
    [('a', 1), ('b', 2), ('c', 3), ('d', 4)],
    # Deep single chain
    [('x/y/z/w/leaf.txt', 999)],
])
def test_parity_pandas_vs_duckdb(rows):
    scan_root = 'gcs://b1'
    inputs = _build_inputs(rows, scan_root)
    got_pandas = _normalize(aggregate_pandas(inputs.copy(), scan_root=scan_root))
    got_duckdb = _normalize(aggregate_duckdb(inputs.copy(), scan_root=scan_root))
    pd.testing.assert_frame_equal(got_pandas, got_duckdb)


# ---------- Standing cross-engine identity check (mgu's ask post-a2a) ----------
#
# The two engines' contract is byte-identical layer-2 output. mgu's 2026-08-14
# west4 a2a caught TWO drifts (`n_desc` semantics, `//` path components) that
# looked like production-vs-DT disagreements but actually would have surfaced
# as engine-vs-engine drift with the right fixture. This fixture is that
# fixture — every edge pattern that showed up in real listings, checked in so
# regressions get caught in CI, not on the next 588M-row a2a.
#
# When adding a new edge case: add it here first. If both engines still agree,
# ship. If they disagree, treat as a bug in whichever one drifted from spec.

_IDENTITY_LISTING = [
    # Normal case: nested files with siblings
    ('data/train/a.txt', 1024),
    ('data/train/b.txt', 2048),
    ('data/val/c.txt',   512),
    ('data/test/d.bin',  8192),
    # Sibling top-level dirs
    ('logs/2026-01-01.log', 4096),
    ('logs/2026-01-02.log', 4096),
    # Root-level file (parent = '')
    ('README.md', 300),
    ('LICENSE',   100),
    # Deep single-child chain
    ('deep/very/nested/leaf.txt', 42),
    # `//` empty-component path — real marin regression
    # (tokenized/finemath_3_plus-a26b0f//.artifact.json in production)
    ('tokenized/finemath//artifact.json', 4),
    ('tokenized/starcoder//artifact.json', 4),
    ('tokenized/normal/regular.txt', 1000),
    # Unicode paths (bucket keys can be arbitrary UTF-8)
    ('café/résumé.pdf', 500),
    ('日本語/データ.csv', 700),
    # Files with dots and no extension
    ('.hidden', 10),
    ('.git/config', 20),
    # Big single dir with many children (nudges the fold + groupby edges)
    *[(f'many/f{i:03d}.dat', i) for i in range(20)],
    # Zero-size file (real: placeholder objects)
    ('placeholder/marker', 0),
    # Single-file top-level dir (leaf-like)
    ('single/only.txt', 999),
]


def test_cross_engine_identity_on_real_edge_cases(tmp_path: Path):
    """Real-world edge patterns must produce byte-identical output from both
    engines. Standing identity check — any drift here is a bug."""
    listing = tmp_path / 'identity-listing.parquet'
    pd.DataFrame({
        'bucket': ['b1'] * len(_IDENTITY_LISTING),
        'name': [n for n, _ in _IDENTITY_LISTING],
        'size_bytes': [s for _, s in _IDENTITY_LISTING],
        'created': [TS] * len(_IDENTITY_LISTING),
        'storage_class_id': [1] * len(_IDENTITY_LISTING),
    }).to_parquet(listing)

    # Pandas engine: import_listing (in-memory).
    got_pandas = _normalize(import_listing((str(listing),), bucket='b1', scheme='gcs').df)

    # DuckDB engine: aggregate_listing_to_parquet (out-of-core).
    con = duckdb.connect()
    out = str(tmp_path / 'ooc.parquet')
    aggregate_listing_to_parquet(
        prepare_listing(con, (str(listing),)),
        bucket='b1', scheme='gcs', out_parquet=out, con=con,
    )
    got_duckdb = _normalize(pd.read_parquet(out))

    pd.testing.assert_frame_equal(got_pandas, got_duckdb)

    # Additional invariants that would be silently masked by identity alone:
    root_p = got_pandas[got_pandas.path == '.'].iloc[0]
    total_size = sum(s for _, s in _IDENTITY_LISTING)
    assert int(root_p['size']) == total_size, \
        f"root size drift: got {int(root_p['size'])}, expected {total_size}"
    assert int(root_p['n_files']) == len(_IDENTITY_LISTING), \
        f"root n_files drift: got {int(root_p['n_files'])}, expected {len(_IDENTITY_LISTING)}"
    # `//` file's bytes stay in `tokenized` (regression-locked)
    tok = got_pandas[got_pandas.path == 'tokenized'].iloc[0]
    assert int(tok['size']) == 1008  # 4 + 4 + 1000
    assert int(tok['n_files']) == 3


# ---------- True out-of-core path (parquet → parquet) matches import_listing ----------

@pytest.fixture
def listing_parquet(tmp_path: Path):
    listing = tmp_path / 'l.parquet'
    pd.DataFrame({
        'bucket': ['b1'] * 5,
        'name': ['a.txt', 'sub/b.txt', 'sub/c.txt', 'sub/deep/d.txt', 'other/e.txt'],
        'size_bytes': [100, 200, 300, 400, 50],
        'created': [TS] * 5,
        'storage_class_id': [1] * 5,
    }).to_parquet(listing)
    return str(listing)


def test_out_of_core_matches_import_listing(listing_parquet, tmp_path: Path):
    """aggregate_listing_to_parquet output equals import_listing (pandas) output."""
    con = duckdb.connect()
    out = str(tmp_path / 'out.parquet')
    stats = aggregate_listing_to_parquet(
        prepare_listing(con, (listing_parquet,)),
        bucket='b1', scheme='gcs', out_parquet=out, con=con,
    )
    got_ooc = _normalize(pd.read_parquet(out))
    got_ref = _normalize(import_listing((listing_parquet,), bucket='b1', scheme='gcs').df)
    pd.testing.assert_frame_equal(got_ooc, got_ref)
    # Root stats returned by aggregate_listing_to_parquet match the frame's root row
    root = got_ref[got_ref['path'] == '.'].iloc[0]
    assert stats['root_size'] == int(root['size'])
    assert stats['root_n_desc'] == int(root['n_desc'])
    assert stats['root_n_files'] == int(root['n_files'])
    assert stats['root_n_children'] == int(root['n_children'])
    assert stats['rows'] == len(got_ref)
    assert stats['files'] == 5


def test_out_of_core_missing_bucket_raises(listing_parquet, tmp_path: Path):
    con = duckdb.connect()
    with pytest.raises(ValueError, match="no rows for bucket 'nope'"):
        aggregate_listing_to_parquet(
            prepare_listing(con, (listing_parquet,)),
            bucket='nope', scheme='gcs',
            out_parquet=str(tmp_path / 'out.parquet'), con=con,
        )


def test_out_of_core_collapses_double_slashes(tmp_path: Path):
    """DuckDB parity for the `//` fix (see test_import.test_import_listing_collapses_double_slashes).
    Real marin regression: `regexp_extract` fails to match trailing-`/` paths →
    the intermediate dir gets parent='' → its bytes hop to the tree root."""
    listing = tmp_path / 'l.parquet'
    pd.DataFrame({
        'bucket': ['b1'] * 3,
        'name': ['tokenized/a.txt', 'tokenized/sub//x.txt', 'other/b.txt'],
        'size_bytes': [100, 4, 50],
        'created': [TS] * 3,
        'storage_class_id': [1] * 3,
    }).to_parquet(listing)
    con = duckdb.connect()
    stats = aggregate_listing_to_parquet(
        prepare_listing(con, (str(listing),)),
        bucket='b1', scheme='gcs',
        out_parquet=str(tmp_path / 'out.parquet'), con=con,
    )
    df = pd.read_parquet(tmp_path / 'out.parquet')

    # Bytes conserved.
    assert stats['root_size'] == 154

    # `//` file's bytes stay in tokenized (not hoisted to root).
    tok = df[df.path == 'tokenized'].iloc[0]
    assert int(tok['size']) == 104

    other = df[df.path == 'other'].iloc[0]
    assert int(other['size']) == 50

    # Canonical single-slash path in the output.
    assert 'tokenized/sub//x.txt' not in df.path.tolist()
    assert 'tokenized/sub/x.txt' in df.path.tolist()

    # And a proper `tokenized/sub` dir row (parent = tokenized, not empty).
    sub = df[df.path == 'tokenized/sub'].iloc[0]
    assert int(sub['size']) == 4
    assert sub['parent'] == 'tokenized'


# ---------- Scale smoke: 50k rows through the OOC path, tight memory cap ----------

def test_out_of_core_scale(tmp_path: Path):
    """50k rows aggregate under a 512MB duckdb cap (would spill if needed)."""
    listing = tmp_path / 'l.parquet'
    # 200 top-level dirs × 250 files each, varying depth 1-3
    names = []
    for d in range(200):
        for f in range(250):
            depth = (d + f) % 3
            if depth == 0:
                names.append(f'top{d}/file{f}.bin')
            elif depth == 1:
                names.append(f'top{d}/mid{f % 10}/file{f}.bin')
            else:
                names.append(f'top{d}/mid{f % 10}/deep{f % 5}/file{f}.bin')
    pd.DataFrame({
        'bucket': ['b1'] * len(names),
        'name': names,
        'size_bytes': [i % 10000 for i in range(len(names))],
        'created': [TS] * len(names),
        'storage_class_id': [1] * len(names),
    }).to_parquet(listing)

    con = duckdb.connect()
    temp_dir = tmp_path / 'ddb-tmp'
    temp_dir.mkdir()
    stats = aggregate_listing_to_parquet(
        prepare_listing(con, (str(listing),)),
        bucket='b1', scheme='gcs',
        out_parquet=str(tmp_path / 'out.parquet'),
        con=con, memory_limit='512MB', temp_dir=str(temp_dir),
    )
    assert stats['files'] == len(names)
    # root n_desc = 1 (self) + all files + all synthesized dir rows
    df = pd.read_parquet(tmp_path / 'out.parquet')
    n_dirs = int((df['kind'] == 'dir').sum())
    assert stats['root_n_desc'] == n_dirs + stats['files']  # includes self via n_dirs count


# ---------- CLI --engine duckdb creates a scan row (subprocess isolation) ----------

def test_cli_engine_duckdb_creates_scan(tmp_path: Path):
    listing = tmp_path / 'listing.parquet'
    pd.DataFrame({
        'bucket': ['b1', 'b1'],
        'name': ['a.txt', 'sub/b.txt'],
        'size_bytes': [100, 200],
        'created': [TS, TS],
        'storage_class_id': [1, 1],
    }).to_parquet(listing)
    root = tmp_path / 'dt-root'
    env = {**os.environ, 'DISK_TREE_ROOT': str(root)}
    r = subprocess.run(
        [sys.executable, '-m', 'disk_tree.cli.main', 'import',
         '-e', 'duckdb', '-l', str(listing), '-b', 'b1', '-t', TS.isoformat()],
        env=env, capture_output=True, text=True, check=False,
    )
    assert r.returncode == 0, f"stdout={r.stdout!r} stderr={r.stderr!r}"
    conn = sqlite3.connect(root / 'disk-tree.db')
    rows = conn.execute("SELECT path, size, n_children, n_desc FROM scan").fetchall()
    conn.close()
    # Matches the pandas-engine test's expected shape (byte-parity is enforced elsewhere)
    assert rows == [('gcs://b1', 300, 2, 4)]
