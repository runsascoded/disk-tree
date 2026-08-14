"""Phase-0 end-to-end: listing parquet → import_listing → /api/compare."""

import datetime as dt
import os
import shutil
import sqlite3
import subprocess
import sys
import tempfile
from pathlib import Path

import pandas as pd
import pytest

from disk_tree.find.import_listing import import_listing, list_buckets
from disk_tree.server import app, clear_cache

TS_A = dt.datetime(2026, 7, 1, tzinfo=dt.timezone.utc)
TS_B = dt.datetime(2026, 7, 28, tzinfo=dt.timezone.utc)


def write_listing(path: Path, rows: list[dict]) -> str:
    """Write a raw object-listing parquet (layer-1 schema)."""
    pd.DataFrame(rows).to_parquet(path)
    return str(path)


# ---------- Unit tests: import_listing produces canonical layer-2 rows ----------

def test_import_listing_shape(tmp_path: Path):
    """The layer-2 frame has synthesized dir rows, correct n_desc / n_children / depth."""
    listing = write_listing(tmp_path / "l.parquet", [
        {'bucket': 'b1', 'name': 'a.txt', 'size_bytes': 100, 'created': TS_A, 'storage_class_id': 1},
        {'bucket': 'b1', 'name': 'sub/b.txt', 'size_bytes': 200, 'created': TS_A, 'storage_class_id': 1},
        {'bucket': 'b1', 'name': 'sub/c.txt', 'size_bytes': 300, 'created': TS_A, 'storage_class_id': 1},
        {'bucket': 'b1', 'name': 'sub/deep/d.txt', 'size_bytes': 400, 'created': TS_A, 'storage_class_id': 1},
    ])
    df = import_listing((listing,), bucket='b1', scheme='gcs').df

    # by-path dict for order-insensitive assertion (aggregation ordering is well-defined
    # but the important invariant is per-row)
    got = {r['path']: (r['size'], r['kind'], r['parent'], r['uri'], r['n_desc'], r['n_files'], r['n_children'], r['depth'])
           for _, r in df.iterrows()}
    # n_desc for dirs includes self + all descendant dirs (walk-backend semantics:
    # gfind emits `path='' kind='dir'` for the scan root, so root/sub/sub/deep all
    # count self). `n_files` is objects-only, i.e. the count consumers expect from
    # an S3/GCS bucket.
    assert got == {
        '.':               (1000, 'dir',  '',         'gcs://b1',                7, 4, 2, 0),
        'a.txt':           (100,  'file', '',         'gcs://b1/a.txt',          1, 1, 0, 1),
        'sub':             (900,  'dir',  '.',        'gcs://b1/sub',            5, 3, 3, 1),
        'sub/b.txt':       (200,  'file', 'sub',      'gcs://b1/sub/b.txt',      1, 1, 0, 2),
        'sub/c.txt':       (300,  'file', 'sub',      'gcs://b1/sub/c.txt',      1, 1, 0, 2),
        'sub/deep':        (400,  'dir',  'sub',      'gcs://b1/sub/deep',       2, 1, 1, 2),
        'sub/deep/d.txt':  (400,  'file', 'sub/deep', 'gcs://b1/sub/deep/d.txt', 1, 1, 0, 3),
    }


def test_import_listing_bucket_filter(tmp_path: Path):
    """`bucket=` filters; other buckets don't contribute to the scan."""
    listing = write_listing(tmp_path / "l.parquet", [
        {'bucket': 'b1', 'name': 'x.txt', 'size_bytes': 10, 'created': TS_A, 'storage_class_id': 1},
        {'bucket': 'b2', 'name': 'y.txt', 'size_bytes': 20, 'created': TS_A, 'storage_class_id': 1},
    ])
    df = import_listing((listing,), bucket='b1', scheme='gcs').df
    assert sorted(df['path'].tolist()) == ['.', 'x.txt']
    root = df[df.path == '.'].iloc[0]
    assert root['size'] == 10 and root['n_desc'] == 2  # self (dir) + 1 file


def test_import_listing_missing_bucket_raises(tmp_path: Path):
    listing = write_listing(tmp_path / "l.parquet", [
        {'bucket': 'b1', 'name': 'x.txt', 'size_bytes': 10, 'created': TS_A, 'storage_class_id': 1},
    ])
    with pytest.raises(ValueError, match="no rows for bucket 'nope'"):
        import_listing((listing,), bucket='nope', scheme='gcs')


def test_list_buckets_distinct(tmp_path: Path):
    listing = write_listing(tmp_path / "l.parquet", [
        {'bucket': 'b1', 'name': 'x', 'size_bytes': 1, 'created': TS_A, 'storage_class_id': 1},
        {'bucket': 'b2', 'name': 'y', 'size_bytes': 2, 'created': TS_A, 'storage_class_id': 1},
        {'bucket': 'b1', 'name': 'z', 'size_bytes': 3, 'created': TS_A, 'storage_class_id': 1},
    ])
    assert list_buckets((listing,)) == ['b1', 'b2']


def test_import_listing_collapses_double_slashes(tmp_path: Path):
    """Keys with empty path components (`a//b`) must stay in their real
    subtree — not get hoisted to the tree root by trailing-slash-borked
    parent-walking (real marin regression:
    `tokenized/finemath_3_plus-a26b0f//.artifact.json` moved bytes across
    top-level subtrees; see specs/import-a2a-findings.md item 2)."""
    listing = write_listing(tmp_path / "l.parquet", [
        {'bucket': 'b1', 'name': 'tokenized/a.txt',      'size_bytes': 100, 'created': TS_A, 'storage_class_id': 1},
        {'bucket': 'b1', 'name': 'tokenized/sub//x.txt', 'size_bytes':   4, 'created': TS_A, 'storage_class_id': 1},
        {'bucket': 'b1', 'name': 'other/b.txt',          'size_bytes':  50, 'created': TS_A, 'storage_class_id': 1},
    ])
    df = import_listing((listing,), bucket='b1', scheme='gcs').df

    # Root sum unchanged (bytes-conserving).
    root = df[df.path == '.'].iloc[0]
    assert int(root['size']) == 154

    # The `//` file's bytes stay inside the `tokenized` subtree.
    tok = df[df.path == 'tokenized'].iloc[0]
    assert int(tok['size']) == 104  # 100 + 4

    # `other` is unaffected.
    other = df[df.path == 'other'].iloc[0]
    assert int(other['size']) == 50

    # File row uses the canonicalized (single-slash) path — no `//` survives.
    assert 'tokenized/sub//x.txt' not in df.path.tolist()
    assert 'tokenized/sub/x.txt' in df.path.tolist()

    # And a `tokenized/sub` dir row exists (properly synthesized, not orphaned).
    sub = df[df.path == 'tokenized/sub'].iloc[0]
    assert int(sub['size']) == 4
    assert sub['parent'] == 'tokenized'


# ---------- End-to-end: two imports → /api/compare shows the delta ----------

def _init_scans_db(db_path: str) -> None:
    """Create the scan schema used by disk_tree.server (mirrors test_server.py)."""
    conn = sqlite3.connect(db_path)
    conn.executescript('''
        CREATE TABLE scan (
            id INTEGER NOT NULL PRIMARY KEY,
            path VARCHAR NOT NULL,
            time DATETIME NOT NULL,
            blob VARCHAR NOT NULL,
            error_count INTEGER,
            error_paths TEXT,
            size INTEGER,
            n_children INTEGER,
            n_desc INTEGER,
            mtime INTEGER
        );
        CREATE INDEX ix_scan_path_time ON scan(path, time);
    ''')
    conn.commit()
    conn.close()


def _insert_scan(db_path: str, scan_path: str, when: dt.datetime, blob: str, root_stats: dict) -> int:
    conn = sqlite3.connect(db_path)
    cur = conn.execute(
        'INSERT INTO scan (path, time, blob, size, n_children, n_desc, mtime) VALUES (?, ?, ?, ?, ?, ?, ?)',
        (scan_path, when.replace(tzinfo=None).isoformat(), blob,
         root_stats['size'], root_stats['n_children'], root_stats['n_desc'], root_stats['mtime']),
    )
    conn.commit()
    scan_id = cur.lastrowid
    conn.close()
    return scan_id


@pytest.fixture
def compare_env(monkeypatch, tmp_path: Path):
    """Isolated DB + scans dir, hooked into the Flask app + parquet backend."""
    db_path = str(tmp_path / 'disk-tree.db')
    scans_dir = str(tmp_path / 'scans')
    os.makedirs(scans_dir)
    _init_scans_db(db_path)

    monkeypatch.setattr('disk_tree.server.DB_PATH', db_path)
    monkeypatch.setattr('disk_tree.config.SCANS_DIR', scans_dir)
    monkeypatch.setenv('DISK_TREE_BACKEND', 'parquet')

    from disk_tree.storage import reset_backend
    reset_backend()
    clear_cache()

    app.config['TESTING'] = True
    with app.test_client() as client:
        yield {'client': client, 'db_path': db_path, 'scans_dir': scans_dir, 'listings_dir': tmp_path}
    reset_backend()


def _import_snapshot(env: dict, listing_rows: list[dict], when: dt.datetime, tag: str) -> int:
    """Write a listing, import it as bucket b1, insert a scan row; return scan id."""
    from disk_tree.storage import get_backend
    listing = write_listing(env['listings_dir'] / f'listing-{tag}.parquet', listing_rows)
    df = import_listing((listing,), bucket='b1', scheme='gcs').df
    backend = get_backend()
    blob = backend.save(df, 'gcs://b1')
    root = df[df.path == '.'].iloc[0]
    return _insert_scan(
        env['db_path'], 'gcs://b1', when, blob,
        {'size': int(root['size']), 'n_children': int(root['n_children']),
         'n_desc': int(root['n_desc']), 'mtime': int(root['mtime'])},
    )


def test_two_imports_diff_via_api_compare(compare_env):
    """Import two listing snapshots; /api/compare surfaces added/removed/changed."""
    # Snapshot A: a.txt, sub/b.txt, sub/c.txt
    id_a = _import_snapshot(compare_env, [
        {'bucket': 'b1', 'name': 'a.txt',     'size_bytes': 100, 'created': TS_A, 'storage_class_id': 1},
        {'bucket': 'b1', 'name': 'sub/b.txt', 'size_bytes': 200, 'created': TS_A, 'storage_class_id': 1},
        {'bucket': 'b1', 'name': 'sub/c.txt', 'size_bytes': 300, 'created': TS_A, 'storage_class_id': 1},
    ], TS_A, 'a')
    # Snapshot B: a.txt grew, sub/c.txt gone, sub/d.txt added
    id_b = _import_snapshot(compare_env, [
        {'bucket': 'b1', 'name': 'a.txt',     'size_bytes': 500, 'created': TS_B, 'storage_class_id': 1},
        {'bucket': 'b1', 'name': 'sub/b.txt', 'size_bytes': 200, 'created': TS_A, 'storage_class_id': 1},
        {'bucket': 'b1', 'name': 'sub/d.txt', 'size_bytes': 700, 'created': TS_B, 'storage_class_id': 1},
    ], TS_B, 'b')

    resp = compare_env['client'].get(f'/api/compare?uri=gcs://b1&scan1={id_a}&scan2={id_b}')
    assert resp.status_code == 200, resp.data
    data = resp.json

    # Root-level rows (a.txt as file, sub as dir): both are "changed"; nothing at root-level is added/removed
    by_path = {r['path']: r for r in data['rows']}
    assert set(by_path) == {'a.txt', 'sub'}
    assert by_path['a.txt']['size_delta'] == 400   # 100 → 500
    assert by_path['a.txt']['status'] == 'changed'
    # sub: b unchanged, c removed, d added → size 500 → 900, +400
    assert by_path['sub']['size_delta'] == 400
    assert by_path['sub']['status'] == 'changed'

    # Drill into `sub`: c.txt removed, d.txt added, b.txt unchanged
    resp2 = compare_env['client'].get(f'/api/compare?uri=gcs://b1/sub&scan1={id_a}&scan2={id_b}')
    assert resp2.status_code == 200, resp2.data
    sub_rows = {r['path']: r for r in resp2.json['rows']}
    assert sub_rows['c.txt']['status'] == 'removed'
    assert sub_rows['d.txt']['status'] == 'added'
    # b.txt is unchanged, so /api/compare may or may not include it depending on filter — assert on what changed
    assert sub_rows['c.txt']['size_delta'] == -300
    assert sub_rows['d.txt']['size_delta'] == 700


# ---------- CLI smoke test (subprocess to sidestep the sqla singleton) ----------

def test_import_cli_creates_scan(tmp_path: Path):
    """`disk-tree import -l <listing>` creates a scan row in an isolated DISK_TREE_ROOT."""
    listing = write_listing(tmp_path / 'listing.parquet', [
        {'bucket': 'b1', 'name': 'a.txt', 'size_bytes': 100, 'created': TS_A, 'storage_class_id': 1},
        {'bucket': 'b1', 'name': 'sub/b.txt', 'size_bytes': 200, 'created': TS_A, 'storage_class_id': 1},
    ])
    root = tmp_path / 'dt-root'
    env = {**os.environ, 'DISK_TREE_ROOT': str(root)}
    r = subprocess.run(
        [sys.executable, '-m', 'disk_tree.cli.main', 'import',
         '-l', listing, '-b', 'b1', '-t', TS_A.isoformat()],
        env=env, capture_output=True, text=True, check=False,
    )
    assert r.returncode == 0, f"stdout={r.stdout!r} stderr={r.stderr!r}"

    # Scan row landed
    conn = sqlite3.connect(root / 'disk-tree.db')
    rows = conn.execute("SELECT path, size, n_children, n_desc FROM scan").fetchall()
    conn.close()
    # size=300 (100 + 200), n_children=2 (a.txt + sub dir), n_desc=4 (self + a.txt + sub + b.txt)
    assert rows == [('gcs://b1', 300, 2, 4)]
