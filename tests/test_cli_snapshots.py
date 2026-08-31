"""Tests for `disk-tree snapshots` — publish scans as a static snapshot library
(the file-tree integration's no-live-Python tier, spec `file-tree-integration.md` B1)."""

from __future__ import annotations

import datetime as dt
import json
import os
import subprocess
import sys
from pathlib import Path

import pandas as pd
import pytest

TS = dt.datetime(2026, 7, 1, tzinfo=dt.timezone.utc)


def _run_dt(env_root: Path, *args: str) -> subprocess.CompletedProcess:
    env = {**os.environ, 'DISK_TREE_ROOT': str(env_root)}
    return subprocess.run(
        [sys.executable, '-m', 'disk_tree.cli.main', *args],
        env=env, capture_output=True, text=True, check=False,
    )


@pytest.fixture
def scanned(tmp_path: Path) -> Path:
    """One imported bucket `gcs://b1`: a.txt=100, sub/b.txt=200, sub/c.txt=300
    (root=600, sub=500). Rows: root, a.txt, sub/, sub/b.txt, sub/c.txt."""
    env_root = tmp_path / 'root'
    env_root.mkdir()
    listing = tmp_path / 'l.parquet'
    pd.DataFrame([
        {'bucket': 'b1', 'name': 'a.txt',     'size_bytes': 100, 'created': TS, 'storage_class_id': 1},
        {'bucket': 'b1', 'name': 'sub/b.txt', 'size_bytes': 200, 'created': TS, 'storage_class_id': 1},
        {'bucket': 'b1', 'name': 'sub/c.txt', 'size_bytes': 300, 'created': TS, 'storage_class_id': 1},
    ]).to_parquet(listing)
    r = _run_dt(env_root, 'import', '-l', str(listing), '-b', 'b1', '-t', TS.isoformat())
    assert r.returncode == 0, r.stderr
    return env_root


def test_publish_writes_index_and_self_contained_tree(scanned: Path, tmp_path: Path):
    dest = tmp_path / 'pub'
    r = _run_dt(scanned, 'snapshots', str(dest))
    assert r.returncode == 0, r.stderr

    manifest = json.loads((dest / 'snapshots.json').read_text())
    assert manifest['version'] == 1
    assert manifest['row_group_size'] == 65536
    assert manifest['columns'] == [
        'path', 'size', 'mtime', 'kind', 'parent', 'uri', 'n_desc', 'n_children', 'depth', 'mtime_mean?',
    ]
    assert len(manifest['snapshots']) == 1
    snap = manifest['snapshots'][0]
    assert {k: snap[k] for k in ('path', 'size', 'n_children', 'n_desc', 'tree')} == {
        'path': 'gcs://b1', 'size': 600, 'n_children': 2, 'n_desc': 5,
        'tree': f'snapshots/{snap["id"]}/tree.parquet',
    }

    tree = dest / 'snapshots' / str(snap['id']) / 'tree.parquet'
    df = pd.read_parquet(tree)
    # Only the public contract columns — internal `child_scan_id`/`n_files` projected out.
    assert list(df.columns) == ['path', 'size', 'mtime', 'kind', 'parent', 'uri', 'n_desc', 'n_children', 'depth']
    assert df['path'].is_unique
    # Materialized full subtree, sorted (depth, path). `path` is relative to the
    # scan root (root == '.'); `uri` carries the absolute path.
    assert list(zip(df['depth'], df['path'])) == [
        (0, '.'),
        (1, 'a.txt'),
        (1, 'sub'),
        (2, 'sub/b.txt'),
        (2, 'sub/c.txt'),
    ]
    assert df.loc[df['depth'] == 0, 'uri'].tolist() == ['gcs://b1']
    # Depth pushdown still prunes on the published copy.
    assert sorted(pd.read_parquet(tree, filters=[('depth', '==', 1)])['path']) == ['a.txt', 'sub']


def test_dry_run_writes_nothing(scanned: Path, tmp_path: Path):
    dest = tmp_path / 'pub'
    r = _run_dt(scanned, 'snapshots', '-n', str(dest))
    assert r.returncode == 0, r.stderr
    assert not dest.exists()


def test_unknown_scan_id_errors(scanned: Path, tmp_path: Path):
    r = _run_dt(scanned, 'snapshots', '-s', '999', str(tmp_path / 'pub'))
    assert r.returncode != 0
    assert 'no such scan id(s): [999]' in r.stderr


def test_cloud_dest_rejected(scanned: Path):
    r = _run_dt(scanned, 'snapshots', 's3://bucket/prefix')
    assert r.returncode != 0
    assert 'DEST must be a local dir' in r.stderr
