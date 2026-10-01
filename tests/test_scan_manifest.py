"""`<blob>.scan.json`: `index --to <url>` writes the scan's row beside its remote
blob, so the metadata travels with the blob (spec `remote-scan-targets.md`).
"""

from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import pytest

from disk_tree import config
from disk_tree.scan_manifest import SUFFIX

pytest.importorskip('fsspec')


def _run(args: list[str], root: Path, **env_extra: str) -> subprocess.CompletedProcess:
    env = {**os.environ, config.DISK_TREE_ROOT_VAR: str(root)}
    env.pop(config.DISK_TREE_SCAN_DIRS_VAR, None)
    env.update(env_extra)
    return subprocess.run(
        [sys.executable, '-m', 'disk_tree.cli.main', *args],
        env=env, capture_output=True, text=True, check=False,
    )


def _ok(r: subprocess.CompletedProcess) -> subprocess.CompletedProcess:
    assert r.returncode == 0, r.stderr
    return r


def _scans(root: Path) -> list[dict]:
    con = sqlite3.connect(root / 'disk-tree.db')
    con.row_factory = sqlite3.Row
    try:
        return [dict(r) for r in con.execute('SELECT * FROM scan ORDER BY id')]
    finally:
        con.close()


SRC = 's3://bkt/src'


@pytest.fixture
def aws_env(fake_aws) -> dict[str, str]:
    return fake_aws(
        '2026-01-02 03:04:05       5000 src/a.txt\n'
        '2026-01-02 03:04:06        100 src/sub/b.txt\n'
    )


def test_index_to_url_writes_a_manifest(aws_env: dict[str, str], tmp_path: Path):
    root, blobs = tmp_path / 'root', f'file://{tmp_path / "blobs"}'
    r = _ok(_run(['index', '-C', '-q', '-t', blobs, SRC], root, **aws_env))
    (scan,) = _scans(root)
    manifest = f'{blobs}/{scan["blob"]}{SUFFIX}'
    assert [l for l in r.stdout.split('\n') if l.startswith('Scan manifest: ')] == [f'Scan manifest: {manifest}']
    m = json.loads((tmp_path / 'blobs' / f'{scan["blob"]}{SUFFIX}').read_text())
    # The row stores `time` as naive local wall clock; the manifest carries the
    # offset so a reader in another zone can place it.
    assert datetime.fromisoformat(m['time']).astimezone().replace(tzinfo=None) == datetime.fromisoformat(scan['time'])
    assert m == {
        'format': 'disk-tree-scan', 'version': 1,
        'time': m['time'],
        'path': SRC, 'blob': scan['blob'],
        'size': scan['size'], 'n_children': scan['n_children'], 'n_desc': scan['n_desc'],
        'mtime': scan['mtime'], 'error_count': scan['error_count'],
        'error_paths': json.loads(scan['error_paths']) if scan['error_paths'] else None,
    }


def test_index_to_local_dir_writes_no_manifest(aws_env: dict[str, str], tmp_path: Path):
    root, blobs = tmp_path / 'root', tmp_path / 'blobs'
    _ok(_run(['index', '-C', '-q', '-t', str(blobs), SRC], root, **aws_env))
    assert sorted(p.suffix for p in blobs.iterdir()) == ['.parquet']
