"""`disk-tree index --to`, end to end through the CLI (an `s3://` source listed
by a fake `aws`) over a `file://` target — a real fsspec URL that survives
across processes, unlike `memory://`. Spec `remote-scan-targets.md`, Phase 1.
Plus the refusal of a local path (this engine scans object stores only).
"""

from __future__ import annotations

import os
import re
import subprocess
import sys
from pathlib import Path

import pytest
from utz import iec

from disk_tree import config

pytest.importorskip('fsspec')

UUID_PARQUET = r'[0-9a-f-]{36}\.parquet'
SRC = 's3://bkt/src'
LISTING = (
    '2026-01-02 03:04:05          4 src/a.txt\n'
    '2026-01-02 03:04:06          2 src/sub/b.txt\n'
)


def _run(args: list[str], root: Path, **env_extra: str) -> subprocess.CompletedProcess:
    env = {**os.environ, config.DISK_TREE_ROOT_VAR: str(root)}
    env.pop(config.DISK_TREE_SCAN_DIRS_VAR, None)
    env.update(env_extra)
    return subprocess.run(
        [sys.executable, '-m', 'disk_tree.cli.main', *args],
        env=env, capture_output=True, text=True, check=False,
    )


def _stderr_lines(r: subprocess.CompletedProcess) -> list[str]:
    return r.stderr.rstrip('\n').split('\n')


@pytest.fixture
def aws_env(fake_aws) -> dict[str, str]:
    return fake_aws(LISTING)


def test_to_writes_the_blob_to_the_url_target_and_reads_it_back(aws_env: dict[str, str], tmp_path: Path):
    root, remote = tmp_path / 'root', tmp_path / 'remote'
    target = f'file://{remote}'
    r = _run(['index', '-C', '-t', target, SRC], root, **aws_env)
    assert r.returncode == 0, r.stderr
    assert _stderr_lines(r)[0] == f'--to: writing blobs to {target}'

    blobs = sorted(p.name for p in remote.glob('*.parquet'))
    assert len(blobs) == 1
    assert re.fullmatch(UUID_PARQUET, blobs[0])
    assert list((root / 'scans').glob('*.parquet')) == []
    assert [l for l in r.stdout.split('\n') if l.startswith('Scan cached path: ')] == [
        f'Scan cached path: {target}/{blobs[0]} ({iec(os.path.getsize(remote / blobs[0]))})',
    ]

    # A later process finds the blob through the search path — no `--to` needed.
    env = {**os.environ, config.DISK_TREE_ROOT_VAR: str(root), config.DISK_TREE_SCAN_DIRS_VAR: f'{target}:{root / "scans"}'}
    r2 = subprocess.run(
        [sys.executable, '-c', f'from disk_tree.resolve import resolve_blob; print(resolve_blob({blobs[0]!r}))'],
        env=env, capture_output=True, text=True, check=False,
    )
    assert r2.returncode == 0, r2.stderr
    assert r2.stdout == f'{target}/{blobs[0]}\n'


def test_to_writes_a_groups_json_footer_sidecar(aws_env: dict[str, str], tmp_path: Path):
    """`index --to <url>` precomputes the `.groups.json` footer beside the blob,
    so the serverless reader (`site/functions/_lib/index.ts`) plans range
    reads without a cold thrift-footer parse (`find/groups.py`). Absent-safe:
    a reader without it falls back to the blob's own footer."""
    import json

    import pyarrow.parquet as pq

    from disk_tree.find.groups import GROUP_FIELDS, groups_path

    root, remote = tmp_path / 'root', tmp_path / 'remote'
    target = f'file://{remote}'
    r = _run(['index', '-C', '-t', target, SRC], root, **aws_env)
    assert r.returncode == 0, r.stderr

    blob = next(remote.glob('*.parquet'))
    sidecar = Path(groups_path(str(blob)))
    assert sidecar.exists()
    doc = json.loads(sidecar.read_text())

    md = pq.read_metadata(str(blob))
    # One `groups` entry per row group, each an array in GROUP_FIELDS order; a
    # main blob carries no coarse floor; schema leaves are the blob's columns.
    assert [len(g) for g in doc['groups']] == [len(GROUP_FIELDS)] * md.num_row_groups
    assert (doc['v'], doc['floor_bytes']) == (1, None)
    assert [leaf['name'] for leaf in doc['schema'][1:]] == md.schema.names
    # The publish announced it on stdout (the URL path, matching the `--to` target).
    assert [l.split(' (')[0] for l in r.stdout.split('\n') if l.startswith('Scan groups: ')] == [
        f'Scan groups: {groups_path(f"{target}/{blob.name}")}',
    ]


def test_index_scans_the_listing(aws_env: dict[str, str], tmp_path: Path):
    """The blob is the aggregated listing: the root, `sub`, and both objects."""
    from disk_tree.blobfs import read_parquet

    root, remote = tmp_path / 'root', tmp_path / 'remote'
    r = _run(['index', '-C', '-q', '-t', f'file://{remote}', SRC], root, **aws_env)
    assert r.returncode == 0, r.stderr
    df = read_parquet(str(next(remote.glob('*.parquet'))))
    assert df[['path', 'kind', 'size', 'uri']].values.tolist() == [
        ['.', 'dir', 6, SRC],
        ['a.txt', 'file', 4, f'{SRC}/a.txt'],
        ['sub', 'dir', 2, f'{SRC}/sub'],
        ['sub/b.txt', 'file', 2, f'{SRC}/sub/b.txt'],
    ]


@pytest.mark.parametrize('url', ['/some/local/dir', 'file:///some/local/dir'])
def test_local_path_refuses(url: str, tmp_path: Path):
    r = _run(['index', '-C', url], tmp_path / 'root')
    assert r.returncode == 2
    assert _stderr_lines(r)[-1] == (
        "Error: live scanning of local paths isn't supported; index an `s3://` or `r2://` URL, "
        "or import a listing (`disk-tree bulk-list` + `disk-tree import -l <listing>`)"
    )
    assert not (tmp_path / 'root' / 'disk-tree.db').exists()  # refused before the DB opens
