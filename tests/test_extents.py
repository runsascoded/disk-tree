"""Tests for physical-extent mapping (`disk_tree.extents`) and `disk-tree reclaim`.

APFS-specific: `fcntl(F_LOG2PHYS_EXT)` and `cp -c` (clonefile) have no portable
equivalent, so the whole module is Darwin-only.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from disk_tree.extents import SUPPORTED, Extent, file_extents, measure

pytestmark = pytest.mark.skipif(not SUPPORTED, reason='physical extent mapping is Darwin-only')

MIB = 1 << 20


def _write(path: Path, size: int = MIB) -> Path:
    """Incompressible bytes, so APFS stores them as real extents rather than
    inlining them into an xattr (decmpfs), which would leave nothing to map."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(os.urandom(size))
    return path


def _clone(src: Path, dst: Path) -> Path:
    dst.parent.mkdir(parents=True, exist_ok=True)
    subprocess.run(['/bin/cp', '-c', str(src), str(dst)], check=True)
    return dst


def _run_dt(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, '-m', 'disk_tree.cli.main', *args],
        capture_output=True, text=True, check=False,
    )


def test_file_extents_are_nonempty_and_cover_the_file(tmp_path: Path):
    f = _write(tmp_path / 'a.bin')
    ex = file_extents(str(f))
    assert ex, 'a 1 MiB incompressible file should map to at least one extent'
    assert sum(length for _, length in ex) == f.stat().st_size


def test_clone_shares_every_extent(tmp_path: Path):
    a = _write(tmp_path / 'a.bin')
    b = _clone(a, tmp_path / 'b.bin')
    assert file_extents(str(a)) == file_extents(str(b))
    # …and the clone is charged the full block count all the same, which is
    # exactly why scan sizes overstate reclaimable space.
    assert b.stat().st_blocks == a.stat().st_blocks


def test_independent_copy_shares_nothing(tmp_path: Path):
    a = _write(tmp_path / 'a.bin')
    b = tmp_path / 'b.bin'
    subprocess.run(['/bin/cp', str(a), str(b)], check=True)
    assert set(file_extents(str(a))) & set(file_extents(str(b))) == set()


def test_measure_discounts_bytes_the_partner_keeps(tmp_path: Path):
    cache, proj = tmp_path / 'cache', tmp_path / 'proj'
    src = _write(cache / 'pkg.bin')
    _clone(src, proj / 'pkg.bin')
    owned = _write(proj / 'own.bin')

    rec = measure([str(proj)], [str(cache)])
    assert [(s.path, s.n_files) for s in rec.subtrees] == [(str(proj), 2)]
    assert rec.apparent == src.stat().st_blocks * 512 + owned.stat().st_blocks * 512
    assert rec.shared == src.stat().st_size
    assert rec.unique == owned.stat().st_size


def test_measure_without_partners_counts_everything_as_unique(tmp_path: Path):
    cache, proj = tmp_path / 'cache', tmp_path / 'proj'
    src = _write(cache / 'pkg.bin')
    _clone(src, proj / 'pkg.bin')

    rec = measure([str(proj)], [])
    assert rec.shared == 0
    assert rec.unique == src.stat().st_size


def test_hardlinks_within_the_set_are_charged_once(tmp_path: Path):
    proj = tmp_path / 'proj'
    a = _write(proj / 'a.bin')
    os.link(a, proj / 'b.bin')

    rec = measure([str(proj)], [])
    assert [(s.n_files, s.apparent) for s in rec.subtrees] == [(1, a.stat().st_blocks * 512)]


def test_cli_reports_apparent_shared_and_frees(tmp_path: Path):
    cache, proj = tmp_path / 'cache', tmp_path / 'proj'
    src = _write(cache / 'pkg.bin')
    _clone(src, proj / 'pkg.bin')
    _write(proj / 'own.bin')

    r = _run_dt('reclaim', '-P', '-p', str(cache), '-j', str(proj))
    assert r.returncode == 0, r.stderr
    d = json.loads(r.stdout)
    assert d['partners'] == [str(cache)]
    assert d['shared'] == MIB
    assert d['frees'] == MIB
    assert [(row['path'], row['shared'], row['frees'], row['n_files']) for row in d['rows']] == [
        (str(proj), MIB, d['apparent'] - MIB, 2),
    ]
