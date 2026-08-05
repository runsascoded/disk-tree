"""Tests for `disk-tree diff` and `disk-tree series` (spec Item C)."""

from __future__ import annotations

import datetime as dt
import os
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
import pytest

TS_A = dt.datetime(2026, 7, 1, tzinfo=dt.timezone.utc)
TS_B = dt.datetime(2026, 7, 28, tzinfo=dt.timezone.utc)


def _write_listing(path: Path, rows: list[dict]) -> str:
    pd.DataFrame(rows).to_parquet(path)
    return str(path)


def _run_dt(env_root: Path, *args: str) -> subprocess.CompletedProcess:
    env = {**os.environ, 'DISK_TREE_ROOT': str(env_root)}
    return subprocess.run(
        [sys.executable, '-m', 'disk_tree.cli.main', *args],
        env=env, capture_output=True, text=True, check=False,
    )


def _import_two_snapshots(env_root: Path, tmp_path: Path) -> None:
    """Import a small bucket at two snapshot dates."""
    la = _write_listing(tmp_path / 'a.parquet', [
        {'bucket': 'b1', 'name': 'a.txt',     'size_bytes': 100, 'created': TS_A, 'storage_class_id': 1},
        {'bucket': 'b1', 'name': 'sub/b.txt', 'size_bytes': 200, 'created': TS_A, 'storage_class_id': 1},
        {'bucket': 'b1', 'name': 'sub/c.txt', 'size_bytes': 300, 'created': TS_A, 'storage_class_id': 1},
    ])
    lb = _write_listing(tmp_path / 'b.parquet', [
        {'bucket': 'b1', 'name': 'a.txt',     'size_bytes': 500, 'created': TS_B, 'storage_class_id': 1},
        {'bucket': 'b1', 'name': 'sub/b.txt', 'size_bytes': 200, 'created': TS_A, 'storage_class_id': 1},
        {'bucket': 'b1', 'name': 'sub/d.txt', 'size_bytes': 700, 'created': TS_B, 'storage_class_id': 1},
    ])
    r = _run_dt(env_root, 'import', '-l', la, '-b', 'b1', '-t', TS_A.isoformat())
    assert r.returncode == 0, r.stderr
    r = _run_dt(env_root, 'import', '-l', lb, '-b', 'b1', '-t', TS_B.isoformat())
    assert r.returncode == 0, r.stderr


# ---------- Parsers for output rows (structured comparison, per testing rules) ----------

@dataclass(frozen=True)
class DiffRow:
    path: str
    status: str
    size_delta: str
    n_desc_delta: str


DIFF_ROW_RE = re.compile(
    r'^(?P<path>\S.*?)\s{2,}'
    r'(?P<status>added|removed|changed|unchanged)\s+'
    r'\S+\s+\S+\s+'
    r'(?P<size_delta>\S+)\s+'
    r'(?P<n_desc_delta>[+-]?[\d,]+)\s*$'
)


def parse_diff_rows(output: str) -> list[DiffRow]:
    rows: list[DiffRow] = []
    for line in output.splitlines():
        m = DIFF_ROW_RE.match(line)
        if m and m.group('path') != 'TOTAL':
            rows.append(DiffRow(**m.groupdict()))
    return rows


@dataclass(frozen=True)
class SeriesRow:
    id: str
    time: str
    path: str
    n_desc: str
    n_children: str


# SQLite datetime format is `YYYY-MM-DD HH:MM:SS(.ssssss)?` (space, not T),
# so capture it explicitly instead of relying on non-space runs.
SERIES_ROW_RE = re.compile(
    r'^\s*(?P<id>\d+)\s+'
    r'(?P<time>\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}(?:\.\d+)?)\s+'
    r'(?P<path>\S.*?)\s{2,}'
    r'\S+\s+'
    r'(?P<n_desc>[\d,—]+)\s+'
    r'(?P<n_children>[\d,—]+)\s*$'
)


def parse_series_rows(output: str) -> list[SeriesRow]:
    rows: list[SeriesRow] = []
    for line in output.splitlines():
        m = SERIES_ROW_RE.match(line)
        if m:
            rows.append(SeriesRow(**m.groupdict()))
    return rows


# ---------- diff ----------

def test_diff_by_uri_picks_two_most_recent(tmp_path: Path):
    root = tmp_path / 'dt'
    listings_dir = tmp_path / 'listings'
    listings_dir.mkdir()
    _import_two_snapshots(root, listings_dir)

    r = _run_dt(root, 'diff', 'gcs://b1', '-H')  # -H → raw ints so parsing is stable
    assert r.returncode == 0, r.stderr
    rows = {row.path: row for row in parse_diff_rows(r.stdout)}
    # a.txt: 100→500 (+400); sub: 500→900 (+400, b removed, d added, b unchanged)
    assert set(rows) == {'a.txt', 'sub'}
    assert rows['a.txt'].status == 'changed'
    assert rows['a.txt'].size_delta == '+400'
    assert rows['sub'].status == 'changed'
    assert rows['sub'].size_delta == '+400'
    # sub delta: b.txt (0) + (700 - 300) = +400, and one +one -one for n_desc
    assert rows['sub'].n_desc_delta == '+0'  # 3 files → 3 files


def test_diff_by_scan_ids(tmp_path: Path):
    root = tmp_path / 'dt'
    listings_dir = tmp_path / 'listings'
    listings_dir.mkdir()
    _import_two_snapshots(root, listings_dir)

    # ids 1 and 2 exist (fresh DB)
    r = _run_dt(root, 'diff', '1', '2', '-H')
    assert r.returncode == 0, r.stderr
    rows = {row.path: row for row in parse_diff_rows(r.stdout)}
    assert set(rows) == {'a.txt', 'sub'}


def test_diff_with_path_option_drills_in(tmp_path: Path):
    root = tmp_path / 'dt'
    listings_dir = tmp_path / 'listings'
    listings_dir.mkdir()
    _import_two_snapshots(root, listings_dir)

    r = _run_dt(root, 'diff', 'gcs://b1', '-p', 'gcs://b1/sub', '-H')
    assert r.returncode == 0, r.stderr
    rows = {row.path: row for row in parse_diff_rows(r.stdout)}
    # sub had b (unchanged), c (removed), d (added)
    assert rows['c.txt'].status == 'removed'
    assert rows['c.txt'].size_delta == '-300'
    assert rows['d.txt'].status == 'added'
    assert rows['d.txt'].size_delta == '+700'
    # b.txt was unchanged so should NOT appear without --unchanged
    assert 'b.txt' not in rows


def test_diff_requires_two_scans(tmp_path: Path):
    root = tmp_path / 'dt'
    listings_dir = tmp_path / 'listings'
    listings_dir.mkdir()
    la = _write_listing(listings_dir / 'a.parquet', [
        {'bucket': 'b1', 'name': 'a.txt', 'size_bytes': 1, 'created': TS_A, 'storage_class_id': 1},
    ])
    r = _run_dt(root, 'import', '-l', la, '-b', 'b1', '-t', TS_A.isoformat())
    assert r.returncode == 0, r.stderr
    r = _run_dt(root, 'diff', 'gcs://b1')
    assert r.returncode != 0
    assert 'need ≥2 scans' in r.stderr


# ---------- series ----------

def test_series_lists_scans_newest_first(tmp_path: Path):
    root = tmp_path / 'dt'
    listings_dir = tmp_path / 'listings'
    listings_dir.mkdir()
    _import_two_snapshots(root, listings_dir)

    r = _run_dt(root, 'series', 'gcs://b1', '-H')
    assert r.returncode == 0, r.stderr
    rows = parse_series_rows(r.stdout)
    # Two scans of gcs://b1, newest first
    assert len(rows) == 2
    assert rows[0].path == 'gcs://b1'
    assert rows[1].path == 'gcs://b1'
    assert rows[0].time > rows[1].time
    # Snapshot A: 3 files + 2 synth dirs (root + sub) = 5 items; n_desc(root) includes self = 5
    # Snapshot B: 3 files + 2 synth dirs = 5 items; n_desc = 5
    assert rows[0].n_desc == '5'
    assert rows[1].n_desc == '5'


def test_series_empty_uri(tmp_path: Path):
    root = tmp_path / 'dt'
    # No scans imported
    listings_dir = tmp_path / 'listings'
    listings_dir.mkdir()
    la = _write_listing(listings_dir / 'a.parquet', [
        {'bucket': 'b1', 'name': 'a.txt', 'size_bytes': 1, 'created': TS_A, 'storage_class_id': 1},
    ])
    r = _run_dt(root, 'import', '-l', la, '-b', 'b1', '-t', TS_A.isoformat())
    assert r.returncode == 0
    r = _run_dt(root, 'series', 'gcs://nothing-here')
    assert r.returncode == 0
    assert '(no scans of gcs://nothing-here found)' in r.stdout
