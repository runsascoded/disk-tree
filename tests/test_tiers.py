"""Layer-2 as index tiers (spec mgu-scale-unification.md, item C).

Each tier is the finished layer-2 blob filtered and re-sorted: rows are the
same rows (exact sums), the sort is as declared, row groups are bounded, and
the floor is readable from the parquet metadata.
"""

from __future__ import annotations

import datetime as dt
import os
import sqlite3
import subprocess
import sys
from pathlib import Path

import duckdb
import pandas as pd
import pyarrow.parquet as pq
import pytest

from disk_tree.find.aggregate_duckdb import aggregate_listing_to_parquet
from disk_tree.find.tiers import DEFAULT_COARSE_EXP, ROW_GROUP_STEP, coarse_floor, parse_tiers, tier_path, write_tiers
from disk_tree.listing import prepare_listing

TS = dt.datetime(2026, 7, 28, tzinfo=dt.timezone.utc)

# 6 top-level dirs of 5 files each, sizes chosen so the coarse floor at a
# small exponent cuts between dirs: total = 6 × (1+2+3+4+5) × unit.
_UNIT = 1 << 20
_LISTING = [
    (f'd{d}/f{i}.bin', (i + 1) * _UNIT * (d + 1))
    for d in range(6)
    for i in range(5)
]


def _layer2(tmp_path: Path, labels: str | None = None) -> str:
    listing = tmp_path / 'l.parquet'
    pd.DataFrame({
        'bucket': ['b1'] * len(_LISTING),
        'name': [n for n, _ in _LISTING],
        'size_bytes': [s for _, s in _LISTING],
        'created': [TS] * len(_LISTING),
        'storage_class_id': [1] * len(_LISTING),
    }).to_parquet(listing)
    out = str(tmp_path / 'layer2.parquet')
    con = duckdb.connect()
    aggregate_listing_to_parquet(
        prepare_listing(con, (str(listing),)), bucket='b1', scheme='gcs', out_parquet=out, con=con,
        label=labels,
    )
    return out


def _sort_keys(path: str) -> list[tuple]:
    meta = pq.read_metadata(path).metadata
    cols = meta[b'sort'].decode().split(',')
    df = pd.read_parquet(path)
    return [tuple(None if pd.isna(v) else v for v in row) for row in df[cols].itertuples(index=False)]


def test_coarse_floor():
    assert coarse_floor(0) == 0
    assert coarse_floor(1) == 1
    # 3 PiB at the default exponent: round(log2) = round(51.58) = 52 → 2^(52 − 24) = 256 MiB
    assert coarse_floor(3 << 50) == 1 << 28
    assert coarse_floor(1 << 40, exp=10) == 1 << 30
    assert coarse_floor(1 << 40, exp=50) == 1
    assert DEFAULT_COARSE_EXP == 24


def test_parse_tiers():
    assert parse_tiers('dirs,objects,coarse') == ('dirs', 'objects', 'coarse')
    assert parse_tiers('coarse') == ('coarse',)
    with pytest.raises(ValueError, match=r"unknown tier\(s\) \['files'\]"):
        parse_tiers('dirs,files')
    with pytest.raises(ValueError, match="tier repeated in 'dirs,dirs'"):
        parse_tiers('dirs,dirs')


def test_tier_paths():
    assert tier_path('/x/gcs-b1', 'dirs') == '/x/gcs-b1.dirs.parquet'
    assert tier_path('/x/gcs-b1', 'coarse', ('usr',)) == '/x/gcs-b1.coarse-by-usr.parquet'
    assert tier_path('/x/gcs-b1', 'dirs', ('team', 'usr')) == '/x/gcs-b1.dirs-by-team-usr.parquet'


def test_tiers_are_the_layer2_rows_sorted_and_bounded(tmp_path: Path):
    layer2 = _layer2(tmp_path)
    stem = str(tmp_path / 'gcs-b1')
    written = write_tiers(layer2, stem, coarse_exp=4)
    total = sum(s for _, s in _LISTING)
    floor = coarse_floor(total, 4)
    assert floor == 1 << 24  # round(log2(315 MiB)) = 28 → 2^(28−4)
    assert written == {
        f'{stem}.dirs.parquet': 7,
        f'{stem}.objects.parquet': 30,
        f'{stem}.coarse.parquet': 6,
    }
    full = pd.read_parquet(layer2)
    cols = list(full.columns)

    dirs = pd.read_parquet(f'{stem}.dirs.parquet')
    assert list(dirs.columns) == cols
    pd.testing.assert_frame_equal(
        dirs, full[full.kind == 'dir'].sort_values(['depth', 'path']).reset_index(drop=True),
    )
    assert _sort_keys(f'{stem}.dirs.parquet') == sorted(_sort_keys(f'{stem}.dirs.parquet'))

    objects = pd.read_parquet(f'{stem}.objects.parquet')
    pd.testing.assert_frame_equal(
        objects, full[full.kind == 'file'].sort_values('path').reset_index(drop=True),
    )
    assert objects['path'].tolist() == sorted(n for n, _ in _LISTING)

    coarse = pd.read_parquet(f'{stem}.coarse.parquet')
    kept = full[(full.kind == 'dir') & (full['size'] >= floor)]
    pd.testing.assert_frame_equal(coarse, kept.sort_values(['depth', 'path']).reset_index(drop=True))
    # The floor cut a real boundary: d0 (15 MiB) is out, the rest + root are in.
    assert coarse['path'].tolist() == ['.', 'd1', 'd2', 'd3', 'd4', 'd5']
    assert coarse.set_index('path')['size'].to_dict() == {
        '.': total, 'd1': 30 * _UNIT, 'd2': 45 * _UNIT, 'd3': 60 * _UNIT, 'd4': 75 * _UNIT, 'd5': 90 * _UNIT,
    }

    kv = {k.decode(): v.decode() for k, v in pq.read_metadata(f'{stem}.coarse.parquet').metadata.items()
          if k != b'ARROW:schema'}
    assert kv == {
        'tier': 'coarse', 'sort': 'depth,path',
        'floor_bytes': str(floor), 'coarse_exp': '4', 'total_size': str(total),
    }
    kv = {k.decode(): v.decode() for k, v in pq.read_metadata(f'{stem}.objects.parquet').metadata.items()
          if k != b'ARROW:schema'}
    assert kv == {'tier': 'objects', 'sort': 'path'}


def test_row_groups_are_bounded(tmp_path: Path):
    """5000 objects at 2048 rows per group: `[2048, 2048, 904]` — the bound is
    exact for multiples of DuckDB's vector size, which `write_tiers` insists on."""
    listing = tmp_path / 'l.parquet'
    n = 5000
    pd.DataFrame({
        'bucket': ['b1'] * n,
        'name': [f'flat/f{i:05d}' for i in range(n)],
        'size_bytes': [1] * n,
        'created': [TS] * n,
        'storage_class_id': [1] * n,
    }).to_parquet(listing)
    layer2 = str(tmp_path / 'layer2.parquet')
    con = duckdb.connect()
    aggregate_listing_to_parquet(
        prepare_listing(con, (str(listing),)), bucket='b1', scheme='gcs', out_parquet=layer2, con=con,
    )
    stem = str(tmp_path / 'gcs-b1')
    assert write_tiers(layer2, stem, tiers=('objects', 'dirs'), row_group_rows=ROW_GROUP_STEP) == {
        f'{stem}.objects.parquet': n,
        f'{stem}.dirs.parquet': 2,
    }
    md = pq.read_metadata(f'{stem}.objects.parquet')
    assert [md.row_group(i).num_rows for i in range(md.num_row_groups)] == [2048, 2048, 904]
    assert pd.read_parquet(f'{stem}.objects.parquet')['path'].tolist() == [f'flat/f{i:05d}' for i in range(n)]
    md = pq.read_metadata(f'{stem}.dirs.parquet')
    assert [md.row_group(i).num_rows for i in range(md.num_row_groups)] == [2]


def test_sort_variants_over_label_slices(tmp_path: Path):
    """With label slices the sort closes on the label columns; a variant leads
    with them and the floor applies per row."""
    labels = tmp_path / 'labels.parquet'
    pd.DataFrame({'prefix': ['d0', 'd1', 'd3', 'd5'], 'usr': ['c', 'a', 'b', 'a']}).to_parquet(labels)
    layer2 = _layer2(tmp_path, labels=str(labels))
    stem = str(tmp_path / 'gcs-b1')
    written = write_tiers(layer2, stem, tiers=('dirs', 'coarse'), coarse_exp=4, sort_variants=(('usr',),))
    assert list(written) == [
        f'{stem}.dirs.parquet', f'{stem}.dirs-by-usr.parquet',
        f'{stem}.coarse.parquet', f'{stem}.coarse-by-usr.parquet',
    ]
    dirs = pd.read_parquet(f'{stem}.dirs.parquet')
    assert list(dirs.columns[:3]) == ['path', 'usr', 'size']
    assert [tuple(r) for r in dirs[['path', 'usr', 'size']].itertuples(index=False)] == [
        ('.', None, 120 * _UNIT), ('.', 'a', 120 * _UNIT), ('.', 'b', 60 * _UNIT), ('.', 'c', 15 * _UNIT),
        ('d0', 'c', 15 * _UNIT), ('d1', 'a', 30 * _UNIT), ('d2', None, 45 * _UNIT),
        ('d3', 'b', 60 * _UNIT), ('d4', None, 75 * _UNIT), ('d5', 'a', 90 * _UNIT),
    ]
    by_usr = pd.read_parquet(f'{stem}.dirs-by-usr.parquet')
    assert pq.read_metadata(f'{stem}.dirs-by-usr.parquet').metadata[b'sort'] == b'usr,depth,path'
    assert [tuple(r) for r in by_usr[['usr', 'path']].itertuples(index=False)] == [
        (None, '.'), (None, 'd2'), (None, 'd4'),
        ('a', '.'), ('a', 'd1'), ('a', 'd5'),
        ('b', '.'), ('b', 'd3'),
        ('c', '.'), ('c', 'd0'),
    ]
    # Floor 16 MiB (2^(28−4)) per *row*: the root's `c` slice (15 MiB) drops
    # while its other slices stay; `d0` drops with it.
    coarse = pd.read_parquet(f'{stem}.coarse-by-usr.parquet')
    assert [tuple(r) for r in coarse[['usr', 'path']].itertuples(index=False)] == [
        (None, '.'), (None, 'd2'), (None, 'd4'),
        ('a', '.'), ('a', 'd1'), ('a', 'd5'),
        ('b', '.'), ('b', 'd3'),
    ]


def test_write_tiers_validation(tmp_path: Path):
    layer2 = _layer2(tmp_path)
    stem = str(tmp_path / 'x')
    with pytest.raises(ValueError, match=r"sort variant \('nope',\): column\(s\) \['nope'\] not in"):
        write_tiers(layer2, stem, sort_variants=(('nope',),))
    with pytest.raises(ValueError, match="row_group_rows must be a positive multiple of 2048; got 3000"):
        write_tiers(layer2, stem, row_group_rows=3000)
    with pytest.raises(ValueError, match="unknown tier 'files'"):
        write_tiers(layer2, stem, tiers=('files',))
    not_layer2 = tmp_path / 'other.parquet'
    pd.DataFrame({'a': [1]}).to_parquet(not_layer2)
    with pytest.raises(ValueError, match=r"not a layer-2 parquet \(columns \['a'\]\)"):
        write_tiers(str(not_layer2), stem)


def test_cli_import_writes_tiers(tmp_path: Path):
    listing = tmp_path / 'listing.parquet'
    pd.DataFrame({
        'bucket': ['b1'] * len(_LISTING),
        'name': [n for n, _ in _LISTING],
        'size_bytes': [s for _, s in _LISTING],
        'created': [TS] * len(_LISTING),
        'storage_class_id': [1] * len(_LISTING),
    }).to_parquet(listing)
    root = tmp_path / 'dt-root'
    tiers_dir = tmp_path / 'tiers'
    env = {**os.environ, 'DISK_TREE_ROOT': str(root)}
    r = subprocess.run(
        [sys.executable, '-m', 'disk_tree.cli.main', 'import',
         '-e', 'duckdb', '-l', str(listing), '-b', 'b1', '-t', TS.isoformat(),
         '-i', 'dirs,coarse', '-O', str(tiers_dir), '-E', '4', '-r', '2048'],
        env=env, capture_output=True, text=True, check=False,
    )
    assert r.returncode == 0, f"stdout={r.stdout!r} stderr={r.stderr!r}"
    assert sorted(p.name for p in tiers_dir.iterdir()) == ['gcs-b1.coarse.parquet', 'gcs-b1.dirs.parquet']
    assert pd.read_parquet(tiers_dir / 'gcs-b1.coarse.parquet')['path'].tolist() == ['.', 'd1', 'd2', 'd3', 'd4', 'd5']
    conn = sqlite3.connect(root / 'disk-tree.db')
    rows = conn.execute("SELECT path, size, n_children, n_desc FROM scan").fetchall()
    conn.close()
    assert rows == [('gcs://b1', sum(s for _, s in _LISTING), 6, 37)]


def test_cli_tiers_need_a_dir_and_a_disk_engine(tmp_path: Path):
    from disk_tree.cli.import_listing import TierOpts, import_bucket
    with pytest.raises(ValueError, match="--tiers needs a blob on disk: use the duckdb or stream engine"):
        import_bucket(
            db=None, storage=None, con=None, engine='pandas', listings=('x',), bucket='b1',
            scheme='gcs', snap_time=TS, tier_opts=TierOpts(tiers=('dirs',), out_dir=str(tmp_path)),
        )
    listing = tmp_path / 'listing.parquet'
    pd.DataFrame({'bucket': ['b1'], 'name': ['a'], 'size_bytes': [1], 'created': [TS], 'storage_class_id': [1]}).to_parquet(listing)
    r = subprocess.run(
        [sys.executable, '-m', 'disk_tree.cli.main', 'import',
         '-e', 'duckdb', '-l', str(listing), '-b', 'b1', '-i', 'dirs'],
        env={**os.environ, 'DISK_TREE_ROOT': str(tmp_path / 'root')}, capture_output=True, text=True, check=False,
    )
    assert r.returncode != 0
    assert r.stderr.rstrip().split('\n')[-1] == 'ValueError: --tiers needs --tiers-dir (or --out-dir)'
