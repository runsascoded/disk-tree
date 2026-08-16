"""Tests for the opt-in aggregation extensions (spec: aggregation-extensions.md).

`--pivot-sum <col>` (per-category byte sums) + `--mean-mtime` (size-weighted
mean mtime), byte-identical across all 3 engines, exact expected values on a
hand-computed fixture, cardinality guard, no-flags regression (covered by the
untouched pre-existing suites).
"""

import datetime as dt
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
import pytest

from disk_tree.find.agg_ext import PIVOT_MAX
from disk_tree.find.aggregate_duckdb import aggregate_listing_to_parquet
from disk_tree.find.aggregate_stream import aggregate_stream
from disk_tree.find.import_listing import import_listing
from disk_tree.listing import prepare_listing

from test_aggregate_duckdb import _normalize as _normalize_base, NUMERIC


def _ts(day: int) -> dt.datetime:
    return dt.datetime(2026, 7, day, tzinfo=dt.timezone.utc)


# (name, size, created-day, storage_class_id) — covers: multi-class dirs,
# single-class dirs, a zero-byte file (excluded from mtime_mean terms), a
# zero-size-total dir (mtime_mean NULL), and multi-day mtimes.
_EXT_LISTING = [
    ('data/hot.bin',       1000, 10, 1),
    ('data/warm.bin',      2000, 20, 2),
    ('data/cold/old.bin',  4000, 5,  4),
    ('archive/a.bin',      8000, 1,  4),
    ('archive/b.bin',      8000, 3,  4),
    ('empty/marker',       0,    15, 1),
    ('top.txt',            500,  25, 1),
]


def _write_ext_listing(path: Path) -> str:
    rows = sorted(_EXT_LISTING)
    pd.DataFrame({
        'bucket': ['b1'] * len(rows),
        'name': [n for n, *_ in rows],
        'size_bytes': [s for _, s, *_ in rows],
        'created': [_ts(d) for *_, d, _ in rows],
        'storage_class_id': [c for *_, c in rows],
    }).to_parquet(path)
    return str(path)


EXT_COLS = ['sum_storage_class_id_1', 'sum_storage_class_id_2', 'sum_storage_class_id_4', 'mtime_mean']


def _normalize(df: pd.DataFrame) -> pd.DataFrame:
    base = _normalize_base(df)
    ext = df[EXT_COLS].astype({c: 'int64' for c in EXT_COLS[:-1]}).copy()
    ext = ext.loc[df[NUMERIC + ['path']].sort_values(['depth', 'path']).index].reset_index(drop=True)
    return pd.concat([base, ext], axis=1)


def _all_engines(tmp_path: Path, listing: str) -> dict[str, pd.DataFrame]:
    kw = dict(pivot_sums=('storage_class_id',), mean_mtime=True)
    out = {}
    out['pandas'] = import_listing((listing,), bucket='b1', scheme='gcs', **kw).df

    con = duckdb.connect()
    ddb = str(tmp_path / 'ddb.parquet')
    aggregate_listing_to_parquet(
        prepare_listing(con, (listing,)),
        bucket='b1', scheme='gcs', out_parquet=ddb, con=con, **kw,
    )
    out['duckdb'] = pd.read_parquet(ddb)

    stream = str(tmp_path / 'stream.parquet')
    aggregate_stream((listing,), bucket='b1', scheme='gcs', out_parquet=stream, **kw)
    out['stream'] = pd.read_parquet(stream)
    return out


def test_three_engine_identity_with_extensions(tmp_path: Path):
    listing = _write_ext_listing(tmp_path / 'l.parquet')
    got = {k: _normalize(v) for k, v in _all_engines(tmp_path, listing).items()}
    pd.testing.assert_frame_equal(got['pandas'], got['duckdb'])
    pd.testing.assert_frame_equal(got['pandas'], got['stream'])


def test_exact_values(tmp_path: Path):
    """Hand-computed expectations — the spec of the two extensions."""
    listing = _write_ext_listing(tmp_path / 'l.parquet')
    df = _normalize(_all_engines(tmp_path, listing)['pandas'])

    def row(p):
        r = df[df.path == p]
        assert len(r) == 1
        return r.iloc[0]

    ep = {d: int(_ts(d).timestamp()) for d in (1, 3, 5, 10, 15, 20, 25)}

    # data/: class 1 = 1000, class 2 = 2000, class 4 = 4000
    data = row('data')
    assert [int(data[c]) for c in EXT_COLS[:-1]] == [1000, 2000, 4000]
    # mtime_mean(data) = (ep10·1000 + ep20·2000 + ep5·4000) / 7000
    expect = float(ep[10] * 1000 + ep[20] * 2000 + ep[5] * 4000) / float(7000)
    assert float(data['mtime_mean']) == expect

    # archive/: all class 4; equal weights → midpoint of day1/day3
    arch = row('archive')
    assert [int(arch[c]) for c in EXT_COLS[:-1]] == [0, 0, 16000]
    assert float(arch['mtime_mean']) == float(ep[1] * 8000 + ep[3] * 8000) / float(16000)

    # empty/: only a zero-byte file → all sums 0, mtime_mean NULL
    empty = row('empty')
    assert [int(empty[c]) for c in EXT_COLS[:-1]] == [0, 0, 0]
    assert np.isnan(empty['mtime_mean'])

    # the zero-byte file itself: files carry their own mtime
    marker = row('empty/marker')
    assert float(marker['mtime_mean']) == float(ep[15])

    # a file row's pivot columns are its own contribution
    warm = row('data/warm.bin')
    assert [int(warm[c]) for c in EXT_COLS[:-1]] == [0, 2000, 0]

    # root: totals conserved across classes (class 4 = data/cold 4000 + archive 16000)
    root = row('.')
    assert [int(root[c]) for c in EXT_COLS[:-1]] == [1500, 2000, 20000]
    total_w = sum(int(_ts(d).timestamp()) * s for _, s, d, _ in _EXT_LISTING)
    total_s = sum(s for _, s, _, _ in _EXT_LISTING)
    assert float(root['mtime_mean']) == float(total_w) / float(total_s)


def test_no_flags_output_unchanged(tmp_path: Path):
    """Extensions off → no extra columns (schema regression guard)."""
    listing = _write_ext_listing(tmp_path / 'l.parquet')
    df = import_listing((listing,), bucket='b1', scheme='gcs').df
    assert [c for c in df.columns if c.startswith('sum_') or c == 'mtime_mean'] == []


def test_cardinality_guard(tmp_path: Path):
    n = PIVOT_MAX + 1
    listing = tmp_path / 'wide.parquet'
    pd.DataFrame({
        'bucket': ['b1'] * n,
        'name': [f'f{i:03d}' for i in range(n)],
        'size_bytes': [1] * n,
        'created': [_ts(1)] * n,
        'storage_class_id': list(range(n)),
    }).to_parquet(listing)

    match = rf"--pivot-sum storage_class_id: {n} distinct values"
    with pytest.raises(ValueError, match=match):
        import_listing((str(listing),), bucket='b1', scheme='gcs', pivot_sums=('storage_class_id',))
    con = duckdb.connect()
    with pytest.raises(ValueError, match=match):
        aggregate_listing_to_parquet(
            prepare_listing(con, (str(listing),)),
            bucket='b1', scheme='gcs', out_parquet=str(tmp_path / 'o.parquet'),
            con=con, pivot_sums=('storage_class_id',),
        )
    with pytest.raises(ValueError, match=match):
        aggregate_stream(
            (str(listing),), bucket='b1', scheme='gcs',
            out_parquet=str(tmp_path / 'o2.parquet'), pivot_sums=('storage_class_id',),
        )


def test_extensions_with_dirty_keys(tmp_path: Path):
    """Pivot/mean values must survive the `//` dirty-key side-merge in the
    stream engine (and the canonicalization in the other two)."""
    rows = pd.DataFrame({
        'bucket': ['b1'] * 3,
        'name': sorted(['tok/a//x.bin', 'tok/a/y.bin', 'other/z.bin']),
        'size_bytes': [100, 300, 500],
        'created': [_ts(2), _ts(4), _ts(6)],
        'storage_class_id': [1, 2, 1],
    }).sort_values('name')
    listing = tmp_path / 'l.parquet'
    rows.to_parquet(listing)

    kw = dict(pivot_sums=('storage_class_id',), mean_mtime=True)
    got_pandas = import_listing((str(listing),), bucket='b1', scheme='gcs', **kw).df
    out = str(tmp_path / 's.parquet')
    aggregate_stream((str(listing),), bucket='b1', scheme='gcs', out_parquet=out, **kw)
    got_stream = pd.read_parquet(out)

    cols = ['sum_storage_class_id_1', 'sum_storage_class_id_2', 'mtime_mean']
    norm = lambda d: pd.concat(
        [_normalize_base(d), d[cols].reset_index(drop=True)], axis=1,
    )
    pd.testing.assert_frame_equal(norm(got_pandas), norm(got_stream))


def test_mean_mtime_exact_at_scale_boundary(tmp_path: Path):
    """Σ mtime·size beyond int64: a 5 EB file (near the int64 size ceiling)
    makes mtime·size ≈ 9e27 — far past int64 max (~9.2e18), forcing the
    exact-integer (bigint/HUGEINT) path in every engine."""
    big = 5 * 10**18
    listing = tmp_path / 'big.parquet'
    pd.DataFrame({
        'bucket': ['b1'] * 2,
        'name': ['huge.bin', 'tiny.bin'],
        'size_bytes': [big, 1],
        'created': [_ts(10), _ts(20)],
        'storage_class_id': [1, 1],
    }).to_parquet(listing)

    kw = dict(mean_mtime=True)
    got_pandas = import_listing((str(listing),), bucket='b1', scheme='gcs', **kw).df
    out = str(tmp_path / 's.parquet')
    aggregate_stream((str(listing),), bucket='b1', scheme='gcs', out_parquet=out, **kw)
    got_stream = pd.read_parquet(out)
    con = duckdb.connect()
    ddb = str(tmp_path / 'd.parquet')
    aggregate_listing_to_parquet(
        prepare_listing(con, (str(listing),)),
        bucket='b1', scheme='gcs', out_parquet=ddb, con=con, **kw,
    )
    got_duckdb = pd.read_parquet(ddb)

    e10, e20 = int(_ts(10).timestamp()), int(_ts(20).timestamp())
    expect = float(e10 * big + e20 * 1) / float(big + 1)
    for name, df in [('pandas', got_pandas), ('duckdb', got_duckdb), ('stream', got_stream)]:
        root = df[df.path == '.'].iloc[0]
        assert float(root['mtime_mean']) == expect, name


def test_mtime_mean_hugeint_rounding(tmp_path):
    """`mt_wsum` ≥ 2^64 with an unlucky bit pattern: DuckDB's direct
    HUGEINT→DOUBLE cast rounds up 1 ULP where Python's int→float (the
    pandas/stream engines' `mean_of`) rounds to even. The duckdb engine must
    route the conversion through VARCHAR (correctly-rounded parse) so all
    three engines stay byte-identical.

    Fixture: one file, size=20_064_072_762, mtime=1_785_542_400 →
    wsum = 35_825_252_633_236_108_800 (65 bits, tie-ish pattern)."""
    import duckdb
    import pandas as pd
    from disk_tree.find.aggregate_stream import aggregate_stream
    from disk_tree.find.aggregate_duckdb import aggregate_listing_to_parquet
    from disk_tree.listing import prepare_listing

    listing = str(tmp_path / 'l.parquet')
    pd.DataFrame({
        'bucket': ['b1'],
        'name': ['d/f.bin'],
        'size_bytes': [20_064_072_762],
        'created': [pd.Timestamp(1_785_542_400, unit='s', tz='UTC')],
        'storage_class_id': [1],
    }).to_parquet(listing)
    exact = float(1_785_542_400 * 20_064_072_762) / float(20_064_072_762)

    out_s = str(tmp_path / 's.parquet')
    aggregate_stream((listing,), bucket='b1', scheme='s3', out_parquet=out_s, mean_mtime=True)
    con = duckdb.connect()
    out_d = str(tmp_path / 'd.parquet')
    aggregate_listing_to_parquet(
        prepare_listing(con, (listing,)), bucket='b1', scheme='s3',
        out_parquet=out_d, con=con, mean_mtime=True,
    )
    ds = pd.read_parquet(out_s).set_index('path')['mtime_mean']
    dd = pd.read_parquet(out_d).set_index('path')['mtime_mean']
    assert ds['d'] == exact
    assert dd['d'] == exact
