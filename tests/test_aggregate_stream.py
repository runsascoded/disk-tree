"""Tests for the streaming aggregation engine (spec: streaming-aggregation.md).

Parity target: byte-identical layer-2 output vs the pandas and DuckDB
engines on the same listing — including the `_IDENTITY_LISTING` edge-case
fixture (which contains `//` keys, the one pattern that perturbs sorted
order and exercises the dirty-key side-merge).
"""

import datetime as dt
import os
import sqlite3
import subprocess
import sys
from pathlib import Path

import duckdb
import pandas as pd
import pytest

from disk_tree.find.aggregate_duckdb import aggregate_listing_to_parquet
from disk_tree.find.aggregate_stream import aggregate_stream
from disk_tree.find.import_listing import import_listing
from disk_tree.listing import prepare_listing

from test_aggregate_duckdb import _IDENTITY_LISTING, _normalize, TS


def _write_listing(path: Path, rows: list[tuple[str, int]], bucket: str = 'b1', sort: bool = True):
    """Raw-schema listing parquet; sorted by key like real object-store listings."""
    if sort:
        rows = sorted(rows)
    pd.DataFrame({
        'bucket': [bucket] * len(rows),
        'name': [n for n, _ in rows],
        'size_bytes': [s for _, s in rows],
        'created': [TS] * len(rows),
        'storage_class_id': [1] * len(rows),
    }).to_parquet(path)
    return str(path)


def _stream(listings: tuple[str, ...], tmp_path: Path, bucket: str = 'b1'):
    out = str(tmp_path / 'stream-out.parquet')
    stats = aggregate_stream(listings, bucket=bucket, scheme='gcs', out_parquet=out)
    return _normalize(pd.read_parquet(out)), stats


# ---------- Cross-engine identity: all 3 engines, same edge-case fixture ----------

def test_three_engine_identity(tmp_path: Path):
    listing = _write_listing(tmp_path / 'l.parquet', _IDENTITY_LISTING)

    got_pandas = _normalize(import_listing((listing,), bucket='b1', scheme='gcs').df)

    con = duckdb.connect()
    ooc = str(tmp_path / 'ooc.parquet')
    aggregate_listing_to_parquet(
        prepare_listing(con, (listing,)),
        bucket='b1', scheme='gcs', out_parquet=ooc, con=con,
    )
    got_duckdb = _normalize(pd.read_parquet(ooc))

    got_stream, stats = _stream((listing,), tmp_path)

    pd.testing.assert_frame_equal(got_pandas, got_duckdb)
    pd.testing.assert_frame_equal(got_pandas, got_stream)

    # Column ORDER is contractual too (file-level diffs, positional set ops
    # like EXCEPT) — `_normalize` reindexes to COLS, hiding order skew, so
    # lock the raw parquet column lists explicitly. Caught on the CW 92.7M
    # acceptance: stream emitted (path,size,mtime,kind,parent,uri,n_*,depth).
    assert list(pd.read_parquet(str(tmp_path / 'stream-out.parquet')).columns) == \
        list(pd.read_parquet(ooc).columns) == [
        'path', 'size', 'mtime', 'n_desc', 'n_files', 'n_children', 'kind', 'parent', 'uri', 'depth',
    ]

    root = got_stream[got_stream.path == '.'].iloc[0]
    assert stats['root_size'] == int(root['size'])
    assert stats['root_n_desc'] == int(root['n_desc'])
    assert stats['root_n_files'] == int(root['n_files'])
    assert stats['root_n_children'] == int(root['n_children'])
    assert stats['rows'] == len(got_stream)
    assert stats['files'] == len(_IDENTITY_LISTING)


@pytest.mark.parametrize('rows', [
    # Small nested tree with a sibling top-level dir
    [('a.txt', 100), ('sub/b.txt', 200), ('sub/c.txt', 300), ('sub/deep/d.txt', 400), ('other/e.txt', 50)],
    # Single-file bucket
    [('lonely.bin', 42)],
    # All at root
    [('a', 1), ('b', 2), ('c', 3), ('d', 4)],
    # Deep single chain
    [('x/y/z/w/leaf.txt', 999)],
    # A key that is a proper prefix of a dir name (file 'a' + dir 'a/')
    [('a', 7), ('a/b', 8), ('a/b/c', 9)],
])
def test_stream_matches_pandas(rows, tmp_path: Path):
    listing = _write_listing(tmp_path / 'l.parquet', rows)
    got_pandas = _normalize(import_listing((listing,), bucket='b1', scheme='gcs').df)
    got_stream, _ = _stream((listing,), tmp_path)
    pd.testing.assert_frame_equal(got_pandas, got_stream)


# ---------- Multi-shard k-way merge ----------

def test_multi_shard_merge(tmp_path: Path):
    """Rows split across 2 shards by key range must aggregate identically to
    the same rows in one shard."""
    rows = sorted(_IDENTITY_LISTING)
    mid = len(rows) // 2
    shard_dir = tmp_path / 'shards'
    shard_dir.mkdir()
    _write_listing(shard_dir / 'shard-000.parquet', rows[:mid], sort=False)
    _write_listing(shard_dir / 'shard-001.parquet', rows[mid:], sort=False)
    single = _write_listing(tmp_path / 'single.parquet', rows, sort=False)

    got_multi, stats_multi = _stream((str(shard_dir / '*.parquet'),), tmp_path)
    out2 = str(tmp_path / 'single-out.parquet')
    aggregate_stream((single,), bucket='b1', scheme='gcs', out_parquet=out2)
    got_single = _normalize(pd.read_parquet(out2))

    pd.testing.assert_frame_equal(got_multi, got_single)
    assert stats_multi['files'] == len(rows)


def test_interleaved_shards(tmp_path: Path):
    """Shards with interleaved (not range-partitioned) keys still merge correctly —
    each shard is sorted, global order comes from the heap merge."""
    rows = sorted(_IDENTITY_LISTING)
    shard_dir = tmp_path / 'shards'
    shard_dir.mkdir()
    _write_listing(shard_dir / 'shard-000.parquet', rows[0::2], sort=False)
    _write_listing(shard_dir / 'shard-001.parquet', rows[1::2], sort=False)

    got_multi, _ = _stream((str(shard_dir / '*.parquet'),), tmp_path)
    got_pandas = _normalize(import_listing((str(shard_dir / '*.parquet'),), bucket='b1', scheme='gcs').df)
    pd.testing.assert_frame_equal(got_pandas, got_multi)


# ---------- Error paths ----------

def test_fractional_second_timestamps(tmp_path: Path):
    """Sub-second `created` values must TRUNCATE to epoch seconds in every
    engine. Bare `epoch(ts)::BIGINT` in DuckDB *rounds* — on the CW 92.7M
    acceptance run that skewed ~50% of mtimes +1s vs the stream engine
    (fractional parts ≥ .5). Whole-second fixtures can't see this."""
    ts_frac = dt.datetime(2026, 7, 28, 12, 0, 0, 700_000, tzinfo=dt.timezone.utc)  # .7s
    listing = tmp_path / 'l.parquet'
    pd.DataFrame({
        'bucket': ['b1', 'b1'],
        'name': ['a.txt', 'sub/b.txt'],
        'size_bytes': [100, 200],
        'created': [ts_frac, ts_frac],
        'storage_class_id': [1, 1],
    }).to_parquet(listing)
    expect = int(ts_frac.timestamp())  # floor: …00, not …01

    got_pandas = _normalize(import_listing((str(listing),), bucket='b1', scheme='gcs').df)

    con = duckdb.connect()
    ooc = str(tmp_path / 'ooc.parquet')
    aggregate_listing_to_parquet(
        prepare_listing(con, (str(listing),)),
        bucket='b1', scheme='gcs', out_parquet=ooc, con=con,
    )
    got_duckdb = _normalize(pd.read_parquet(ooc))

    got_stream, _ = _stream((str(listing),), tmp_path)

    assert sorted(got_stream[got_stream.kind == 'file'].mtime.tolist()) == [expect, expect]
    pd.testing.assert_frame_equal(got_pandas, got_duckdb)
    pd.testing.assert_frame_equal(got_pandas, got_stream)


def test_piecewise_sorted_shard(tmp_path: Path):
    """bulk-list bin-packs multiple sorted key ranges into one shard, so shards
    are piecewise sorted: each maximal sorted run becomes its own merge source.
    An out-of-order shard must aggregate identically to the duckdb engine."""
    rows = [('z/a.txt', 1), ('z/b.txt', 2), ('a/x.txt', 3), ('a/y.txt', 4), ('m.txt', 5)]
    listing = _write_listing(tmp_path / 'l.parquet', rows, sort=False)

    got_stream, stats = _stream((listing,), tmp_path)

    con = duckdb.connect()
    ooc = str(tmp_path / 'ooc.parquet')
    aggregate_listing_to_parquet(
        prepare_listing(con, (listing,)),
        bucket='b1', scheme='gcs', out_parquet=ooc, con=con,
    )
    got_duckdb = _normalize(pd.read_parquet(ooc))

    pd.testing.assert_frame_equal(got_stream, got_duckdb)
    assert stats['files'] == 5


def test_interleaved_buckets_multi_run_row_groups(tmp_path: Path):
    """Locks the raw-ordinal contract behind row-group skipping: run-start
    ordinals are *raw* (unfiltered) row indices, so a shard interleaving two
    buckets makes raw ≠ bucket-filtered ordinals (a filtered-ordinal reader
    would slice the wrong rows), and `row_group_size=2` forces the reader to
    actually map ordinals onto row-group boundaries and skip non-intersecting
    groups."""
    rows = [
        ('b2', 'zz/1', 9),
        ('b1', 'z/a.txt', 1),
        ('b1', 'z/b.txt', 2),
        ('b2', 'aa/1', 9),
        ('b2', 'ab/2', 9),
        ('b1', 'a/x.txt', 3),
        ('b1', 'a/y.txt', 4),
        ('b2', 'q/1', 9),
        ('b1', 'm.txt', 5),
        ('b1', 'n.txt', 6),
    ]
    listing = str(tmp_path / 'l.parquet')
    pd.DataFrame({
        'bucket': [b for b, _, _ in rows],
        'name': [n for _, n, _ in rows],
        'size_bytes': [s for _, _, s in rows],
        'created': [TS] * len(rows),
        'storage_class_id': [1] * len(rows),
    }).to_parquet(listing, row_group_size=2)

    for bucket, n_files in [('b1', 6), ('b2', 4)]:
        got_stream, stats = _stream((listing,), tmp_path, bucket=bucket)
        con = duckdb.connect()
        ooc = str(tmp_path / f'ooc-{bucket}.parquet')
        aggregate_listing_to_parquet(
            prepare_listing(con, (listing,)),
            bucket=bucket, scheme='gcs', out_parquet=ooc, con=con,
        )
        got_duckdb = _normalize(pd.read_parquet(ooc))
        pd.testing.assert_frame_equal(got_stream, got_duckdb)
        assert stats['files'] == n_files


def test_lazy_merge_bounds_open_sources(tmp_path: Path):
    """`heapq.merge` primed every run source up front — at fleet scale
    (thousands of bin-packed runs) that's EMFILE plus a row-group read buffer
    per run. The lazy merge opens a source only when the merge horizon reaches
    its first key and drops it at exhaustion, bounding concurrently-open
    sources by the runs' range-overlap depth. Here: 4 runs across 2 shards,
    pairwise overlapping with depth 2 → high-water must be 2, not 4."""
    a = [(f'{i:03d}.txt', i + 1) for i in [*range(20, 30, 2), *range(0, 10, 2)]]
    b = [(f'{i:03d}.txt', i + 1) for i in [*range(21, 31, 2), *range(1, 11, 2)]]
    la = _write_listing(tmp_path / 'a.parquet', a, sort=False)
    lb = _write_listing(tmp_path / 'b.parquet', b, sort=False)

    got_stream, stats = _stream((f'{tmp_path}/[ab].parquet',), tmp_path)
    assert stats['max_open_sources'] == 2

    con = duckdb.connect()
    ooc = str(tmp_path / 'ooc.parquet')
    aggregate_listing_to_parquet(
        prepare_listing(con, (f'{tmp_path}/[ab].parquet',)),
        bucket='b1', scheme='gcs', out_parquet=ooc, con=con,
    )
    pd.testing.assert_frame_equal(got_stream, _normalize(pd.read_parquet(ooc)))


def test_finalize_failure_preserves_parts_and_resumes(tmp_path: Path, capsys, monkeypatch):
    """A finalize-only failure keeps the streamed parts dir (a 63-minute
    stream pass must not re-run), and a rerun with the same output path
    resumes at the merge — skipping the stream pass entirely."""
    from disk_tree.find import aggregate_stream as mod
    listing = _write_listing(tmp_path / 'l.parquet', [('a/x', 1), ('b/y', 2)])
    out = tmp_path / 'out.parquet'

    def boom(*a, **kw):
        raise RuntimeError('injected finalize failure')

    monkeypatch.setattr(mod, '_finalize_parts', boom)
    with pytest.raises(RuntimeError):
        aggregate_stream((listing,), bucket='b1', scheme='gcs', out_parquet=str(out))
    err = capsys.readouterr().err
    assert 'streamed parts preserved at' in err.rsplit('\n', 2)[-2]
    parts_dir = Path(f'{out}.parts')
    assert (parts_dir / 'manifest.json').exists()

    monkeypatch.undo()
    # Same output path → the parts manifest is the resume token.
    stats = aggregate_stream((listing,), bucket='b1', scheme='gcs', out_parquet=str(out))
    assert 'stream pass skipped' in capsys.readouterr().err
    assert not parts_dir.exists()
    got = _normalize(pd.read_parquet(out))
    expected = _normalize(import_listing((listing,), bucket='b1', scheme='gcs').df)
    pd.testing.assert_frame_equal(got, expected)
    assert stats['rows'] == 5


def test_prefix_sibling_dir_inversion(tmp_path: Path):
    """Sibling dirs where one name is a proper prefix of the other with next
    char < '/' (`store` vs `store-backup`): `store-backup/`'s subtree sorts
    *before* `store/`, so its dir row pops first — the depth's dir part is
    genuinely unsorted and the finalize must sort it. Identity vs pandas locks
    the repair."""
    rows = [
        ('store-backup/old.bin', 10),
        ('store.old/x.bin', 7),
        ('store/a.bin', 1),
        ('store/b.bin', 2),
    ]
    listing = _write_listing(tmp_path / 'l.parquet', rows)
    got, _ = _stream((listing,), tmp_path)
    expected = _normalize(import_listing((listing,), bucket='b1', scheme='gcs').df)
    pd.testing.assert_frame_equal(got, expected)


def test_deep_chain(tmp_path: Path):
    """Many depths, ~one row each — exercises part-writer family churn and the
    depth-ascending finalize."""
    path = '/'.join(f'd{i:02d}' for i in range(40))
    listing = _write_listing(tmp_path / 'l.parquet', [(f'{path}/leaf.bin', 5), ('top.bin', 3)])
    got, stats = _stream((listing,), tmp_path)
    expected = _normalize(import_listing((listing,), bucket='b1', scheme='gcs').df)
    pd.testing.assert_frame_equal(got, expected)
    assert stats['rows'] == 43  # 2 files + 40 chain dirs + root


def test_same_path_file_and_dir_tiny_batches(tmp_path: Path, monkeypatch):
    """A key that is both a file and a dir name at the same depth is the merge
    tiebreak (dir row first); tiny flush/batch sizes force the boundary-split
    paths in `_merge_two_sorted` instead of whole-batch passthrough."""
    from disk_tree.find import aggregate_stream as mod
    monkeypatch.setattr(mod, '_FLUSH_ROWS', 2)
    rows = [
        ('a/x', 1), ('a/x/1.bin', 2), ('a/x/2.bin', 3),
        ('a/y', 4), ('b/x', 5), ('b/y/z.bin', 6), ('c.bin', 7),
    ]
    listing = _write_listing(tmp_path / 'l.parquet', rows)
    out = str(tmp_path / 'out.parquet')
    mod.aggregate_stream((listing,), bucket='b1', scheme='gcs', out_parquet=out)
    got = pd.read_parquet(out)
    expected = import_listing((listing,), bucket='b1', scheme='gcs').df
    # Raw (un-normalized) comparison: row order and column order must match
    # the pandas engine byte-for-byte, including the dir-before-file tiebreak.
    pd.testing.assert_frame_equal(
        got.reset_index(drop=True), expected.reset_index(drop=True), check_dtype=False,
    )


def test_essentially_unsorted_raises(tmp_path: Path, monkeypatch):
    """The run-count guard: input whose runs explode (≈ every row its own run)
    gets the clear duckdb hint instead of a degenerate 1-row-per-source merge."""
    import disk_tree.find.aggregate_stream as mod
    monkeypatch.setattr(mod, '_MAX_RUNS', 2)
    listing = _write_listing(
        tmp_path / 'l.parquet',
        [('z.txt', 1), ('y.txt', 2), ('x.txt', 3), ('w.txt', 4)],  # strictly descending
        sort=False,
    )
    with pytest.raises(ValueError, match=r"sorted runs across listing shards.*use `-e duckdb`"):
        aggregate_stream((listing,), bucket='b1', scheme='gcs', out_parquet=str(tmp_path / 'out.parquet'))


def test_missing_bucket_raises(tmp_path: Path):
    listing = _write_listing(tmp_path / 'l.parquet', [('a.txt', 1)])
    with pytest.raises(ValueError, match="no rows for bucket 'nope'"):
        aggregate_stream((listing,), bucket='nope', scheme='gcs', out_parquet=str(tmp_path / 'out.parquet'))


def test_non_raw_schema_raises(tmp_path: Path):
    """SII-shaped listing (size/timeCreated, no size_bytes) → clear error + duckdb hint."""
    listing = tmp_path / 'sii.parquet'
    pd.DataFrame({
        'bucket': ['b1'],
        'name': ['a.txt'],
        'size': [100],
        'timeCreated': [TS],
        'storageClass': ['STANDARD'],
    }).to_parquet(listing)
    with pytest.raises(ValueError, match=r"lacks required columns.*use `-e duckdb`"):
        aggregate_stream((str(listing),), bucket='b1', scheme='gcs', out_parquet=str(tmp_path / 'out.parquet'))


def test_empty_glob_raises(tmp_path: Path):
    with pytest.raises(ValueError, match='no files match listing glob'):
        aggregate_stream(
            (str(tmp_path / 'nope-*.parquet'),),
            bucket='b1', scheme='gcs', out_parquet=str(tmp_path / 'out.parquet'),
        )


# ---------- Earlier-source-wins across multiple listing globs ----------

def test_earlier_listing_wins(tmp_path: Path):
    """Bucket present in both globs → first glob's rows win (prepare_listing parity)."""
    fresh = _write_listing(tmp_path / 'fresh.parquet', [('new.txt', 111)])
    stale = _write_listing(tmp_path / 'stale.parquet', [('old.txt', 999)])
    got, stats = _stream((fresh, stale), tmp_path)
    assert stats['root_size'] == 111
    assert got[got.kind == 'file'].path.tolist() == ['new.txt']


def test_later_listing_fills_missing_bucket(tmp_path: Path):
    """Bucket only in the second glob → falls through to it."""
    other = _write_listing(tmp_path / 'other.parquet', [('x.txt', 5)], bucket='b_other')
    mine = _write_listing(tmp_path / 'mine.parquet', [('y.txt', 6)], bucket='b1')
    got, stats = _stream((other, mine), tmp_path)
    assert stats['root_size'] == 6
    assert got[got.kind == 'file'].path.tolist() == ['y.txt']


# ---------- The `//` order-perturbation case, isolated ----------

def test_dirty_keys_out_of_raw_order(tmp_path: Path):
    """Raw sort puts `a//b/c` before `a/aa`, but canonical `a/b/c` belongs
    after — without the dirty-key side-merge, dir `a/b` would be closed
    before its second file arrived. Locks the two-pass design."""
    rows = [
        ('a//b/c.txt', 1),   # canonical a/b/c.txt — arrives "early" in raw order
        ('a/aa.txt', 2),
        ('a/b/d.txt', 4),    # same canonical dir as the dirty key's target
        ('a/z.txt', 8),
    ]
    listing = _write_listing(tmp_path / 'l.parquet', rows)  # raw sort: a// first
    got_pandas = _normalize(import_listing((listing,), bucket='b1', scheme='gcs').df)
    got_stream, stats = _stream((listing,), tmp_path)
    pd.testing.assert_frame_equal(got_pandas, got_stream)
    ab = got_stream[got_stream.path == 'a/b'].iloc[0]
    assert int(ab['size']) == 5
    assert int(ab['n_files']) == 2


def test_trailing_slash_key(tmp_path: Path):
    """Keys ending in '/' (placeholder "dirs") canonicalize by stripping —
    they become zero-or-more-byte file rows at the stripped path."""
    rows = [
        ('pre/', 0),
        ('pre/x.txt', 10),
    ]
    listing = _write_listing(tmp_path / 'l.parquet', rows)
    got_pandas = _normalize(import_listing((listing,), bucket='b1', scheme='gcs').df)
    got_stream, _ = _stream((listing,), tmp_path)
    pd.testing.assert_frame_equal(got_pandas, got_stream)


# ---------- CLI --engine stream creates a scan row (subprocess isolation) ----------

def test_cli_engine_stream_creates_scan(tmp_path: Path):
    listing = _write_listing(tmp_path / 'listing.parquet', [('a.txt', 100), ('sub/b.txt', 200)])
    root = tmp_path / 'dt-root'
    env = {**os.environ, 'DISK_TREE_ROOT': str(root)}
    r = subprocess.run(
        [sys.executable, '-m', 'disk_tree.cli.main', 'import',
         '-e', 'stream', '-l', listing, '-b', 'b1', '-t', TS.isoformat()],
        env=env, capture_output=True, text=True, check=False,
    )
    assert r.returncode == 0, f"stdout={r.stdout!r} stderr={r.stderr!r}"
    conn = sqlite3.connect(root / 'disk-tree.db')
    rows = conn.execute("SELECT path, size, n_children, n_desc FROM scan").fetchall()
    conn.close()
    assert rows == [('gcs://b1', 300, 2, 4)]
