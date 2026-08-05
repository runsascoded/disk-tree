"""Tests for `disk_tree.find.bulk` — the scheme-generic sharded-listing
primitives (bin-packing, dedupe, hot-prefix range-splitting, entry framing).

The GCS-specific pieces (`bulk_gcs`) aren't tested here — they need cloud
credentials. The generic helpers cover ~70% of the ported LOC.
"""

from __future__ import annotations

import datetime as dt
import io
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional

import pandas as pd
import pytest

from disk_tree.find import bulk


# ---------- pack_chunks ----------

def test_pack_chunks_balances_by_weight():
    # 4 items with weights 100, 50, 25, 25 → 2 chunks should split ~100 vs ~100
    weights = {'a': 100, 'b': 50, 'c': 25, 'd': 25}
    chunks = bulk.pack_chunks(['a', 'b', 'c', 'd'], weights, n=2)
    totals = sorted(sum(weights[p] for p in chunk) for chunk in chunks)
    assert totals == [100, 100]


def test_pack_chunks_missing_weight_defaults_to_one():
    # 'x' has no weight → treated as 1; 'a' at 100 dominates
    chunks = bulk.pack_chunks(['a', 'x', 'y', 'z'], {'a': 100}, n=2)
    totals = sorted(sum({'a': 100}.get(p, 1) for p in chunk) for chunk in chunks)
    assert totals == [3, 100]


def test_pack_chunks_empty_bins_when_fewer_items_than_bins():
    chunks = bulk.pack_chunks(['a'], {'a': 1}, n=3)
    assert sorted(len(c) for c in chunks) == [0, 0, 1]


def test_pack_chunks_stable_bin_indices():
    # Same input → same output (deterministic tiebreak on str(p) sort).
    a = bulk.pack_chunks(['x', 'y', 'z'], {}, n=2)
    b = bulk.pack_chunks(['x', 'y', 'z'], {}, n=2)
    assert a == b


# ---------- dedupe_prefixes ----------

def test_dedupe_prefixes_drops_nested_children():
    kept, dropped = bulk.dedupe_prefixes(['a/', 'a/b/', 'a/b/c/', 'x/', 'y/'])
    assert kept == ['a/', 'x/', 'y/']
    assert dropped == [('a/b/', 'a/'), ('a/b/c/', 'a/')]


def test_dedupe_prefixes_sorts_and_dedupes():
    kept, dropped = bulk.dedupe_prefixes(['b/', 'a/', 'a/', 'c/'])
    assert kept == ['a/', 'b/', 'c/']
    assert dropped == []


def test_dedupe_prefixes_empty():
    kept, dropped = bulk.dedupe_prefixes([])
    assert kept == []
    assert dropped == []


def test_dedupe_prefixes_all_nested():
    # 'foo/' first; 'foo/bar/' inside it; 'foo/bar/baz/' inside that
    kept, dropped = bulk.dedupe_prefixes(['foo/bar/baz/', 'foo/', 'foo/bar/'])
    assert kept == ['foo/']
    assert dropped == [('foo/bar/', 'foo/'), ('foo/bar/baz/', 'foo/')]


# ---------- entries_to_frame ----------

def test_entries_to_frame_maps_storage_class_id():
    ts = "2026-08-05T12:00:00Z"
    rows = [
        ('a.txt', 100, ts, 'STANDARD'),
        ('b.txt', 200, ts, 'NEARLINE'),
        ('c.txt', 300, ts, 'COLDLINE'),
        ('d.txt', 400, ts, 'ARCHIVE'),
        ('e.txt', 500, ts, None),          # unknown → 0
        ('f.txt', 600, ts, 'WEIRD_CLASS'),  # unrecognized → 0
    ]
    df = bulk.entries_to_frame('b1', rows)
    assert df['bucket'].tolist() == ['b1'] * 6
    assert df['name'].tolist() == ['a.txt', 'b.txt', 'c.txt', 'd.txt', 'e.txt', 'f.txt']
    assert df['size_bytes'].tolist() == [100, 200, 300, 400, 500, 600]
    assert df['storage_class_id'].tolist() == [1, 2, 3, 4, 0, 0]


def test_entries_to_frame_empty():
    df = bulk.entries_to_frame('b1', [])
    assert list(df.columns) == ['bucket', 'name', 'size_bytes', 'created', 'storage_class_id']
    assert len(df) == 0


# ---------- split_hot_prefixes ----------

def test_split_hot_prefixes_ignores_cool_prefixes(tmp_path: Path):
    # Build a listing parquet with all names under 'hot/'; 'cool/' is < ideal.
    hot_names = [f'hot/obj{i:06d}' for i in range(1000)]
    cool_names = [f'cool/obj{i:04d}' for i in range(10)]
    weights_pq = tmp_path / 'w.parquet'
    pd.DataFrame({'bucket': 'b', 'name': hot_names + cool_names}).to_parquet(weights_pq)
    weights = {'hot/': 1000, 'cool/': 10}
    items, out_w = bulk.split_hot_prefixes(
        ['hot/', 'cool/'], weights, str(weights_pq), n_streams=4,
    )
    # cool/ passes through as-is; hot/ is split into <=4 ranges
    prefixes_in_items = [i if isinstance(i, str) else i[0] for i in items]
    assert 'cool/' in prefixes_in_items
    hot_ranges = [i for i in items if not isinstance(i, str) and i[0] == 'hot/']
    assert len(hot_ranges) >= 2  # actually split
    # Ranges cover the whole prefix contiguously: edges alternate None, name, name, ..., None
    first = hot_ranges[0]
    last = hot_ranges[-1]
    assert first[1] is None  # inclusive start of first range
    assert last[2] is None   # exclusive end of last range


def test_split_hot_prefixes_below_threshold_untouched(tmp_path: Path):
    weights_pq = tmp_path / 'w.parquet'
    pd.DataFrame({'bucket': 'b', 'name': [f'p/{i}' for i in range(10)]}).to_parquet(weights_pq)
    # Only one prefix → ideal = total → k < 2 → no split
    items, out_w = bulk.split_hot_prefixes(['p/'], {'p/': 10}, str(weights_pq), n_streams=1)
    assert items == ['p/']


# ---------- prefix_weights ----------

def test_prefix_weights_from_listing_parquet(tmp_path: Path):
    weights_pq = tmp_path / 'listing.parquet'
    names = [
        'foo/bar/a.txt', 'foo/bar/b.txt', 'foo/bar/c.txt',   # foo/ = 3, foo/bar/ = 3
        'foo/baz/d.txt',                                      # foo/ + 1, foo/baz/ = 1
        'top-level',                                          # no slash → excluded
        'x/y/z.txt',                                          # x/ = 1, x/y/ = 1
    ]
    pd.DataFrame({'bucket': 'b1', 'name': names}).to_parquet(weights_pq)
    weights = bulk.prefix_weights(str(weights_pq))
    assert weights == {
        'foo/': 4,
        'foo/bar/': 3,
        'foo/baz/': 1,
        'x/': 1,
        'x/y/': 1,
    }


# ---------- resolve_existing ----------

def test_resolve_existing_returns_none_when_empty(tmp_path: Path):
    import fsspec

    out_fs, out_root = fsspec.core.url_to_fs(str(tmp_path / 'listing'))
    out_fs.makedirs(out_root, exist_ok=True)
    assert bulk.resolve_existing(out_fs, out_root, 'error') is None


def test_resolve_existing_raises_when_shards_present_and_error(tmp_path: Path):
    import fsspec

    d = tmp_path / 'listing'
    d.mkdir()
    (d / 'shard-00.parquet').write_bytes(b'')
    out_fs, out_root = fsspec.core.url_to_fs(str(d))
    with pytest.raises(ValueError, match='shards'):
        bulk.resolve_existing(out_fs, out_root, 'error')


def test_resolve_existing_reuses_when_marker_present(tmp_path: Path):
    import fsspec

    d = tmp_path / 'listing'
    d.mkdir()
    (d / 'shard-00.parquet').write_bytes(b'')
    (d / bulk.SUCCESS_MARKER).write_text(json.dumps({'bucket': 'b', 'objects': 42}))
    out_fs, out_root = fsspec.core.url_to_fs(str(d))
    payload = bulk.resolve_existing(out_fs, out_root, 'reuse')
    assert payload == {'bucket': 'b', 'objects': 42}
    # shards + marker preserved
    assert sorted(p.name for p in d.iterdir()) == [bulk.SUCCESS_MARKER, 'shard-00.parquet']


def test_resolve_existing_clears_partial_run_on_reuse(tmp_path: Path):
    import fsspec

    d = tmp_path / 'listing'
    d.mkdir()
    (d / 'shard-00.parquet').write_bytes(b'')
    # No _SUCCESS marker → partial run
    out_fs, out_root = fsspec.core.url_to_fs(str(d))
    assert bulk.resolve_existing(out_fs, out_root, 'reuse') is None
    assert list(d.iterdir()) == []


def test_resolve_existing_clears_when_clear(tmp_path: Path):
    import fsspec

    d = tmp_path / 'listing'
    d.mkdir()
    (d / 'shard-00.parquet').write_bytes(b'')
    (d / bulk.SUCCESS_MARKER).write_text(json.dumps({'bucket': 'b', 'objects': 5}))
    out_fs, out_root = fsspec.core.url_to_fs(str(d))
    assert bulk.resolve_existing(out_fs, out_root, 'clear') is None
    assert list(d.iterdir()) == []


# ---------- end-to-end via a fake lister ----------

@dataclass(frozen=True)
class _FakeFs:
    """Minimal fsspec-like double for `generic_discover`."""
    tree: dict  # {'bucket/foo/': [{'name': 'bucket/foo/x', 'type': 'file', ...}, ...]}

    def ls(self, path: str, detail: bool = True):
        return self.tree[path.rstrip('/') + '/']


@dataclass(frozen=True)
class _FakeLister:
    """In-memory :class:`bulk.BulkLister` seeded from a `{prefix: [rows]}` dict."""
    scheme: str = "fake"
    contents: dict = None  # {prefix: [(name, size, created, storage_class)]}

    def stream_prefix(self, bucket, prefix, start, end):
        for name, size, created, storage_class in (self.contents or {}).get(prefix, []):
            if start is not None and name < start:
                continue
            if end is not None and name >= end:
                continue
            yield bulk.BlobRow(name=name, size=size, created=created, storage_class=storage_class)

    def placeholder_rows(self, bucket, self_dirs):
        return []


def test_end_to_end_bulk_list_shards_and_writes_success(tmp_path: Path):
    """Fake lister + fake fs → sharded listing parquet + success marker."""
    # Two depth-2 prefixes under bucket root; both stream single-file bodies.
    fs = _FakeFs(tree={
        'b1/': [
            {'name': 'b1/foo', 'type': 'directory'},
            {'name': 'b1/bar', 'type': 'directory'},
        ],
        'b1/foo/': [
            {'name': 'b1/foo/x', 'type': 'directory'},
        ],
        'b1/bar/': [
            {'name': 'b1/bar/y', 'type': 'directory'},
        ],
        'b1/foo/x/': [],
        'b1/bar/y/': [],
    })
    ts = "2026-08-05T00:00:00Z"
    lister = _FakeLister(contents={
        'foo/x/': [('foo/x/a.txt', 100, ts, 'STANDARD'), ('foo/x/b.txt', 200, ts, 'STANDARD')],
        'bar/y/': [('bar/y/c.txt', 300, ts, 'STANDARD')],
    })
    out_dir = str(tmp_path / 'listing')
    total = bulk.list_bucket_to_parquet(
        lister=lister, bucket='b1', out_dir=out_dir, fs=fs,
        procs=1, threads=1, exists='error',
    )
    assert total == 3
    files = sorted(p.name for p in Path(out_dir).iterdir())
    assert bulk.SUCCESS_MARKER in files
    shards = [f for f in files if f.endswith('.parquet')]
    assert shards, files
    # All shard contents combined should recreate the 3 rows.
    frames = [pd.read_parquet(Path(out_dir) / f) for f in shards]
    combined = pd.concat(frames, ignore_index=True).sort_values('name').reset_index(drop=True)
    assert combined['name'].tolist() == ['bar/y/c.txt', 'foo/x/a.txt', 'foo/x/b.txt']
    assert combined['size_bytes'].tolist() == [300, 100, 200]
    assert combined['bucket'].tolist() == ['b1', 'b1', 'b1']
    # And the marker payload records the same total.
    marker = json.loads((Path(out_dir) / bulk.SUCCESS_MARKER).read_text())
    assert marker == {'bucket': 'b1', 'prefix': None, 'objects': 3}


def test_end_to_end_bulk_list_reuse_short_circuits(tmp_path: Path):
    """A completed run + `exists=reuse` skips listing entirely."""
    fs = _FakeFs(tree={'b1/': []})
    lister = _FakeLister(contents={})
    out_dir = tmp_path / 'listing'
    out_dir.mkdir()
    (out_dir / 'shard-00.parquet').write_bytes(b'')
    (out_dir / bulk.SUCCESS_MARKER).write_text(json.dumps({'bucket': 'b1', 'objects': 999}))
    total = bulk.list_bucket_to_parquet(
        lister=lister, bucket='b1', out_dir=str(out_dir), fs=fs,
        procs=1, threads=1, exists='reuse',
    )
    assert total == 999
    # Marker + shard untouched.
    assert (out_dir / bulk.SUCCESS_MARKER).exists()
    assert (out_dir / 'shard-00.parquet').exists()


# ---------- CLI wiring (`disk-tree bulk-list`) ----------

def test_cli_bulk_list_rejects_local_path():
    """`disk-tree bulk-list /some/local/path` must fail with a clear error."""
    import os, subprocess, sys

    r = subprocess.run(
        [sys.executable, '-m', 'disk_tree.cli.main', 'bulk-list',
         '-o', '/tmp/should-not-be-used', '/local/path'],
        capture_output=True, text=True, check=False, env={**os.environ},
    )
    assert r.returncode != 0
    assert 'requires a cloud URI' in r.stderr


def test_cli_bulk_list_s3_not_implemented():
    """S3 lister is a follow-up; the CLI should say so, not crash silently."""
    import os, subprocess, sys

    r = subprocess.run(
        [sys.executable, '-m', 'disk_tree.cli.main', 'bulk-list',
         '-o', '/tmp/should-not-be-used', 's3://some-bucket'],
        capture_output=True, text=True, check=False, env={**os.environ},
    )
    assert r.returncode != 0
    # NotImplementedError bubbles to stderr
    assert "isn't wired yet" in r.stderr
