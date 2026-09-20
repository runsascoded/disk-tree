"""Specs for the index tier writer (`dt_cloud.index`): a synthetic layer-2
parquet (disk-tree's names) → the site's row contract, bucket-prefixed and
sorted (depth, path), plus the coarse tiers' floors and membership."""
import math
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from dt_cloud import index as X

BUCKET = "marin-us-east-02a"


def _write_l2(path: Path, rows: list[tuple]) -> None:
    """(path, depth, kind, size, n_files, mtime_mean|None)."""
    tbl = pa.table({
        "path": [r[0] for r in rows],
        "depth": pa.array([r[1] for r in rows], pa.int32()),
        "kind": [r[2] for r in rows],
        "size": pa.array([r[3] for r in rows], pa.int64()),
        "n_files": pa.array([r[4] for r in rows], pa.int64()),
        "mtime_mean": pa.array([r[5] for r in rows], pa.float64()),
        "mtime": pa.array([0] * len(rows), pa.int64()),
    })
    pq.write_table(tbl, path)


GIB = 1024**3
# root 40 GiB: marin 30 (a 20 + b 10), tmp 10; files are excluded; the root's
# mtime_mean is NULL (no weighted time → wts/wb 0).
L2 = [
    (".", 0, "dir", 40 * GIB, 400, None),
    ("marin", 1, "dir", 30 * GIB, 300, 1_700_000_000.0),
    ("marin/a", 2, "dir", 20 * GIB, 200, 1_700_000_000.0),
    ("marin/b", 2, "dir", 10 * GIB, 100, 1_600_000_000.0),
    ("tmp", 1, "dir", 10 * GIB, 100, 1_500_000_000.0),
    ("marin/a/x.bin", 3, "file", 20 * GIB, 1, 1_700_000_000.0),
]


def _rows(path: Path) -> list[tuple]:
    return duckdb.connect().execute(f"SELECT path, depth, usr, b, o, wts, wb, c2, c3, c4, a FROM read_parquet('{path}') ORDER BY depth, path").fetchall()


def test_write_index(tmp_path: Path):
    l2 = tmp_path / "l2.parquet"
    _write_l2(l2, L2)
    s = X.write_index([(BUCKET, str(l2))], tmp_path / "index", mem="1GB", threads=2)
    # fleet = the bucket row = 40 GiB → log2 = 35.32 → 35; E=16 → 2^19 = 512 KiB, 20 → 32 KiB, 24 → 2 KiB
    pyr_files = {b: str(tmp_path / "index" / f"age-pyramid-{b}.parquet") for b in ("1d", "1mo", "1y")}
    assert s == {
        "rows": 5,
        "buckets": [BUCKET],
        "floors": {"16": 2**19, "20": 2**15, "24": 2**11},
        "paths": {"16": 5, "20": 5, "24": 5},
        # No file rows with mtime > 0 in the fixture, so the age pyramid is empty.
        "pyramid": {"floor": 1, "bins": {b: {"rows": 0, "file": pyr_files[b]} for b in ("1d", "1mo", "1y")}},
        "files": {
            **{v: str(tmp_path / "index" / f) for v, f in X.INDEX_VARIANTS.items()},
            **{f"age-pyramid-{b}": pyr_files[b] for b in ("1d", "1mo", "1y")},
        },
    }
    # the floor-free tier: dir rows only, bucket-prefixed, depth + 1, gcs's columns
    assert _rows(tmp_path / "index" / "path-index.parquet") == [
        (BUCKET, 1, None, 40 * GIB, 400, 0.0, 0, 0, 0, 0, None),
        (f"{BUCKET}/marin", 2, None, 30 * GIB, 300, 1_700_000_000.0 * 30 * GIB, 30 * GIB, 0, 0, 0, None),
        (f"{BUCKET}/tmp", 2, None, 10 * GIB, 100, 1_500_000_000.0 * 10 * GIB, 10 * GIB, 0, 0, 0, None),
        (f"{BUCKET}/marin/a", 3, None, 20 * GIB, 200, 1_700_000_000.0 * 20 * GIB, 20 * GIB, 0, 0, 0, None),
        (f"{BUCKET}/marin/b", 3, None, 10 * GIB, 100, 1_600_000_000.0 * 10 * GIB, 10 * GIB, 0, 0, 0, None),
    ]
    # a coarse tier carries its floor in the parquet key-value metadata and 8k-row groups
    md = pq.ParquetFile(tmp_path / "index" / "path-index-coarse16.parquet").metadata
    assert md.metadata[b"coarse_floor"] == b"524288"
    assert pq.ParquetFile(tmp_path / "index" / "path-index.parquet").metadata.row_group(0).num_rows == 5


def test_coarse_tier_membership(tmp_path: Path):
    # a fleet where the floors bite: 2 TiB root → E=24 floor 128 KiB (everything), E=20 2 MiB, E=16 32 MiB
    TIB = 1024**4
    rows = [
        (".", 0, "dir", 2 * TIB, 10, None),
        ("big", 1, "dir", 2 * TIB - 3 * (1 << 20), 8, 1e9),
        ("big/mid", 2, "dir", 3 * (1 << 20), 3, 1e9),  # 3 MiB: clears E=20 (2 MiB), not E=16 (32 MiB)
        ("small", 1, "dir", 3 * (1 << 20), 2, 1e9),
        ("small/tiny", 2, "dir", 1 << 20, 1, 1e9),  # 1 MiB: only E=24
    ]
    l2 = tmp_path / "l2.parquet"
    _write_l2(l2, rows)
    s = X.write_index([("b", str(l2))], tmp_path / "index", mem="1GB", threads=2)
    assert s["floors"] == {"16": 1 << 25, "20": 1 << 21, "24": 1 << 17}
    assert s["paths"] == {"16": 2, "20": 4, "24": 5}
    assert [r[0] for r in _rows(tmp_path / "index" / "path-index-coarse16.parquet")] == ["b", "b/big"]
    assert [r[0] for r in _rows(tmp_path / "index" / "path-index-coarse20.parquet")] == ["b", "b/big", "b/small", "b/big/mid"]


def test_write_index_two_buckets(tmp_path: Path):
    # a second bucket's rows join the same tiers under its own depth-1 row; the
    # fleet (floors) is both buckets' sum: 40 + 24 GiB = 64 GiB → log2 = 36 →
    # E=16 → 2^20, 20 → 2^16, 24 → 2^12
    HERO = [
        (".", 0, "dir", 24 * GIB, 60, None),
        ("tmp", 1, "dir", 24 * GIB, 60, 1_710_000_000.0),
        ("tmp/ttl=14d", 2, "dir", 24 * GIB, 60, 1_710_000_000.0),
    ]
    a, b = tmp_path / "a.parquet", tmp_path / "b.parquet"
    _write_l2(a, L2)
    _write_l2(b, HERO)
    s = X.write_index([(BUCKET, str(a)), ("hero-checkpoints", str(b))], tmp_path / "index", mem="1GB", threads=2)
    pyr_files = {b: str(tmp_path / "index" / f"age-pyramid-{b}.parquet") for b in ("1d", "1mo", "1y")}
    assert s == {
        "rows": 8,
        "buckets": [BUCKET, "hero-checkpoints"],
        "floors": {"16": 2**20, "20": 2**16, "24": 2**12},
        "paths": {"16": 8, "20": 8, "24": 8},
        # No file rows with mtime > 0 in the fixture, so the age pyramid is empty.
        "pyramid": {"floor": 1, "bins": {b: {"rows": 0, "file": pyr_files[b]} for b in ("1d", "1mo", "1y")}},
        "files": {
            **{v: str(tmp_path / "index" / f) for v, f in X.INDEX_VARIANTS.items()},
            **{f"age-pyramid-{b}": pyr_files[b] for b in ("1d", "1mo", "1y")},
        },
    }
    assert [(r[0], r[1], r[3]) for r in _rows(tmp_path / "index" / "path-index.parquet")] == [
        ("hero-checkpoints", 1, 24 * GIB),
        (BUCKET, 1, 40 * GIB),
        ("hero-checkpoints/tmp", 2, 24 * GIB),
        (f"{BUCKET}/marin", 2, 30 * GIB),
        (f"{BUCKET}/tmp", 2, 10 * GIB),
        ("hero-checkpoints/tmp/ttl=14d", 3, 24 * GIB),
        (f"{BUCKET}/marin/a", 3, 20 * GIB),
        (f"{BUCKET}/marin/b", 3, 10 * GIB),
    ]


def test_write_index_rejects_bad_sources(tmp_path: Path):
    with pytest.raises(ValueError, match="no \\(bucket, layer-2\\) sources"):
        X.write_index([], tmp_path / "index")
    with pytest.raises(ValueError, match="duplicate bucket"):
        X.write_index([("b", "x"), ("b", "y")], tmp_path / "index")


def test_coarse_floor():
    assert X.coarse_floor(0, 24) == 1
    assert X.coarse_floor(923_009_082_966_172, 24) == 2**26  # ~840 TiB → 2^50 → 64 MiB
    assert X.coarse_floor(2**40, 16) == 2**24
