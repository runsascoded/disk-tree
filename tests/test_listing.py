"""Tests for listing-source normalization (raw object-listing + SII inventory schemas).

Ported from `oa/marin-gcs-usage/tests/test_listing.py` (2026-08-04).
"""

import datetime as dt
from pathlib import Path

import duckdb
import pandas as pd
import pytest

from disk_tree.listing import prepare_listing

TS1 = dt.datetime(2026, 7, 1, tzinfo=dt.timezone.utc)
TS2 = dt.datetime(2026, 7, 27, tzinfo=dt.timezone.utc)


@pytest.fixture
def scan(tmp_path: Path) -> str:
    """Raw object-listing schema: buckets b1 (stale copy) + c2 (only source for c2)."""
    path = tmp_path / "scan.parquet"
    pd.DataFrame(
        {
            "bucket": ["b1", "c2"],
            "name": ["x/stale.bin", "y/c2.bin"],
            "size_bytes": [11, 22],
            "created": [TS1, TS1],
            "storage_class_id": [1, 2],
        }
    ).to_parquet(path)
    return str(path)


@pytest.fixture
def sii(tmp_path: Path) -> str:
    """SII inventory schema for bucket b1, incl. a soft-deleted row to drop."""
    path = tmp_path / "sii.parquet"
    pd.DataFrame(
        {
            "bucket": ["b1", "b1", "b1"],
            "name": ["x/fresh.bin", "z/cold.bin", "gone.bin"],
            "size": [100, 200, 300],
            "timeCreated": [TS2, TS2, TS2],
            "timeDeleted": [None, None, TS2],
            "storageClass": ["STANDARD", "COLDLINE", "STANDARD"],
        }
    ).to_parquet(path)
    return str(path)


def rows(con: duckdb.DuckDBPyConnection, src: str) -> list[tuple]:
    # epoch() rather than raw TIMESTAMPTZ: materializing tz-aware values into
    # Python needs pytz, which the pipeline itself never requires
    return con.execute(
        f"SELECT bucket, name, size_bytes, epoch(created)::BIGINT, storage_class_id FROM {src} ORDER BY bucket, name"
    ).fetchall()


def test_sii_normalization(sii: str):
    con = duckdb.connect()
    assert rows(con, prepare_listing(con, (sii,))) == [
        ("b1", "x/fresh.bin", 100, int(TS2.timestamp()), 1),
        ("b1", "z/cold.bin", 200, int(TS2.timestamp()), 3),
    ]  # soft-deleted gone.bin dropped; class strings mapped to ids


def test_scan_passthrough(scan: str):
    con = duckdb.connect()
    assert rows(con, prepare_listing(con, (scan,))) == [
        ("b1", "x/stale.bin", 11, int(TS1.timestamp()), 1),
        ("c2", "y/c2.bin", 22, int(TS1.timestamp()), 2),
    ]


def test_multi_source_bucket_priority(sii: str, scan: str):
    # earlier source (SII) wins for b1; scan contributes only its c2 rows
    con = duckdb.connect()
    assert rows(con, prepare_listing(con, (sii, scan))) == [
        ("b1", "x/fresh.bin", 100, int(TS2.timestamp()), 1),
        ("b1", "z/cold.bin", 200, int(TS2.timestamp()), 3),
        ("c2", "y/c2.bin", 22, int(TS1.timestamp()), 2),
    ]


def test_unrecognized_schema_raises(tmp_path: Path):
    path = tmp_path / "junk.parquet"
    pd.DataFrame({"foo": [1]}).to_parquet(path)
    con = duckdb.connect()
    with pytest.raises(ValueError, match="unrecognized listing schema"):
        prepare_listing(con, (str(path),))
