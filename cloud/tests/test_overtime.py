"""Specs for the cross-scan over-time index (`dt_cloud.overtime`): synthetic
per-scan `path-index`es → an SCD-2 interval table whose runs reconstruct every
scan's `(depth, path)` totals, with a synthesized depth-0 fleet root."""
import json
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from click.testing import CliRunner

from dt_cloud import overtime as OT
from dt_cloud.cli import main


def _write_path_index(path: Path, rows: list[tuple]) -> None:
    """(path, depth, usr, b, o) — the columns the over-time roll-up reads."""
    tbl = pa.table({
        "path": [r[0] for r in rows],
        "depth": pa.array([r[1] for r in rows], pa.int32()),
        "usr": pa.array([r[2] for r in rows], pa.string()),
        "b": pa.array([r[3] for r in rows], pa.int64()),
        "o": pa.array([r[4] for r in rows], pa.int64()),
    })
    pq.write_table(tbl, path)


B = "marin-us-east-02a"

# Four scans, oldest first. Exercises: a changing value (B), a long static run
# (B/a, split into two owner slices at s0 that must sum), and an absence gap
# (B/b present only s1–s2). Fleet root (depth 0) = the depth-1 rows summed.
SCANS = {
    "s0": [(B, 1, None, 100, 10), (f"{B}/a", 2, "u1", 30, 3), (f"{B}/a", 2, "u2", 20, 2)],
    "s1": [(B, 1, None, 100, 10), (f"{B}/a", 2, None, 50, 5), (f"{B}/b", 2, None, 7, 1)],
    "s2": [(B, 1, None, 200, 20), (f"{B}/a", 2, None, 50, 5), (f"{B}/b", 2, None, 7, 1)],
    "s3": [(B, 1, None, 200, 20), (f"{B}/a", 2, None, 50, 5)],
}


def _intervals(path: Path) -> list[tuple]:
    return duckdb.connect().execute(
        f"SELECT depth, path, b, o, {OT.SCAN_LO}, {OT.SCAN_HI} "
        f"FROM read_parquet('{path}') ORDER BY depth, path, {OT.SCAN_LO}"
    ).fetchall()


def test_write_over_time_index(tmp_path: Path):
    scans = []
    for sid, rows in SCANS.items():
        pi = tmp_path / f"{sid}.parquet"
        _write_path_index(pi, rows)
        scans.append((sid, str(pi)))
    s = OT.write_over_time_index(scans, tmp_path / "ot", mem="1GB", threads=2)

    assert s == {
        "rows": 6,
        "paths": 4,
        "scans": ["s0", "s1", "s2", "s3"],
        "intervals_per_path": 6 / 4,
        "file": str(tmp_path / "ot" / "over-time.parquet"),
        "scans_file": str(tmp_path / "ot" / "over-time.scans.json"),
    }
    assert json.loads((tmp_path / "ot" / "over-time.scans.json").read_text()) == ["s0", "s1", "s2", "s3"]
    # SCD-2 runs (pyrmts' interval kernel): fleet root + B change at s2, B/a
    # static, B/b only s1–s2.
    assert _intervals(tmp_path / "ot" / "over-time.parquet") == [
        (0, "", 100, 10, 0, 1),
        (0, "", 200, 20, 2, 3),
        (1, B, 100, 10, 0, 1),
        (1, B, 200, 20, 2, 3),
        (2, f"{B}/a", 50, 5, 0, 3),
        (2, f"{B}/b", 7, 1, 1, 2),
    ]


def test_intervals_reconstruct_each_scan(tmp_path: Path):
    """Every scan `k`'s `(depth, path)` totals = the intervals covering `k`."""
    scans = []
    for sid, rows in SCANS.items():
        pi = tmp_path / f"{sid}.parquet"
        _write_path_index(pi, rows)
        scans.append((sid, str(pi)))
    OT.write_over_time_index(scans, tmp_path / "ot", mem="1GB", threads=2)
    ivals = _intervals(tmp_path / "ot" / "over-time.parquet")

    for k, (sid, rows) in enumerate(SCANS.items()):
        expected = {}
        for p, d, _usr, b, o in rows:
            e = expected.setdefault((d, p), [0, 0])
            e[0] += b
            e[1] += o
        # add the synthesized fleet root (sum of depth-1)
        root = [0, 0]
        for (d, _p), (b, o) in expected.items():
            if d == 1:
                root[0] += b
                root[1] += o
        expected[(0, "")] = root
        got = {
            (d, p): [b, o]
            for d, p, b, o, lo, hi in ivals
            if lo <= k <= hi
        }
        assert got == expected, f"scan {sid} (idx {k})"


def test_write_over_time_groups(tmp_path: Path):
    """Fixed K=2 groups: two self-contained MSs, each with its own scan list and
    interval bounds indexing that group (0-based)."""
    scans = []
    for sid, rows in SCANS.items():
        pi = tmp_path / f"{sid}.parquet"
        _write_path_index(pi, rows)
        scans.append((sid, str(pi)))
    s = OT.write_over_time_groups(scans, tmp_path / "g", group_size=2)
    assert s["group_size"] == 2
    assert [(g["group"], g["first"], g["last"], g["n"]) for g in s["groups"]] == [
        ("s1", "s0", "s1", 2),
        ("s3", "s2", "s3", 2),
    ]
    # group s0–s1: B/b appears at s1 only; bounds are 0-based within the group.
    assert json.loads((tmp_path / "g" / "s1" / "over-time.scans.json").read_text()) == ["s0", "s1"]
    assert _intervals(tmp_path / "g" / "s1" / "over-time.parquet") == [
        (0, "", 100, 10, 0, 1),
        (1, B, 100, 10, 0, 1),
        (2, f"{B}/a", 50, 5, 0, 1),
        (2, f"{B}/b", 7, 1, 1, 1),
    ]
    # group s2–s3: B/b present s2, gone s3.
    assert json.loads((tmp_path / "g" / "s3" / "over-time.scans.json").read_text()) == ["s2", "s3"]
    assert _intervals(tmp_path / "g" / "s3" / "over-time.parquet") == [
        (0, "", 200, 20, 0, 1),
        (1, B, 200, 20, 0, 1),
        (2, f"{B}/a", 50, 5, 0, 1),
        (2, f"{B}/b", 7, 1, 0, 0),
    ]


def test_cli_over_time_write_explicit(tmp_path: Path):
    """`over-time-write <date>=<parquet> …` bypasses D1 and builds the index."""
    args = []
    for sid, rows in SCANS.items():
        pi = tmp_path / f"{sid}.parquet"
        _write_path_index(pi, rows)
        args.append(f"{sid}={pi}")
    out = tmp_path / "ot"
    r = CliRunner().invoke(main, ["over-time-write", "-o", str(out), *args])
    assert r.exit_code == 0, r.output
    summary = json.loads(r.output.strip().splitlines()[-1])
    assert summary["rows"] == 6
    assert summary["paths"] == 4
    assert summary["scans"] == ["s0", "s1", "s2", "s3"]
    assert _intervals(out / "over-time.parquet") == [
        (0, "", 100, 10, 0, 1),
        (0, "", 200, 20, 2, 3),
        (1, B, 100, 10, 0, 1),
        (1, B, 200, 20, 2, 3),
        (2, f"{B}/a", 50, 5, 0, 3),
        (2, f"{B}/b", 7, 1, 1, 2),
    ]
