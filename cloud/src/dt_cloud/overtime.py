"""The cross-scan **over-time index** — one path's `(b, o)` at every scan, laid
out so the size-over-time chart reads a whole line in one contiguous range
instead of one point read per scan generation (specs/obs-axis-indexing.md
Phase 1).

Unlike the per-scan tiers (`dt_cloud.index`), this index spans the *observation
axis*: it stacks all published scans' `path-index`es, rolled to `(depth, path)`
totals (owner slices summed away — over-time is `(path)`-granular), and encodes
each path's value stream as **SCD-2 change intervals** — pyrmts'
`multi-scan-consolidation` layout (`__scan_lo`/`__scan_hi`, indices into the
ordered scan list). Storage is ~static (real cw churn ~0.02 %/scan), so the
interval table is ~one path-index's size for the whole history, and a run
boundary in a scan span is the diff-index Phase 2 will read too — one encoding,
two uses (`series_for` / `diff_scans`).

Rows `(depth, path, b, o, __scan_lo, __scan_hi)`, sorted `(depth, path,
__scan_lo)` — a path's intervals contiguous, RG-pruned by `path`. The ordered
scan list (index → scan id) rides in the parquet KV metadata as `scans` (JSON)
so the reader maps interval bounds back to dates. A depth-0 fleet-root row
(`path=''`) makes the whole-store series a point lookup, as on the age pyramid.
"""
from __future__ import annotations

import json
import sys
from functools import partial
from pathlib import Path

import duckdb

from .index import ROW_GROUP_SIZE

err = partial(print, file=sys.stderr)

OVER_TIME_VARIANT = "over-time"
OVER_TIME_FILE = "over-time.parquet"
#: The ordered scan list rides both the parquet KV metadata (for a query
#: engine reading the file directly) and a sidecar JSON the CFW reader fetches
#: — the D1-footer read path rebuilds row-group stats but not KV metadata, so
#: the reader needs this to map `__scan_lo`/`__scan_hi` back to scan ids.
OVER_TIME_SCANS = "over-time.scans.json"
#: The KV-metadata schema tag the reader checks before trusting the layout.
OVER_TIME_SCHEMA = "over-time-v1"
SCAN_LO = "__scan_lo"
SCAN_HI = "__scan_hi"


def _scan_totals_sql(path_index: str, si: int) -> str:
    """One scan's `path-index` rolled to `(depth, path)` totals, tagged with its
    scan index `si` — owner slices summed away."""
    return (
        f"SELECT depth, path, sum(b)::BIGINT AS b, sum(o)::BIGINT AS o, {si} AS si "
        f"FROM read_parquet('{path_index}') GROUP BY depth, path"
    )


def write_over_time_index(
    scans: list[tuple[str, str | Path]],
    out_dir: str | Path,
    *,
    mem: str = "8GB",
    threads: int = 8,
    tmp_dir: str | Path | None = None,
    con: "duckdb.DuckDBPyConnection | None" = None,
) -> dict:
    """Write `over-time.parquet` under ``out_dir`` from ``scans`` — an ordered
    ``(scan_id, path_index_parquet)`` list, **oldest first**. Each path's
    `(b, o)` stream is SCD-2 interval-encoded over the scan axis; a path absent
    from a scan simply has no interval covering it (a gap, distinct from `b=0`).

    Returns ``{rows, scans, intervals_per_path, floor, file}`` (``scans`` is the
    ordered id list the bounds index into)."""
    if not scans:
        raise ValueError("write_over_time_index: no scans")
    ids = [s for s, _ in scans]
    if len(set(ids)) != len(ids):
        raise ValueError(f"write_over_time_index: duplicate scan id in {ids}")
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    out_path = out / OVER_TIME_FILE
    own = con is None
    if own:
        con = duckdb.connect()
        con.execute(f"SET memory_limit='{mem}'; SET threads={threads}")
        con.execute(f"SET temp_directory='{tmp_dir or out / '.duckdb-tmp'}'")
    try:
        union = " UNION ALL ".join(
            f"({_scan_totals_sql(str(pi), i)})" for i, (_, pi) in enumerate(scans)
        )
        con.execute(f"CREATE TEMP TABLE ot AS {union}")
        # A depth-0 fleet-root row per scan (sum of the buckets at depth 1), so
        # the whole-store series is a single-key read like every drilled path.
        con.execute(
            "INSERT INTO ot SELECT 0, '', sum(b)::BIGINT, sum(o)::BIGINT, si "
            "FROM ot WHERE depth = 1 GROUP BY si"
        )
        # SCD-2 encode: a new run starts on a value change or a scan-index gap
        # (absence). `grp` islands consecutive scans holding a constant `(b, o)`.
        interval_sql = f"""
        WITH marked AS (
            SELECT depth, path, b, o, si,
                CASE WHEN lag(b) OVER w IS DISTINCT FROM b
                       OR lag(o) OVER w IS DISTINCT FROM o
                       OR lag(si) OVER w IS DISTINCT FROM si - 1
                     THEN 1 ELSE 0 END AS brk
            FROM ot
            WINDOW w AS (PARTITION BY depth, path ORDER BY si)
        ),
        runs AS (
            SELECT depth, path, b, o, si,
                sum(brk) OVER (PARTITION BY depth, path ORDER BY si
                               ROWS UNBOUNDED PRECEDING) AS run
            FROM marked
        )
        SELECT depth, path, b, o,
               min(si) AS {SCAN_LO}, max(si) AS {SCAN_HI}
        FROM runs GROUP BY depth, path, run, b, o
        """
        scans_json = json.dumps(ids).replace("'", "''")
        kv = (
            f"(FORMAT parquet, ROW_GROUP_SIZE {ROW_GROUP_SIZE}, "
            f"KV_METADATA {{schema: '{OVER_TIME_SCHEMA}', scans: '{scans_json}'}})"
        )
        con.execute(
            f"COPY (SELECT * FROM ({interval_sql}) ORDER BY depth, path, {SCAN_LO}) "
            f"TO '{out_path}' {kv}"
        )
        rows = con.execute(f"SELECT count(*) FROM ({interval_sql})").fetchone()[0]
        paths = con.execute(f"SELECT count(DISTINCT (depth, path)) FROM ({interval_sql})").fetchone()[0]
        con.execute("DROP TABLE ot")
    finally:
        if own:
            con.close()
    (out / OVER_TIME_SCANS).write_text(json.dumps(ids) + "\n")
    err(f"over-time: {rows:,} intervals over {len(ids)} scans / {paths:,} paths → {out_path}")
    return {
        "rows": int(rows),
        "paths": int(paths),
        "scans": ids,
        "intervals_per_path": (rows / paths) if paths else 0.0,
        "file": str(out_path),
        "scans_file": str(out / OVER_TIME_SCANS),
    }
