"""The CoreWeave scan's index tiers (`dt-cloud index-write`), from the
layer-2 parquet `disk-tree import` writes (specs/view-serving.md §1, ported
from gcs's `viz.py` path-index + `write_coarse_tiers`).

The site reads one row format on every tier — `path, depth, usr, b, o, wts,
wb, c2, c3, c4, a`, descendant-inclusive, sorted `(depth, path)`, 8k-row
groups — through `functions/_lib/index.ts`, which is shared with gcs verbatim.
cw's layer-2 parquet has the same rows in disk-tree's names (`size`,
`n_files`, `mtime_mean`, …) in 262k-row groups, so this module rewrites it:

- **floor-free tier** `path-index.parquet`: every `dir` row, its path prefixed
  with the bucket (`marin-us-east-02a/marin/…`, depth + 1) so depth 1 is the
  bucket like gcs's fleet index and the site's "one bucket" root opens inside
  it; `b` = subtree bytes, `o` = subtree objects, `wts`/`wb` = the
  byte-weighted mtime sum and its weight (`mtime_mean · size`, `size`), so the
  reader's `d` (mean written day) falls out as on gcs; no attribution, no
  storage classes, no access log → `usr`/`a` NULL, `c2..c4` 0.
- **coarse tiers** `path-index-coarse<E>.parquet`, E ∈ COARSE_EXPS: the same
  rows restricted to paths whose subtree clears F_E = 2^(round(log2 fleet) − E),
  the floor in the parquet key-value metadata for `index-sync` to record. A
  tier, not a loss — the reader only answers a query from a tier whose floor
  is at or below the query's threshold.

No `by-user` sorts: CoreWeave has no ownership signal (the intended delta).
"""
from __future__ import annotations

import math
import os
import sys
from functools import partial
from pathlib import Path

import duckdb

err = partial(print, file=sys.stderr)

# Coarse tiers, coarsest first — the reader's COARSE_EXPS (`_lib/view.ts`).
# At a ~900 TiB bucket: E=16 → 16 GiB, E=20 → 1 GiB, E=24 → 64 MiB floors.
COARSE_EXPS = (16, 20, 24)
# Variant → file name, what `index-sync` publishes and `_lib/index.ts`
# `indexKey` resolves (D1 keys (date, variant)).
INDEX_VARIANTS: dict[str, str] = {"path": "path-index.parquet"}
for _e in COARSE_EXPS:
    INDEX_VARIANTS[f"coarse{_e}"] = f"path-index-coarse{_e}.parquet"

# The per-path created-day strata behind a path-aware `AgeChart` (specs/age-index.md).
# A distinct index (not a path-index tier): rows `(path, depth, day, b, o)` sorted
# `(depth, path, day)`, floored, served by prefix as a point lookup. Registered as
# variant `age` in `index_footer.INDEX_VARIANTS` too (what `index-sync` publishes).
AGE_INDEX = "age-index.parquet"
# The age index floors at the finest coarse tier: every prefix the treemap can drill
# to as a page is covered; below it `/api/age` falls back to the nearest ancestor.
AGE_FLOOR_EXP = max(COARSE_EXPS)

ROW_GROUP_SIZE = 8192
# The index rows, in the site's column contract (`_lib/index.ts` `Row`).
INDEX_COLS = "path, depth, usr, b, o, wts::DOUBLE AS wts, wb, c2, c3, c4, a"


def index_rows_sql(l2: str, bucket: str) -> str:
    """The floor-free index rows as a DuckDB SELECT over the layer-2 parquet
    (`L2` DuckDB variable or a literal path expression): dir rows only (the
    `.` root becomes the bucket row), bucket-prefixed paths, gcs's names."""
    return f"""
        SELECT
          CASE WHEN path = '.' THEN '{bucket}' ELSE '{bucket}/' || path END AS path,
          depth + 1 AS depth,
          NULL::VARCHAR AS usr,
          size::BIGINT AS b,
          n_files::BIGINT AS o,
          CASE WHEN mtime_mean IS NULL THEN 0 ELSE (mtime_mean::DECIMAL(38,0) * size::DECIMAL(38,0)) END AS wts,
          CASE WHEN mtime_mean IS NULL THEN 0::BIGINT ELSE size::BIGINT END AS wb,
          0::BIGINT AS c2, 0::BIGINT AS c3, 0::BIGINT AS c4,
          NULL::INTEGER AS a
        FROM read_parquet({l2})
        WHERE kind = 'dir'
    """


def coarse_floor(fleet: int, e: int) -> int:
    """F_E = 2^(round(log2 fleet) − E); 1 for an empty fleet."""
    return 2 ** (round(math.log2(fleet)) - e) if fleet > 0 else 1


def write_coarse_tiers(con: "duckdb.DuckDBPyConnection", path_index: Path, rows: str) -> tuple[dict[int, int], dict[int, int]]:
    """Write the coarse tiers beside ``path_index`` from ``rows`` (a relation
    of index rows); a temp table ``tot`` (``path, pb``) must exist. Returns
    ({E: floor}, {E: kept paths}). gcs's `viz.write_coarse_tiers` minus the
    by-user sort."""
    floors: dict[int, int] = {}
    counts: dict[int, int] = {}
    fleet = int(con.execute("SELECT coalesce(sum(pb), 0) FROM tot WHERE depth = 1").fetchone()[0])
    n_paths = con.execute("SELECT count(*) FROM tot").fetchone()[0]
    for e in COARSE_EXPS:
        floor = coarse_floor(fleet, e)
        floors[e] = floor
        con.execute(f"CREATE TEMP TABLE coarse AS SELECT path FROM tot WHERE pb >= {floor}")
        counts[e] = con.execute("SELECT count(*) FROM coarse").fetchone()[0]
        kv = f"(FORMAT parquet, ROW_GROUP_SIZE {ROW_GROUP_SIZE}, KV_METADATA {{coarse_floor: '{floor}'}})"
        out = path_index.with_name(f"path-index-coarse{e}.parquet")
        con.execute(f"COPY (SELECT {INDEX_COLS} FROM {rows} r JOIN coarse USING (path) ORDER BY depth, path) TO '{out}' {kv}")
        con.execute("DROP TABLE coarse")
        err(f"coarse tier E={e}: floor {floor:,} B, {counts[e]:,} of {n_paths:,} paths")
    return floors, counts


def age_rows_sql(l2: str, bucket: str) -> str:
    """Per-`(prefix, created-day)` bytes/objects from a bucket's layer-2 **file**
    rows, descendant-inclusive: each file's `mtime`-day-bucketed size/count rolled
    up to every ancestor dir prefix (bucket-prefixed, `depth + 1`, so depth 1 is
    the bucket — same key space as the path-index). Two-stage — aggregate files to
    `(parent-dir, day)` first, then explode that to ancestors — so the intermediate
    is distinct-dirs × days, not files × depth."""
    return f"""
        WITH files AS (
          SELECT
            CASE WHEN path LIKE '%/%' THEN regexp_replace(path, '/[^/]*$', '') ELSE '' END AS pdir,
            CAST(mtime / 86400 AS BIGINT) AS day,
            size::BIGINT AS b
          FROM read_parquet({l2})
          WHERE kind = 'file' AND mtime > 0
        ),
        leaf AS (
          SELECT pdir, day, sum(b) AS b, count(*) AS o FROM files GROUP BY pdir, day
        ),
        comps AS (
          SELECT day, b, o,
                 CASE WHEN pdir = '' THEN []::VARCHAR[] ELSE string_split(pdir, '/') END AS parts
          FROM leaf
        ),
        expl AS (
          SELECT day, b, o, parts, unnest(range(0, len(parts) + 1)) AS i FROM comps
        )
        SELECT
          '{bucket}' || CASE WHEN i = 0 THEN '' ELSE '/' || array_to_string(parts[1:i], '/') END AS path,
          (i + 1)::INTEGER AS depth,
          day::BIGINT AS day,
          b::BIGINT AS b,
          o::BIGINT AS o
        FROM expl
    """


def write_age_index(
    con: "duckdb.DuckDBPyConnection",
    sources: list[tuple[str, str]],
    out_dir: Path,
) -> dict:
    """Write `age-index.parquet` under ``out_dir`` from ``sources`` (the same
    `(bucket, layer-2 parquet)` pairs as the path-index), on the caller's ``con``.
    Rows are the UNION of each bucket's `(prefix, created-day)` strata, grouped,
    floored to prefixes clearing the finest coarse tier's byte floor, and sorted
    `(depth, path, day)` — the prefix-range + row-group-prune contract of the
    path-index, so `/api/age?path=P` is a point lookup on P's own day rows.
    Returns a summary (rows, floor, kept paths, file)."""
    out = Path(out_dir)
    selects = []
    for i, (bucket, l2_path) in enumerate(sources):
        con.execute(f"SET VARIABLE AGE_L2_{i} = ?", [str(l2_path)])
        selects.append(age_rows_sql(f"getvariable('AGE_L2_{i}')", bucket))
    con.execute(
        "CREATE TEMP TABLE age AS "
        + " UNION ALL ".join(f"({s})" for s in selects)
    )
    # Cast the sums back to BIGINT: DuckDB's SUM(BIGINT) is HUGEINT, which parquet
    # stores without the int64 min/max stats the D1 footer + reader require.
    con.execute("CREATE TEMP TABLE age_agg AS SELECT path, depth, sum(b)::BIGINT AS b, sum(o)::BIGINT AS o, day FROM age GROUP BY path, depth, day")
    # A depth-0 fleet-root row (path '') summing every bucket per day, so the
    # multi-bucket root view is a single point lookup like any drilled prefix
    # (the buckets are the depth-1 rows; summing them counts each file once).
    con.execute("INSERT INTO age_agg SELECT '', 0, sum(b)::BIGINT, sum(o)::BIGINT, day FROM age_agg WHERE depth = 1 GROUP BY day")
    fleet = int(con.execute("SELECT coalesce(sum(b), 0) FROM age_agg WHERE depth = 1").fetchone()[0])
    floor = max(1, int(coarse_floor(fleet, AGE_FLOOR_EXP)))
    # Per-path total bytes (over all days) decides what clears the floor.
    con.execute(f"CREATE TEMP TABLE age_keep AS SELECT path FROM age_agg GROUP BY path HAVING sum(b) >= {floor}")
    kept = con.execute("SELECT count(*) FROM age_keep").fetchone()[0]
    all_paths = con.execute("SELECT count(DISTINCT path) FROM age_agg").fetchone()[0]
    out_path = out / AGE_INDEX
    kv = f"(FORMAT parquet, ROW_GROUP_SIZE {ROW_GROUP_SIZE}, KV_METADATA {{coarse_floor: '{floor}'}})"
    n = con.execute(
        f"COPY (SELECT a.path, a.depth, a.day, a.b, a.o FROM age_agg a JOIN age_keep USING (path) "
        f"ORDER BY a.depth, a.path, a.day) TO '{out_path}' {kv}"
    )
    n_rows = con.execute("SELECT count(*) FROM age_agg a JOIN age_keep USING (path)").fetchone()[0]
    con.execute("DROP TABLE age")
    con.execute("DROP TABLE age_agg")
    con.execute("DROP TABLE age_keep")
    err(f"age-index: {n_rows:,} rows, {kept:,} of {all_paths:,} paths ≥ floor {floor:,} B → {out_path}")
    return {"rows": int(n_rows), "floor": floor, "paths": int(kept), "file": str(out_path)}


# --- Phase B: multi-scale time pyramid (specs/age-index.md) ------------------
# Path-major tiers (one parquet per bin), reusing pyrmts's planner + our
# D1-footer reader at serve time (DIY sum-combine, so the producer stays our own
# DuckDB — no pyrmts Python dep). Bins finest→coarsest; the base (first) is
# exploded once, coarser bins re-bin from it (pyrmts `cascade_tiers` in SQL).
# Day base: CW's history is day-granular and a 1h base-tier explode over ~92M
# objects is a needless cost; add "1h" if finer zoom is ever wanted (the serve
# tier list in `site/functions/_lib/agePyramid.ts` must match).
AGE_PYRAMID_BINS = ("1d", "1mo", "1y")
# Variant name per pyramid bin (what `index-sync`/`indexKey` resolve).
AGE_PYRAMID_VARIANTS = {b: f"age-pyramid-{b}" for b in AGE_PYRAMID_BINS}


def _binstart_ms_sql(bin: str, secs: str) -> str:
    """Epoch-**ms** bucket start (pyrmts `binCol` convention) for the epoch-**seconds**
    int expression ``secs`` at ``bin`` (fixed-width `Nmin|Nh|Nd`, or calendar
    `1mo`/`1y` via `date_trunc`)."""
    if bin.endswith("min"):
        n = int(bin[:-3]) * 60
    elif bin.endswith("mo"):
        return f"epoch_ms(date_trunc('month', to_timestamp({secs})))"
    elif bin.endswith("h"):
        n = int(bin[:-1]) * 3600
    elif bin.endswith("d"):
        n = int(bin[:-1]) * 86400
    elif bin.endswith("y"):
        return f"epoch_ms(date_trunc('year', to_timestamp({secs})))"
    else:
        raise ValueError(f"bad bin {bin!r}")
    return f"((({secs}) // {n}) * {n * 1000})"


def _age_explode_sql(l2: str, bucket: str, binstart_ms: str) -> str:
    """Per-`(prefix, time-bin)` bytes/objects from a bucket's layer-2 file rows,
    descendant-inclusive — `write_age_index`'s two-stage explode with an
    arbitrary time bin (`binstart_ms` = an epoch-ms expression over `mtime`)."""
    return f"""
        WITH files AS (
          SELECT
            CASE WHEN path LIKE '%/%' THEN regexp_replace(path, '/[^/]*$', '') ELSE '' END AS pdir,
            {binstart_ms} AS binstart,
            size::BIGINT AS b
          FROM read_parquet({l2})
          WHERE kind = 'file' AND mtime > 0
        ),
        leaf AS (
          SELECT pdir, binstart, sum(b) AS b, count(*) AS o FROM files GROUP BY pdir, binstart
        ),
        comps AS (
          SELECT binstart, b, o,
                 CASE WHEN pdir = '' THEN []::VARCHAR[] ELSE string_split(pdir, '/') END AS parts
          FROM leaf
        ),
        expl AS (
          SELECT binstart, b, o, parts, unnest(range(0, len(parts) + 1)) AS i FROM comps
        )
        SELECT
          '{bucket}' || CASE WHEN i = 0 THEN '' ELSE '/' || array_to_string(parts[1:i], '/') END AS path,
          (i + 1)::INTEGER AS depth,
          binstart::BIGINT AS binstart,
          b::BIGINT AS b,
          o::BIGINT AS o
        FROM expl
    """


def write_age_pyramid(
    con: "duckdb.DuckDBPyConnection",
    sources: list[tuple[str, str]],
    out_dir: str | Path,
    bins: tuple[str, ...] = AGE_PYRAMID_BINS,
) -> dict:
    """Write one path-major tier `age-pyramid-<bin>.parquet` per bin under
    ``out_dir`` from ``sources``. Explode once at the base (finest) bin; re-bin
    coarser tiers from it. Rows `(path, depth, binstart, b, o)` sorted
    `(depth, path, binstart)` — a path's whole history contiguous, RG-pruned by
    `path` (the drilled-path query is a prefix range). Floored per-path (total
    bytes ≥ the finest coarse-tier floor); a depth-0 fleet-root row per bin.
    Returns `{floor, bins: {bin: {rows, file}}}`."""
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    base = bins[0]
    selects = []
    for i, (bucket, l2_path) in enumerate(sources):
        con.execute(f"SET VARIABLE PYR_L2_{i} = ?", [str(l2_path)])
        selects.append(_age_explode_sql(f"getvariable('PYR_L2_{i}')", bucket, _binstart_ms_sql(base, "mtime")))
    con.execute("CREATE TEMP TABLE pyr AS " + " UNION ALL ".join(f"({s})" for s in selects))
    con.execute("CREATE TEMP TABLE pyr_base AS SELECT path, depth, sum(b)::BIGINT AS b, sum(o)::BIGINT AS o, binstart FROM pyr GROUP BY path, depth, binstart")
    con.execute("INSERT INTO pyr_base SELECT '', 0, sum(b)::BIGINT, sum(o)::BIGINT, binstart FROM pyr_base WHERE depth = 1 GROUP BY binstart")
    fleet = int(con.execute("SELECT coalesce(sum(b), 0) FROM pyr_base WHERE depth = 1").fetchone()[0])
    floor = max(1, int(coarse_floor(fleet, AGE_FLOOR_EXP)))
    con.execute(f"CREATE TEMP TABLE pyr_keep AS SELECT path FROM pyr_base GROUP BY path HAVING sum(b) >= {floor}")
    summ: dict[str, dict] = {}
    for bin in bins:
        out_path = out / f"age-pyramid-{bin}.parquet"
        if bin == base:
            sel = "SELECT b.path, b.depth, b.binstart, b.b, b.o FROM pyr_base b JOIN pyr_keep USING (path)"
        else:
            # re-bin the base's ms bucket start (ms // 1000 = exact seconds) up.
            rb = _binstart_ms_sql(bin, "(b.binstart // 1000)")
            sel = f"SELECT b.path, b.depth, {rb} AS binstart, sum(b.b)::BIGINT AS b, sum(b.o)::BIGINT AS o FROM pyr_base b JOIN pyr_keep USING (path) GROUP BY b.path, b.depth, {rb}"
        kv = f"(FORMAT parquet, ROW_GROUP_SIZE {ROW_GROUP_SIZE}, KV_METADATA {{coarse_floor: '{floor}', bin: '{bin}'}})"
        con.execute(f"COPY ({sel} ORDER BY depth, path, binstart) TO '{out_path}' {kv}")
        n = con.execute(f"SELECT count(*) FROM ({sel})").fetchone()[0]
        summ[bin] = {"rows": int(n), "file": str(out_path)}
        err(f"age-pyramid[{bin}]: {n:,} rows → {out_path}")
    con.execute("DROP TABLE pyr")
    con.execute("DROP TABLE pyr_base")
    con.execute("DROP TABLE pyr_keep")
    return {"floor": floor, "bins": summ}


def write_index(
    sources: list[tuple[str, str]],
    out_dir: str | Path,
    *,
    mem: str = "8GB",
    threads: int = 8,
    tmp_dir: str | Path | None = None,
) -> dict:
    """Write the floor-free `path-index.parquet` + the coarse tiers under
    ``out_dir`` from ``sources`` — one ``(bucket, layer-2 parquet)`` per
    bucket of the scan (specs/cw-multi-bucket.md §2): the rows are the UNION
    of each bucket's rows, so depth 1 holds every bucket and the coarse
    floors derive from their sum. Returns a summary (rows, buckets, floors,
    kept counts, files)."""
    if not sources:
        raise ValueError("write_index: no (bucket, layer-2) sources")
    buckets = [b for b, _ in sources]
    if len(set(buckets)) != len(buckets):
        raise ValueError(f"write_index: duplicate bucket in {buckets}")
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    path_index = out / INDEX_VARIANTS["path"]
    con = duckdb.connect()
    con.execute(f"SET memory_limit='{mem}'; SET threads={threads}")
    con.execute(f"SET temp_directory='{tmp_dir or out / '.duckdb-tmp'}'")
    selects = []
    for i, (bucket, l2_path) in enumerate(sources):
        con.execute(f"SET VARIABLE L2_{i} = ?", [str(l2_path)])
        selects.append(index_rows_sql(f"getvariable('L2_{i}')", bucket))
    con.execute(f"CREATE TEMP TABLE idx AS {' UNION ALL '.join(f'({s})' for s in selects)}")
    n = con.execute("SELECT count(*) FROM idx").fetchone()[0]
    con.execute(f"COPY (SELECT {INDEX_COLS} FROM idx ORDER BY depth, path) TO '{path_index}' (FORMAT parquet, ROW_GROUP_SIZE {ROW_GROUP_SIZE})")
    err(f"path-index: {n:,} rows → {path_index}")
    # Rows are descendant-inclusive already (disk-tree's dir sizes are subtree
    # sums), so a path's subtree bytes are its own `b`.
    con.execute("CREATE TEMP TABLE tot AS SELECT path, depth, b AS pb FROM idx")
    floors, counts = write_coarse_tiers(con, path_index, rows="idx")
    # The age chart's backend: the multi-scale pyramid (Phase B) supersedes the
    # single-bin `age-index.parquet` (Phase A). `write_age_index` is kept for
    # ad-hoc use but no longer produced by the job.
    pyramid = write_age_pyramid(con, sources, out)
    return {
        "rows": int(n),
        "buckets": buckets,
        "floors": {str(e): floors[e] for e in COARSE_EXPS},
        "paths": {str(e): counts[e] for e in COARSE_EXPS},
        "pyramid": pyramid,
        "files": {
            **{v: str(out / f) for v, f in INDEX_VARIANTS.items()},
            **{AGE_PYRAMID_VARIANTS[b]: s["file"] for b, s in pyramid["bins"].items()},
        },
    }
