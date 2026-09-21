"""Data extracts for the web viz (``dt-cloud webdata``).

Everything here is laptop-scale DuckDB over the deduped listing parquet:
a nested prefix tree for the treemap, created-day age strata, and a
small meta blob. Output is plain JSON consumed by ``site/``.
"""

from __future__ import annotations

import datetime as dt
import json
import math
import os
import resource
import sys
from collections import defaultdict
from functools import partial
from pathlib import Path

import duckdb

err = partial(print, file=sys.stderr)


def prefix_labels(
    con: "duckdb.DuckDBPyConnection",
    attributions: tuple[str, ...],
    identities_path: "Path | None",
    listing_src: str,
) -> "pd.DataFrame":
    """The attribution prefix map as rows ``(key, user, depth)`` — ``key`` is
    ``<bucket>[/<dir>…]`` (no ``gs://``, no trailing slash), deepest-prefix
    semantics applied by the caller (``webdata``'s per-depth joins, or DT's
    ``import --label``). Path-glob rules expand against ``listing_src``'s dirs
    (``(bucket, name)``); identities resolve to canonical users.

    Prefixes deeper than ``GCS_USAGE_ATTR_MAX_DEPTH`` (12) are dropped: the
    ancestor join is dirs × max depth, so a handful of ultra-deep prefixes
    inflate memory for everything (the 2026-08-26 wandb re-mine's 231
    depth-14+ config paths pushed it 13 → 16 and OOMed the 100GB REPROC); a
    truncated prefix would over-attribute whole parent dirs, so they go."""
    import pandas as pd

    from .identity import DEFAULT_IDENTITIES, load_identities
    from .prefixes import load_prefix_map

    identities = load_identities(identities_path or DEFAULT_IDENTITIES)
    by_prefix = load_prefix_map(con, attributions, identities, listing_src)
    pfx_df = pd.DataFrame(
        [{"key": k.removeprefix("gs://").rstrip("/"), "user": u, "source": source} for k, (u, source) in by_prefix.items()],
        columns=["key", "user", "source"],
    )
    pfx_df["depth"] = pfx_df["key"].str.count("/") + 1
    attr_max_depth = int(os.environ.get("GCS_USAGE_ATTR_MAX_DEPTH", "12"))
    deep = pfx_df["depth"] > attr_max_depth
    if deep.any():
        err(f"dropping {int(deep.sum())} attribution prefixes deeper than {attr_max_depth}")
        pfx_df = pfx_df[~deep]
    return pfx_df


def write_labels(
    con: "duckdb.DuckDBPyConnection",
    listings: tuple[str, ...],
    attributions: tuple[str, ...],
    identities_path: "Path | None",
    out_dir: "Path",
) -> dict[str, int]:
    """DT's label tables (``import --label``, spec mgu-scale-unification.md
    §B) from mgu's attribution: one ``labels-<bucket>.parquet`` per bucket in
    the listings, rows ``(prefix, usr)`` with ``prefix`` relative to the
    bucket (``''`` = the bucket-wide rule). Returns bucket → row count."""
    from disk_tree.listing import prepare_listing

    src = prepare_listing(con, listings)
    fp_dir = "CASE WHEN name LIKE '%/%' THEN regexp_replace(name, '/[^/]*$', '') ELSE '' END"
    con.execute(f"CREATE OR REPLACE TEMP VIEW listing_dirs AS SELECT DISTINCT bucket, {fp_dir} AS name FROM {src}")
    pfx_df = prefix_labels(con, attributions, identities_path, "listing_dirs")
    buckets = [b for (b,) in con.execute("SELECT DISTINCT bucket FROM listing_dirs ORDER BY 1").fetchall()]
    out_dir.mkdir(parents=True, exist_ok=True)
    counts: dict[str, int] = {}
    for bucket in buckets:
        rows = pfx_df[(pfx_df["key"] == bucket) | pfx_df["key"].str.startswith(bucket + "/")]
        table = rows.assign(prefix=rows["key"].str.slice(len(bucket) + 1)).rename(columns={"user": "usr"})[["prefix", "usr"]]
        table = table.sort_values("prefix").reset_index(drop=True)
        con.register("labels_out", table)
        con.execute(f"COPY labels_out TO '{out_dir / f'labels-{bucket}.parquet'}' (FORMAT parquet)")
        con.unregister("labels_out")
        counts[bucket] = len(table)
    return counts


def _rss(tag: str) -> None:
    """Log peak RSS so OOM autopsies can name the phase (linux: KB, mac: B)."""
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    err(f"[rss] {tag}: peak {peak / (1024**2 if sys.platform == 'linux' else 1024**3):.1f} GB")

# Coarse index tiers: one per exponent E, keeping paths whose subtree clears
# F_E = 2^(round(log2 fleet_bytes) - E). At a 3 PiB fleet: E=16 → 48 GiB
# (~40k paths, a root view's row groups), E=20 → 3 GiB, E=24 → 256 MiB
# (~2M of 220M paths). The reader serves each query from the coarsest tier
# whose floor is under the query's pixel threshold. specs/view-serving.md §1.
COARSE_EXPS = (16, 20, 24)
# Aggregate bounds on the kept set — parent-relative alone is unbounded (the
# fleet has >100M dirs; 0.02%-of-parent keeps every child of an evenly-split
# parent, recursively — OOM-killed the 8/26 attempt-4 REPROC). Values chosen
# from the 8/26 full-listing estimate (see specs/dir-agg-cache.md).


def write_coarse_tiers(
    con: "duckdb.DuckDBPyConnection",
    path_index: Path,
    rows: str,
) -> tuple[dict[int, int], dict[int, int]]:
    """Write the coarse index tiers beside ``path_index`` (specs/view-serving.md §1).

    ``rows`` is a table/relation holding the floor-free index rows
    (``path, depth, usr, b, o, wts, wb, c2, c3, c4, a``) — ``ptu`` inside
    ``webdata``, or ``read_parquet(...)`` of an archived path-index for a
    backfill (``dt-cloud index-tiers``). A temp table ``tot`` (``path, pb`` =
    per-path subtree bytes) must exist; the caller builds it since ``webdata``
    reuses it for the fold.

    Each tier E keeps the SAME rows, restricted to paths whose subtree clears
    F_E = 2^(round(log2 fleet) - E), in the same two sort orders as the
    floor-free tier. A tier, not a loss: every kept row's sums are exact, and
    the reader only serves a query from a tier whose floor is <= the query's
    threshold (then the tier's rows are a superset of what the query keeps).
    F rides in the parquet key-value metadata so index-sync can record it
    beside the footer. Returns ({E: floor}, {E: kept paths})."""
    floors: dict[int, int] = {}
    counts: dict[int, int] = {}
    fleet = int(con.execute("SELECT coalesce(sum(pb), 0) FROM tot WHERE path NOT LIKE '%/%'").fetchone()[0])
    n_paths = con.execute("SELECT count(*) FROM tot").fetchone()[0]
    cols = "path, depth, usr, b, o, wts::DOUBLE AS wts, wb, c2, c3, c4, a"
    for e in COARSE_EXPS:
        floor = 2 ** (round(math.log2(fleet)) - e) if fleet > 0 else 1
        floors[e] = floor
        con.execute(f"CREATE TEMP TABLE coarse AS SELECT path FROM tot WHERE pb >= {floor}")
        counts[e] = con.execute("SELECT count(*) FROM coarse").fetchone()[0]
        kv = f"(FORMAT parquet, ROW_GROUP_SIZE 8192, KV_METADATA {{coarse_floor: '{floor}'}})"
        for suffix, order in (
            ("", "depth, path"),
            ("-by-user", "usr NULLS LAST, depth, path"),
        ):
            out = path_index.with_name(f"path-index-coarse{e}{suffix}.parquet")
            con.execute(f"COPY (SELECT {cols} FROM {rows} r JOIN coarse USING (path) ORDER BY {order}) TO '{out}' {kv}")
        con.execute("DROP TABLE coarse")
        err(f"coarse tier E={e}: floor {floor:,} B, {counts[e]:,} of {n_paths:,} paths")
    return floors, counts


def write_webdata(
    listings: tuple[str, ...],
    out_dir: Path,
    asof: str,
    attributions: tuple[str, ...] = (),
    identities_path: Path | None = None,
    access: tuple[str, ...] = (),
    dir_cache: Path | None = None,
    path_index: Path | None = None,
) -> dict:
    """Write age.json / meta.json under ``out_dir`` (+ the path index and its
    tiers at ``path_index``); returns meta.

    ``path_index`` writes the complete floor-free rolled-up path index
    (every ancestor path × attribution, sorted ``(depth, path)``) — the
    artifact the pixel-budget subtree API serves
    (specs/path-index-lazy-drill.md).

    ``dir_cache`` names a directory for the layer-2 rollups (``dir-stats`` /
    ``age-days`` parquet) — attribution-independent per-dir aggregates, cached
    write-through so re-attribution runs skip the 595M-row object scans
    entirely (specs/dir-agg-cache.md). Immutable per scan date, like the
    listing they derive from.

    With ``attributions``, every dir is attributed (deepest-prefix-wins, same
    join as ``report``) and every index row is an owner slice (``usr``).

    With ``access`` (layer-2a access-log agg parquet globs), every index row
    that has been read since logging began carries ``a`` — the epoch day of
    its most recent read (GET/HEAD/LIST anywhere under the prefix) — and meta
    gains the observation window (``meta.access = {from, to}``).
    """
    from disk_tree.listing import prepare_listing

    attr = bool(attributions)
    con = duckdb.connect()
    # attribution mode is node-scale (34M-dir python-side walk); plain mode
    # stays laptop-safe
    con.execute(f"SET memory_limit='{os.environ.get('DUCKDB_MEM', '24GB' if attr else '6GB')}'")
    # large hash aggregations stream instead of materializing ordered output;
    # matters in Cloud Run where DuckDB's disk spill is actually RAM-backed
    con.execute("SET preserve_insertion_order=false")
    con.execute(f"SET threads={os.environ.get('DUCKDB_THREADS', '4')}")
    if tmp := os.environ.get("DUCKDB_TMP"):
        con.execute(f"SET temp_directory='{tmp}'")
    src = prepare_listing(con, listings)

    # --- layer-2 dir rollups (attribution-independent; cached when dir_cache) ---
    # Everything downstream needs objects only via these two aggregates:
    # `dir_stats` (per-dir sizes/objects/classes/weighted-mtimes) and
    # `age_days` (per-(day, dir) bytes/objects). Cache them as parquet next to
    # the listing (immutable per date) so re-attribution runs — REPROC, ledger
    # refreshes — do zero 595M-row object scans; cold runs also drop from four
    # object scans to two (attr dirs + storage classes now derive from these).
    fp_dir = "CASE WHEN name LIKE '%/%' THEN regexp_replace(name, '/[^/]*$', '') ELSE '' END"
    fp = f"CASE WHEN ({fp_dir}) = '' THEN bucket ELSE bucket || '/' || ({fp_dir}) END"
    stats_pq = dir_cache / "dir-stats.parquet" if dir_cache else None
    age_pq = dir_cache / "age-days.parquet" if dir_cache else None
    if stats_pq is not None and stats_pq.exists():
        con.execute(f"CREATE TEMP VIEW dir_stats AS SELECT * FROM read_parquet('{stats_pq}')")
        err(f"dir-stats: cache hit ({stats_pq})")
    else:
        con.execute(
            f"""
            CREATE TEMP TABLE dir_stats AS
            WITH obj AS (
              SELECT bucket, {fp_dir} AS dir, size_bytes, created, storage_class_id, {fp} AS fp
              FROM {src}
            )
            SELECT bucket, dir, fp,
              sum(size_bytes)::BIGINT AS b, count(*)::BIGINT AS o,
              -- exact (DECIMAL, not DOUBLE): a float sum's last bits depend on the
              -- parallel aggregation order, which made re-runs differ in `wts`
              -- (verified 2026-09-06 on a 9/4 re-aggregation); seconds resolution
              -- is plenty for a byte-weighted mean written day.
              sum(CASE WHEN created IS NOT NULL THEN size_bytes::DECIMAL(38,0) * epoch(created)::BIGINT END)::DECIMAL(38,0) AS wts,
              sum(CASE WHEN created IS NOT NULL THEN size_bytes END)::BIGINT AS wb,
              sum(CASE WHEN storage_class_id = 2 THEN size_bytes END)::BIGINT AS c2,
              sum(CASE WHEN storage_class_id = 3 THEN size_bytes END)::BIGINT AS c3,
              sum(CASE WHEN storage_class_id = 4 THEN size_bytes END)::BIGINT AS c4
            FROM obj GROUP BY bucket, dir, fp
            """
        )
        if stats_pq is not None:
            stats_pq.parent.mkdir(parents=True, exist_ok=True)
            con.execute(f"COPY dir_stats TO '{stats_pq}' (FORMAT parquet)")
            err(f"dir-stats: wrote cache ({stats_pq})")
    _rss("dir-stats")
    if age_pq is not None and age_pq.exists():
        con.execute(f"CREATE TEMP VIEW age_days AS SELECT * FROM read_parquet('{age_pq}')")
        err(f"age-days: cache hit ({age_pq})")
    else:
        con.execute(
            f"""
            CREATE TEMP TABLE age_days AS
            SELECT CAST(floor(epoch(created) / 86400) AS INTEGER) AS day, bucket, {fp_dir} AS dir,
              sum(size_bytes)::BIGINT AS bytes, count(*)::BIGINT AS objects
            FROM {src}
            WHERE created IS NOT NULL
            GROUP BY ALL
            """
        )
        if age_pq is not None:
            con.execute(f"COPY age_days TO '{age_pq}' (FORMAT parquet)")
            err(f"age-days: wrote cache ({age_pq})")
    _rss("age-days")
    # Dir-level stand-in for the raw listing where only dir paths matter
    # (bucket enumeration + path-glob prefix_owners expansion).
    con.execute("CREATE TEMP VIEW listing_dirs AS SELECT bucket, dir AS name FROM dir_stats")

    if attr:
        _rss("start")
        pfx_df = prefix_labels(con, attributions, identities_path, "listing_dirs")
        _rss("prefix-map")
        con.register("pfx", pfx_df)
        # Deepest-prefix-wins, one INSERT per prefix depth (deepest first):
        # inner hash join (build side = that depth's prefixes — thousands) plus
        # an anti-join against already-resolved dirs (build side ≤ resolved
        # set, a few GB at fleet scale). Keeps the proven-fast split+equi-join
        # machinery of the original explosion but drops its un-spillable
        # dirs×maxd arg_max aggregate (OOM-killed the daily's 128GB node when
        # the 2026-08-26 wandb re-mine grew the prefix map). A chained-LEFT-
        # JOIN single-pass variant planned pathologically (~100× slower);
        # per-depth INSERTs give the planner 12 trivial queries instead.
        depths = sorted({int(d) for d in pfx_df["depth"]}, reverse=True)
        con.execute('CREATE TEMP TABLE dir_attr (bucket VARCHAR, dir VARCHAR, "user" VARCHAR)')
        dk = "CASE WHEN s.dir = '' THEN s.bucket ELSE s.bucket || '/' || s.dir END"
        for k in depths:
            con.execute(
                f"""
                INSERT INTO dir_attr
                SELECT s.bucket, s.dir, p."user"
                FROM dir_stats s
                JOIN pfx p ON p.depth = {k}
                  AND p.key = array_to_string(str_split({dk}, '/')[1:{k}], '/')
                WHERE len(str_split({dk}, '/')) >= {k}
                  AND NOT EXISTS (
                    SELECT 1 FROM dir_attr d WHERE d.bucket = s.bucket AND d.dir = s.dir
                  )
                """
            )
        _rss("dir_attr")
        # per-user storage-class byte mixes (the site prices per-user roll-ups
        # with class-aware rates) + the per-user leaderboard meta.users needs —
        # both derived from `dir_agg` below, so no separate object scan.
        user_class: dict[str, dict[int, int]] = defaultdict(lambda: defaultdict(int))
        user_bytes: dict[str, int] = defaultdict(int)

    # Access agg first (small): its per-dir last-read day joins into
    # `dir_agg` below so the path index carries a subtree-MAX `a`, and it
    # also decorates the age strata. Empty without logs.
    # As-of rule: a scan dated D sees reads through the end of D-1 UTC
    # (`day < D`), whatever shards exist when this runs — so a re-aggregation
    # of an old date reproduces it instead of leaking later reads into it.
    access_window: tuple[int, int] | None = None
    con.execute("CREATE TEMP TABLE access_agg (bucket VARCHAR, dir VARCHAR, aday INTEGER, ro BIGINT, rb BIGINT)")
    if access:
        globs = "[" + ", ".join(f"'{g}'" for g in access) + "]"
        con.execute(
            f"""
            INSERT INTO access_agg
            SELECT bucket, CASE WHEN path = '.' THEN '' ELSE path END AS dir,
              CAST(floor(epoch(MAX(last_ts)) / 86400) AS INTEGER) AS aday,
              COALESCE(SUM(n_ops) FILTER (WHERE op IN ('GET', 'HEAD')), 0) AS ro,
              COALESCE(SUM(bytes_out) FILTER (WHERE op IN ('GET', 'HEAD')), 0) AS rb
            FROM read_parquet({globs})
            WHERE op IN ('GET', 'HEAD', 'LIST') AND day < DATE '{asof}'
            GROUP BY 1, 2
            """
        )
        lo, hi = con.execute(
            f"SELECT CAST(floor(epoch(MIN(last_ts)) / 86400) AS INTEGER), "
            f"CAST(floor(epoch(MAX(last_ts)) / 86400) AS INTEGER) FROM read_parquet({globs}) "
            f"WHERE day < DATE '{asof}'"
        ).fetchone()
        if lo is not None:
            access_window = (int(lo), int(hi))
        _rss("access")

    # --- the path index: every ancestor path, every depth (specs/view-serving.md) ---
    # Roll every object up to *all* its ancestor prefixes (descendant-inclusive
    # totals at every depth), attribute per dir, keep only prefixes clearing the
    # fold floor so the Python side stays small regardless of object count, and
    # link them parent->child. No d1..d4 cap — the tree is as deep as the data.

    attr_join_s = "LEFT JOIN dir_attr t ON t.bucket = s.bucket AND t.dir = s.dir" if attr else ""
    user_sel = 't."user"' if attr else "CAST(NULL AS VARCHAR)"
    # Attribution is a join over the cached per-dir rollups — a few million
    # rows — never over objects. dir_agg also feeds the class-mix and
    # leaderboard, so those need no separate scan either.
    con.execute(
        f"""
        CREATE TEMP TABLE dir_agg AS
        SELECT s.fp, {user_sel} AS usr,
          s.b, s.o, s.wts, s.wb, s.c2, s.c3, s.c4, xa.aday AS a
        FROM dir_stats s {attr_join_s}
        LEFT JOIN access_agg xa ON xa.bucket = s.bucket AND xa.dir = s.dir
        """
    )
    total_b, total_o = con.execute("SELECT coalesce(sum(b), 0)::BIGINT, coalesce(sum(o), 0)::BIGINT FROM dir_agg").fetchone()
    total_b, total_o = int(total_b), int(total_o)
    maxseg = int(con.execute("SELECT coalesce(max(len(string_split(fp, '/'))), 1) FROM dir_agg").fetchone()[0])
    _rss("dir-agg")

    if attr:
        # class-mix + leaderboard straight off dir_agg (class 1/STANDARD =
        # total minus the non-standard classes we track explicitly).
        for usr, b, c2, c3, c4 in con.execute(
            "SELECT usr, sum(b), sum(c2), sum(c3), sum(c4) FROM dir_agg WHERE usr IS NOT NULL GROUP BY usr"
        ).fetchall():
            c1 = int(b) - int(c2 or 0) - int(c3 or 0) - int(c4 or 0)
            for cid, cv in ((1, c1), (2, c2), (3, c3), (4, c4)):
                if cv:
                    user_class[usr][cid] += int(cv)
            user_bytes[usr] += int(b)

    # The full rolled-up (path, user) relation — every ancestor path,
    # descendant-inclusive, attributed, NO floor. Materialized because three
    # consumers share it: the coarse tiers, the (optional)
    # path-index artifact, and — via that artifact — the pixel-budget subtree
    # API (specs/path-index-lazy-drill.md).
    con.execute(
        f"""
        CREATE TEMP TABLE ptu AS
        WITH da AS (
          -- split computed here (streamed), not materialized into dir_agg —
          -- a LIST column on 150M rows is a ~40GB temp table by itself
          SELECT *, string_split(fp, '/') AS segs FROM dir_agg
        ),
        exploded AS (
          SELECT array_to_string(segs[1:r.k], '/') AS path, r.k AS depth,
            b, o, wts, wb, c2, c3, c4, a, usr
          FROM da, range(1, {maxseg} + 1) r(k)
          WHERE len(segs) >= r.k
        )
        SELECT path, depth, usr,
          sum(b)::BIGINT AS b, sum(o)::BIGINT AS o,
          sum(wts)::DECIMAL(38,0) AS wts, sum(wb)::BIGINT AS wb,
          sum(c2)::BIGINT AS c2, sum(c3)::BIGINT AS c3, sum(c4)::BIGINT AS c4,
          max(a) AS a  -- subtree-max last-read epoch day (NULL = never read)
        FROM exploded GROUP BY path, depth, usr
        """
    )
    _rss("ptu")
    if path_index is not None:
        # The complete, floor-free index the subtree API serves: one row per
        # (path, usr), sorted (depth, path) — the engine's canonical
        # order for prefix-range + row-group pruning. Immutable per date.
        path_index.parent.mkdir(parents=True, exist_ok=True)
        cols = "path, depth, usr, b, o, wts::DOUBLE AS wts, wb, c2, c3, c4, a"
        # 8k rows/group (~1 MB): a deep drill decodes ~8k rows/group, not 64k,
        # and the footer (now in D1 per index-sync) is never parsed on a cold
        # isolate, so the ~27k-group count costs nothing at read time
        # (specs/path-agnostic-serving.md §2.1).
        rg = "(FORMAT parquet, ROW_GROUP_SIZE 8192)"
        con.execute(f"COPY (SELECT {cols} FROM ptu ORDER BY depth, path) TO '{path_index}' {rg}")
        err(f"path-index: wrote {path_index}")
        _rss("path-index")
        # `by-user` copy: the SAME rows re-sorted so a user lens's row groups
        # prune by `usr` (the `by-path` copy's stats are on `b`, useless for a
        # lens). `b` in a usr=X row is X's bytes under `path`, so the lens read
        # pixel-budgets on it directly. NULL usr sorts last (the unclaimed
        # pool). specs/path-agnostic-serving.md §2.3.
        by_user = path_index.with_name("path-index-by-user.parquet")
        con.execute(f"COPY (SELECT {cols} FROM ptu ORDER BY usr NULLS LAST, depth, path) TO '{by_user}' {rg}")
        err(f"path-index: wrote {by_user}")
        _rss("path-index-variants")
        # Provenance sidecar: the attributing prefixes' user/source/evidence.
        from .extras import write_extras
        write_extras(pfx_df if attr else None, path_index.parent)
        _rss("extras")

    # Per-path subtree totals: the coarse tiers' floor test. Staged (its own
    # statement) so the agg runs alone, not stacked under another operator.
    con.execute("CREATE TEMP TABLE tot AS SELECT path, sum(b) AS pb FROM ptu GROUP BY path")
    _rss("tot")
    coarse_floors: dict[int, int] = {}
    coarse_counts: dict[int, int] = {}
    if path_index is not None:
        coarse_floors, coarse_counts = write_coarse_tiers(con, path_index, rows="ptu")
        _rss("coarse")
    con.execute("DROP TABLE tot")
    con.execute("DROP TABLE ptu")

    # Age strata also carry `a` — the dir's last-read epoch day from the access
    # agg (subtree MAX, the same semantics as the tree's `a`) — so the site can
    # color a vintage by whether anyone has touched it since logging began.
    # Absent = no read observed. Multiplies rows by at most the number of
    # distinct read days (a few weeks of logs), not by dirs.
    if attr:
        # (day, d1, user, a) strata: the cached per-(day, dir) rollup
        # joined to the same dir attribution. Day keys are epoch days; the site
        # aggregates to day/week/month.
        age = con.execute(
            """
            SELECT d.day,
              CASE WHEN d.dir = '' THEN '(files)' ELSE regexp_extract(d.dir, '^([^/]+)', 1) END AS d1,
              t."user" AS user, x.aday AS a,
              sum(d.bytes)::BIGINT AS bytes, sum(d.objects)::BIGINT AS objects
            FROM age_days d
            LEFT JOIN dir_attr t ON t.bucket = d.bucket AND t.dir = d.dir
            LEFT JOIN access_agg x ON x.bucket = d.bucket AND x.dir = d.dir
            GROUP BY ALL ORDER BY ALL  -- fully deterministic output order (byte-identical reruns)
            """
        ).fetchall()
        age_rows = [
            {"d": day, "d1": d1, **({"u": u} if u else {}), **({"a": a} if a is not None else {}), "b": b, "o": o}
            for day, d1, u, a, b, o in age
        ]
        _rss("age")
    else:
        age = con.execute(
            """
            SELECT d.day,
              CASE WHEN d.dir = '' THEN NULL ELSE regexp_extract(d.dir, '^([^/]+)', 1) END AS d1,
              x.aday AS a,
              sum(d.bytes)::BIGINT AS bytes, sum(d.objects)::BIGINT AS objects
            FROM age_days d LEFT JOIN access_agg x ON x.bucket = d.bucket AND x.dir = d.dir
            GROUP BY ALL ORDER BY ALL
            """
        ).fetchall()
        age_rows = [
            {"d": day, "d1": d1 or "(files)", **({"a": a} if a is not None else {}), "b": b, "o": o}
            for day, d1, a, b, o in age
        ]

    # Storage-class mix from the dir rollups (class 1/STANDARD = total minus
    # the explicitly-tracked classes — same derivation the per-user mix uses).
    s_b, s_c2, s_c3, s_c4 = con.execute(
        "SELECT coalesce(sum(b), 0)::BIGINT, coalesce(sum(c2), 0)::BIGINT,"
        " coalesce(sum(c3), 0)::BIGINT, coalesce(sum(c4), 0)::BIGINT FROM dir_stats"
    ).fetchone()
    classes = [(cid, cb) for cid, cb in ((1, int(s_b) - int(s_c2) - int(s_c3) - int(s_c4)), (2, int(s_c2)), (3, int(s_c3)), (4, int(s_c4))) if cb]

    meta = {
        "asof": asof,
        "generated": dt.date.today().isoformat(),
        "total_bytes": total_b,
        "total_objects": total_o,
        "class_bytes": {int(c): int(b) for c, b in classes},
    }
    if coarse_floors:
        meta["index"] = {"coarse": {str(e): {"floor": coarse_floors[e], "paths": int(coarse_counts[e])} for e in COARSE_EXPS}}
    if access_window:
        # Epoch days the access logs cover — the UI's "no reads since <from>"
        # is only meaningful relative to when logging began.
        meta["access"] = {"from": access_window[0], "to": access_window[1]}
    if attr:
        meta["users"] = [{"u": u, "b": b} for u, b in sorted(user_bytes.items(), key=lambda kv: -kv[1])]
        meta["user_class_bytes"] = {u: dict(sorted(c.items())) for u, c in sorted(user_class.items())}

    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "age.json").write_text(json.dumps(age_rows, separators=(",", ":")) + "\n")
    (out_dir / "meta.json").write_text(json.dumps(meta, indent=2) + "\n")
    return meta
