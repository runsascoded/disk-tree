"""Out-of-core aggregation via DuckDB (spec Item B).

Same bottom-up group-by cascade as :func:`disk_tree.find.index.aggregate`
(the pandas version), but expressed in SQL so DuckDB can spill to disk
under a memory cap. Callers pick the engine (pandas for small local scans,
DuckDB for large / imported / bulk-listing scans); output shape is
identical so `/api/compare`, treemap, etc. don't know or care.

Two entry points:

- :func:`aggregate_duckdb` — same signature as ``aggregate()``: takes the
  full input DataFrame (files + walk-emitted / synthesized dir rows),
  returns the canonical layer-2 DataFrame. Used for parity testing and for
  wiring into `import_listing` when duckdb is the requested engine.
- :func:`aggregate_listing_to_parquet` — the true out-of-core path: reads
  a layer-1 listing parquet glob straight into DuckDB, aggregates without
  materializing anything in pandas, and writes the layer-2 parquet
  directly. This is what the 588M-row multi-PB regime needs.
"""

from __future__ import annotations

import os
import sys
from datetime import datetime
from typing import TYPE_CHECKING, Callable

import pandas as pd

if TYPE_CHECKING:
    import duckdb


# One place to keep the SQL fragment that computes a path's parent — DuckDB's
# regexp is fine at scale and matches Python `os.path.dirname` semantics for
# our path shapes (no leading slash, no trailing slash, no repeated slashes).
_PARENT_EXPR = (
    "CASE WHEN position('/' IN {col}) > 0 "
    "THEN regexp_extract({col}, '^(.*)/[^/]+$', 1) "
    "ELSE '' END"
)

# Segment count of a canonical path: '' → 0, 'a' → 1, 'a/b' → 2 — the same
# arithmetic the output's `depth` column uses.
_NSEG_EXPR = (
    "CASE WHEN {col} = '' THEN 0 "
    "ELSE length({col}) - length(replace({col}, '/', '')) + 1 END"
)


def _part_expr(col: str, k: int, min_nseg: int | None = None) -> str:
    """Depth-`k` prefix of a canonical path; NULL for paths with fewer than
    `min_nseg` (default `k`) segments. The partitioned cascade passes
    `min_nseg = k + 1` so a key is always a *directory* — a path at exactly
    depth `k` is its own prefix, and keying on it made every depth-`k` file a
    one-file partition (10,848 of them on marin-us-west4 at k = 2, spec
    `mgu-scale-a3-gate.md` ask 1); those rows belong to the top cascade."""
    nseg = _NSEG_EXPR.format(col=col)
    return (
        f"CASE WHEN {nseg} >= {k if min_nseg is None else min_nseg} "
        f"THEN array_to_string(list_slice(string_split({col}, '/'), 1, {k}), '/') END"
    )


#: Default batch target for the partitioned cascade: partitions are packed,
#: in key order, into cascades of up to this many files (a bigger key stands
#: alone). Measured 2026-09-07 (spec `mgu-scale-a3-gate.md`): 4.8M files with
#: labels + pivots + mean mtime + size histogram peaked at 21 GB RSS.
DEFAULT_PARTITION_FILES = 4_000_000


def _batch_partitions(parts: list[tuple[str, int]], partition_files: int) -> list[list[str]]:
    """Pack sorted `(key, n_files)` partitions into consecutive batches of at
    most `partition_files` files each — one key per batch when `partition_files`
    ≤ 0, and a key larger than the budget is a batch by itself. Consecutive
    keys keep each batch's `name` range contiguous for the parquet pushdown."""
    batches: list[list[str]] = []
    cur: list[str] = []
    cur_n = 0
    for key, n in parts:
        if cur and (partition_files <= 0 or cur_n + n > partition_files):
            batches.append(cur)
            cur, cur_n = [], 0
        cur.append(key)
        cur_n += n
    if cur:
        batches.append(cur)
    return batches


def _stage(msg: str) -> None:
    """Stage-boundary log line (stderr): OOM post-mortems need to know which
    statement was in flight — a SIGKILL leaves no traceback."""
    print(f"[agg {datetime.now().isoformat(timespec='seconds')}] {msg}", file=sys.stderr, flush=True)


def _max_rss_mb() -> float:
    """Peak RSS of this process (MiB) — the number the fleet-scale gate reports."""
    import resource
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    # Linux reports KiB, macOS bytes.
    return rss / 1024 if sys.platform == 'linux' else rss / (1024 * 1024)


def _open_db(db: str | None) -> "tuple[duckdb.DuckDBPyConnection | None, str | None]":
    """Open the cascade's database when `db` asks for a file-backed one.

    `db` is a DuckDB database file (created if missing; left in place, so a
    post-mortem can inspect what the cascade left behind) or an existing
    directory, in which case a fresh file is created inside it and removed
    once the aggregation succeeds. `None` → in-memory (the caller's
    connection is used as-is).
    """
    if db is None:
        return None, None
    import duckdb as _duckdb
    if os.path.isdir(db):
        from uuid import uuid4
        path = os.path.join(db, f'disk-tree-agg-{uuid4().hex}.duckdb')
        return _duckdb.connect(path), path
    return _duckdb.connect(db), None


def _register_inputs(con: "duckdb.DuckDBPyConnection", inputs: pd.DataFrame) -> None:
    """Materialize the input frame as a DuckDB table (so subsequent SQL can spill)."""
    con.execute("DROP TABLE IF EXISTS inputs")
    # DuckDB reads a registered pandas view; CREATE TABLE ... AS SELECT materializes.
    con.register('inputs_df', inputs)
    con.execute("""
        CREATE TABLE inputs AS
        SELECT
            path::VARCHAR AS path,
            size::BIGINT AS size,
            mtime::BIGINT AS mtime,
            kind::VARCHAR AS kind,
            COALESCE(parent, '')::VARCHAR AS parent,
            uri::VARCHAR AS uri,
            -- walk-emitted rows: every file is one object, dirs are none
            CASE WHEN kind = 'file' THEN 1 ELSE 0 END::BIGINT AS obj
        FROM inputs_df
    """)
    con.unregister('inputs_df')


def _build_dirs_cascade(
    con: "duckdb.DuckDBPyConnection",
    sum_cols: tuple[str, ...] = (),
    mean_mtime: bool = False,
    src: str = 'inputs',
    out: str = 'dirs_all',
    n_children: str = 'n_children_tbl',
    tag: str = '',
    group_cols: tuple[str, ...] = (),
    max_cols: tuple[str, ...] = (),
) -> None:
    """Bottom-up group-by cascade over `src` → `out` + `n_children` tables.

    `max_cols` are columns on `src` folded with MAX at every level, like
    `mtime` (spec mgu-scale-unification.md D.4: a subtree-max last-read from
    a side table); NULL where nothing beneath carries a value.

    `sum_cols` are extra monoid columns on `src` (per-file contributions;
    0 on dir rows) summed through the cascade unchanged. With `mean_mtime`,
    `src` must carry `mt_wsum` (HUGEINT Σ mtime·size partials — exact, no
    float-summation order sensitivity); consumers divide it into the
    `mtime_mean` output column (see find/agg_ext.py).

    `group_cols` are label columns on `src` (spec mgu-scale-unification.md
    item B) carried as additional group keys: every level groups by
    `(parent, *group_cols)`, so a path comes out once per distinct label
    tuple beneath it, and `n_children` counts per (path, labels-of-child).
    NULL is an ordinary label value (one group).

    `out` holds one row per (level, path[, labels]): a key's totals are the
    SUM over its rows, so a partitioned build can append several cascades'
    outputs (stub ancestors included) into one table and fold them in a
    single GROUP BY. `tag` prefixes the stage log lines.
    """
    from .agg_ext import MT_WSUM
    cascade_cols = [*sum_cols, *([MT_WSUM] if mean_mtime else [])]
    extra_sel = ''.join(f', {c}' for c in [*cascade_cols, *max_cols])
    # SUM(BIGINT) widens to HUGEINT in DuckDB — cast the pivot sums back down
    # (they're bounded by total size); mt_wsum genuinely needs the width.
    extra_sum = ''.join(
        f", SUM({c})::{'HUGEINT' if c == MT_WSUM else 'BIGINT'} AS {c}"
        for c in cascade_cols
    ) + ''.join(f", MAX({c}) AS {c}" for c in max_cols)
    keys = ''.join(f', {c}' for c in group_cols)

    parent_of_path = _PARENT_EXPR.format(col='path')

    # dirs0 = walk-emitted / synthesized dir rows; each contributes n_desc=1 at
    # its own path, and n_files = `obj` — 0 for a synthesized dir, 1 for a
    # folder-placeholder object (`…/` in an object store: an object, listed and
    # billed, that names a directory — spec `mgu-scale-a3-gate.md` ask 2).
    con.execute(f"""
        CREATE OR REPLACE TABLE dirs0 AS
        SELECT path{keys}, size, mtime, 1::BIGINT AS n_desc, obj::BIGINT AS n_files{extra_sel}
        FROM {src}
        WHERE kind = 'dir'
    """)

    # n_children per parent: count of input rows (files + dirs) whose parent is this path.
    # Faithful to pandas `grouped.size()` at level 0 (only). Rows with path='' don't count
    # (that's the scan root; it has no parent).
    con.execute(f"""
        CREATE OR REPLACE TABLE {n_children} AS
        SELECT parent AS path{keys}, COUNT(*)::BIGINT AS n_children
        FROM {src}
        WHERE path != ''
        GROUP BY parent{keys}
    """)

    # `cur` seed: all input rows with n_desc=1; n_files = `obj` (1 for every
    # listed object — files and folder placeholders — 0 for synthesized dirs).
    # Loop group-by-parent to synthesize each subsequent level's dir rows
    # (bottom-up). Stop when nothing left to promote.
    con.execute(f"""
        CREATE OR REPLACE TABLE level_cur AS
        SELECT path{keys}, size, mtime,
               1::BIGINT AS n_desc,
               obj::BIGINT AS n_files{extra_sel}
        FROM {src}
    """)
    level_tables: list[str] = []
    level = 0
    while True:
        next_tbl = f'level_{level}'
        con.execute(f"""
            CREATE OR REPLACE TABLE {next_tbl} AS
            SELECT {parent_of_path} AS path{keys},
                   SUM(size)::BIGINT AS size,
                   MAX(mtime)::BIGINT AS mtime,
                   SUM(n_desc)::BIGINT AS n_desc,
                   SUM(n_files)::BIGINT AS n_files{extra_sum}
            FROM level_cur
            WHERE path != ''
            GROUP BY 1{keys}
        """)
        cnt = con.execute(f"SELECT COUNT(*) FROM {next_tbl}").fetchone()[0]
        _stage(f"{tag}cascade level {level}: {cnt} rows")
        if cnt == 0:
            break
        level_tables.append(next_tbl)
        # promote for next iteration
        con.execute(f"CREATE OR REPLACE TABLE level_cur AS SELECT * FROM {next_tbl}")
        level += 1

    # Union all dir levels + dirs0, group by path, attach n_children.
    base_cols = f"path{keys}, size, mtime, n_desc, n_files{extra_sel}"
    if level_tables:
        levels_union = " UNION ALL ".join(
            f"SELECT {base_cols} FROM {t}" for t in level_tables
        )
        con.execute(f"""
            CREATE OR REPLACE TABLE {out} AS
            SELECT {base_cols} FROM dirs0
            UNION ALL
            {levels_union}
        """)
    else:
        con.execute(f"""
            CREATE OR REPLACE TABLE {out} AS
            SELECT {base_cols} FROM dirs0
        """)

    # The level tables are folded into `out`; keeping them alive doubles the
    # cascade's disk footprint (spill exhaustion at the 92.7M-row scale).
    # `next_tbl` is the empty level that ended the loop.
    for t in ['dirs0', 'level_cur', *level_tables, next_tbl]:
        con.execute(f"DROP TABLE IF EXISTS {t}")


def _aggregate_shared(
    con: "duckdb.DuckDBPyConnection",
    scan_root: str,
    sum_cols: tuple[str, ...] = (),
    mean_mtime: bool = False,
) -> pd.DataFrame:
    """The core SQL cascade + pandas tail. Assumes an `inputs` table with the
    schema above (see :func:`_build_dirs_cascade` for `sum_cols` / `mean_mtime`)."""
    from .agg_ext import MT_WSUM, MTIME_MEAN
    _build_dirs_cascade(con, sum_cols=sum_cols, mean_mtime=mean_mtime)

    # Pivot sums come straight through; mt_wsum divides into mtime_mean here,
    # inside SQL — HUGEINT would lose exactness crossing into pandas.
    extra_out = ''.join(f', SUM(d.{c})::BIGINT AS {c}' for c in sum_cols)
    if mean_mtime:
        extra_out += (
            f", CASE WHEN SUM(d.size) > 0"
            # HUGEINT::DOUBLE is not correctly rounded (1-ULP drift past 2^64 vs
            # Python's int->float in the other engines' `mean_of`); the VARCHAR
            # parse is, keeping `mtime_mean` byte-identical across engines.
            f" THEN SUM(d.{MT_WSUM})::VARCHAR::DOUBLE / SUM(d.size)::DOUBLE"
            f" END AS {MTIME_MEAN}"
        )

    parent_of_agg = _PARENT_EXPR.format(col='d.path')
    dirs_agg = con.execute(f"""
        SELECT
            d.path AS path,
            SUM(d.size)::BIGINT AS size,
            MAX(d.mtime)::BIGINT AS mtime,
            SUM(d.n_desc)::BIGINT AS n_desc,
            SUM(d.n_files)::BIGINT AS n_files,
            COALESCE(MAX(nc.n_children), 0)::BIGINT AS n_children,
            'dir' AS kind,
            {parent_of_agg} AS parent{extra_out}
        FROM dirs_all d
        LEFT JOIN n_children_tbl nc ON d.path = nc.path
        GROUP BY d.path
        ORDER BY d.path
    """).df()

    if dirs_agg.empty:
        # Single-file corner case: no dir rows anywhere. Manufacture the root
        # stub in the same shape as pandas `aggregate()` does at find/index.py.
        dirs_agg = pd.DataFrame([{
            'path': '.', 'size': 0, 'mtime': 0, 'n_desc': 0, 'n_files': 0, 'n_children': 0,
            'kind': 'dir', 'parent': '', 'uri': scan_root,
            **{c: 0 for c in sum_cols},
            **({MTIME_MEAN: None} if mean_mtime else {}),
        }])
    else:
        # Pandas final normalization (find/index.py):
        # - dir rows with parent='' get parent='.' (except the root itself)
        # - the root dir has path='.', parent=''
        # - uri = scan_root for root, else f'{scan_root}/{path}'
        root_mask = dirs_agg['path'] == ''
        dirs_agg.loc[dirs_agg['parent'] == '', 'parent'] = '.'
        dirs_agg.loc[root_mask, ['path', 'parent']] = ['.', '']
        dirs_agg['uri'] = dirs_agg['path'].apply(lambda p: scan_root if p == '.' else f'{scan_root}/{p}')

    # Files: n_desc=1 (self), n_files=1 (self is a file), n_children=0.
    # Extension columns: pivot sums are the file's own contribution (already
    # on `inputs`); mtime_mean is the file's own mtime.
    file_extra = ''.join(f', {c}' for c in sum_cols)
    if mean_mtime:
        file_extra += f', mtime::DOUBLE AS {MTIME_MEAN}'
    files = con.execute(f"""
        SELECT path, size, mtime, 'file'::VARCHAR AS kind, parent, uri,
               1::BIGINT AS n_desc,
               1::BIGINT AS n_files,
               0::BIGINT AS n_children{file_extra}
        FROM inputs
        WHERE kind = 'file'
    """).df()

    out = pd.concat([dirs_agg, files], ignore_index=True)
    out['depth'] = out['path'].apply(lambda p: 0 if p == '.' else p.count('/') + 1)
    return out.sort_values(['depth', 'path']).reset_index(drop=True)


def aggregate_duckdb(
    inputs: pd.DataFrame,
    scan_root: str,
    con: "duckdb.DuckDBPyConnection | None" = None,
    memory_limit: str | None = None,
    temp_dir: str | None = None,
) -> pd.DataFrame:
    """Same-signature parity for :func:`disk_tree.find.index.aggregate` — SQL-backed.

    Mostly useful for parity testing and as the routing target when a caller
    already has an in-memory input frame. For the multi-PB regime, prefer
    :func:`aggregate_listing_to_parquet` which never materializes a full
    layer-1 frame in pandas.
    """
    import duckdb as _duckdb
    if con is None:
        con = _duckdb.connect()
    if memory_limit:
        con.execute(f"SET memory_limit = '{memory_limit}'")
    if temp_dir:
        con.execute(f"SET temp_directory = '{temp_dir}'")
    _register_inputs(con, inputs)
    return _aggregate_shared(con, scan_root)


def _sql_lit(v) -> str:
    """Literal for a pivot value discovered in the data (int or string enum)."""
    if isinstance(v, bool) or not isinstance(v, (int, str)):
        raise ValueError(f"unsupported pivot value type {type(v).__name__}: {v!r}")
    if isinstance(v, int):
        return str(v)
    escaped = v.replace("'", "''")
    return f"'{escaped}'"


def _files_select(
    listing_sql: str,
    bucket: str,
    extra_file_cols: str,
    pivot_pass: str,
    where: str = '',
) -> str:
    """Canonical file rows (`path, size, mtime, kind, parent, <extras>`) straight
    off the listing — the one place the layer-1 → input-row mapping lives.

    `where` is an extra predicate over the raw listing columns (`name`,
    `canonical`), used by the partitioned build to hand DuckDB a range on
    `name` it can push into the parquet row-group statistics.
    """
    # Collapse consecutive slashes + strip trailing slashes so `a//b` reads as
    # `a/b`. Keys with empty path components exist in real listings (marin's
    # 2026-08-14 west4 scan has `tokenized/…//.artifact.json`); leaving the
    # trailing slash on the intermediate dir breaks the parent-of regex below
    # (regexp_extract fails to match a trailing-`/` string → returns '' → the
    # dir gets hoisted to the tree root, moving bytes across subtrees).
    #
    # A name that *ends* in `/` is a folder placeholder (TensorBoard's
    # `plugins/profile/<ts>/`, console-made "folders"): an object — listed,
    # billed per op, deleted by a sweep — that names a directory. It is the
    # directory's own row (`kind = 'dir'`, at the stripped path, carrying the
    # object's size/mtime) and counts as one object there (`obj = 1` →
    # `n_files`), never as a file child. Spec `mgu-scale-a3-gate.md` ask 2.
    squashed = "regexp_replace(name, '/+', '/', 'g')"
    canonical_name = f"rtrim({squashed}, '/')"
    parent_of_name = _PARENT_EXPR.format(col='canonical')
    return f"""
        WITH canon AS (
            SELECT
                name,
                {canonical_name} AS canonical,
                CASE WHEN ends_with({squashed}, '/') THEN 'dir' ELSE 'file' END::VARCHAR AS kind,
                size_bytes,
                created{pivot_pass}
            FROM {listing_sql}
            WHERE bucket = '{bucket}'{where}
        )
        SELECT
            canonical AS path,
            size_bytes::BIGINT AS size,
            -- floor, not ::BIGINT (which rounds): epoch-seconds truncate by
            -- convention, and the stream engine's pyarrow int-division floors —
            -- rounding here skewed ~50% of real (sub-second) timestamps +1s
            COALESCE(floor(epoch(created)), 0)::BIGINT AS mtime,
            kind,
            {parent_of_name} AS parent,
            1::BIGINT AS obj{extra_file_cols}
        FROM canon
    """


def _dir_rows_insert(
    table: str,
    dir_paths: str,
    extra_dir_cols: str,
    labels: "_Labels | None" = None,
    side: "_Side | None" = None,
    exclude: str | None = None,
) -> str:
    """INSERT synthesized dir rows (size/mtime 0, `obj` 0, extras 0) for every
    path in `dir_paths` not already a dir row — a folder placeholder object is
    that directory's row already; a second one would double its `n_desc` and
    its parent's `n_children`. `exclude` is a parenthesized SELECT of the
    paths to skip; default: `table`'s own dir rows. The partitioned build
    passes the listing-wide placeholder set, since a placeholder and the
    partition that synthesizes its ancestor row can sit in different cascades."""
    rows = f"""
        SELECT
            path,
            0::BIGINT AS size,
            0::BIGINT AS mtime,
            'dir'::VARCHAR AS kind,
            {_PARENT_EXPR.format(col='path')} AS parent,
            0::BIGINT AS obj{extra_dir_cols}
        FROM {dir_paths}
        WHERE path NOT IN {exclude or f"(SELECT path FROM {table} WHERE kind = 'dir')"}
    """
    if labels is not None:
        rows = labels.join(rows)
    if side is not None:
        rows = side.join(rows)
    return f"INSERT INTO {table} {rows}"


class _Labels:
    """The attribution label table (spec mgu-scale-unification.md item B): a
    parquet of `prefix → <label columns>`, joined onto every input row by
    deepest matching prefix.

    The join is one LEFT JOIN per distinct prefix depth present in the table
    (a handful, against ~10⁴ prefixes — each a streaming probe with a tiny
    build side), matching the row's depth-d prefix by equality; `COALESCE`
    from the deepest depth down implements deepest-prefix-wins. A `''` prefix
    (depth 0) is the catch-all default. Rows under no prefix get NULLs.
    """

    def __init__(self, con: "duckdb.DuckDBPyConnection", path: str, cols: tuple[str, ...]):
        self.cols = cols
        present = [r[0] for r in con.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{path}') LIMIT 0"
        ).fetchall()]
        if 'prefix' not in present:
            raise ValueError(f"label table {path}: no `prefix` column (has {present})")
        if not cols:
            cols = self.cols = tuple(c for c in present if c != 'prefix')
        missing = [c for c in cols if c not in present]
        if missing:
            raise ValueError(f"label table {path}: missing label column(s) {missing} (has {present})")
        reserved = set(_LAYER2_COLS) | {'kind', 'parent', 'uri', 'depth'}
        clash = [c for c in cols if c in reserved]
        if clash:
            raise ValueError(f"label column(s) {clash} collide with layer-2 columns")
        if not cols:
            raise ValueError(f"label table {path}: no label columns besides `prefix`")
        nseg = _NSEG_EXPR.format(col='prefix')
        con.execute(f"""
            CREATE OR REPLACE TABLE labels AS
            SELECT prefix, {nseg} AS depth{''.join(f', {c}' for c in cols)}
            FROM (
                SELECT rtrim(regexp_replace(prefix, '/+', '/', 'g'), '/') AS prefix{''.join(f', {c}' for c in cols)}
                FROM read_parquet('{path}')
            )
        """)
        dupes = con.execute(
            "SELECT prefix FROM labels GROUP BY prefix HAVING COUNT(*) > 1 ORDER BY 1 LIMIT 5"
        ).fetchall()
        if dupes:
            raise ValueError(f"label table {path}: duplicate prefix(es) {[d[0] for d in dupes]}")
        self.depths = [r[0] for r in con.execute(
            "SELECT DISTINCT depth FROM labels ORDER BY depth DESC"
        ).fetchall()]
        self.n = con.execute("SELECT COUNT(*) FROM labels").fetchone()[0]

    def join(self, rows_sql: str) -> str:
        """Wrap a SELECT producing `path, …` so each row also carries its labels."""
        picks = ''.join(
            f", COALESCE({', '.join(f'l{d}.{c}' for d in self.depths)}) AS {c}"
            for c in self.cols
        )
        joins = ''
        for d in self.depths:
            on = f"l{d}.depth = {d}"
            if d > 0:
                on += f" AND l{d}.prefix = {_part_expr('s.path', d)}"
            joins += f" LEFT JOIN labels l{d} ON {on}"
        return f"SELECT s.*{picks} FROM ({rows_sql}) s{joins}"


_LAYER2_COLS = ('path', 'size', 'mtime', 'n_desc', 'n_files', 'n_children')


class _Side:
    """A side table keyed by `path` (spec mgu-scale-unification.md D.4): the
    access plane's per-scan state, joined onto every input row by exact path
    so `max_cols` (e.g. `last_ts`) can fold through the cascade as a
    subtree MAX. The side's root is `.` (the 2a convention); inputs use `''`.
    A `bucket` column, if present, restricts the side to the imported bucket.
    """

    def __init__(
        self,
        con: "duckdb.DuckDBPyConnection",
        path: str,
        cols: tuple[str, ...],
        bucket: str,
    ):
        if not cols:
            raise ValueError("--side needs at least one --max-col")
        present = [r[0] for r in con.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{path}') LIMIT 0"
        ).fetchall()]
        if 'path' not in present:
            raise ValueError(f"side table {path}: no `path` column (has {present})")
        missing = [c for c in cols if c not in present]
        if missing:
            raise ValueError(f"side table {path}: missing column(s) {missing} (has {present})")
        reserved = set(_LAYER2_COLS) | {'kind', 'parent', 'uri', 'depth'}
        clash = [c for c in cols if c in reserved]
        if clash:
            raise ValueError(f"side column(s) {clash} collide with layer-2 columns")
        self.cols = cols
        where = f" WHERE bucket = {_sql_lit(bucket)}" if 'bucket' in present else ''
        con.execute(f"""
            CREATE OR REPLACE TABLE side AS
            SELECT CASE WHEN path = '.' THEN '' ELSE path END AS path{''.join(f', {c}' for c in cols)}
            FROM read_parquet('{path}'){where}
        """)
        dupes = con.execute(
            "SELECT path FROM side GROUP BY path HAVING COUNT(*) > 1 ORDER BY 1 LIMIT 5"
        ).fetchall()
        if dupes:
            raise ValueError(f"side table {path}: duplicate path(s) {[d[0] for d in dupes]}")
        self.n = con.execute("SELECT COUNT(*) FROM side").fetchone()[0]

    def join(self, rows_sql: str) -> str:
        picks = ''.join(f', side.{c}' for c in self.cols)
        return f"SELECT s.*{picks} FROM ({rows_sql}) s LEFT JOIN side ON side.path = s.path"


def _build_partitioned(
    con: "duckdb.DuckDBPyConnection",
    files_sql_for: "Callable[[str], str]",
    k: int,
    sum_cols: tuple[str, ...],
    mean_mtime: bool,
    extra_dir_cols: str,
    labels: "_Labels | None" = None,
    side: "_Side | None" = None,
    partition_files: int = DEFAULT_PARTITION_FILES,
) -> tuple[int, int]:
    """Prefix-partitioned cascade (spec mgu-scale-unification.md A.2) → `dirs_all` + `n_children_tbl`.

    A dir and all its descendants share their depth-`k` prefix, so each
    distinct prefix is cascaded on its own from a partition-sized input table
    built straight off the listing (a `name` range predicate lets DuckDB skip
    the row groups of other partitions when the shards are prefix-contiguous,
    as bulk-list's are). Each cascade climbs to the root, leaving stub rows
    for the ancestors above depth `k`; a final *top* cascade covers the rows
    at depth ≤ `k` (files there, and the shallow dirs' own `n_desc=1`
    contributions). Every input row lands in exactly one cascade, and
    `dirs_all` is one-row-per-(cascade, level, path), so the caller's GROUP BY
    folds the stubs exactly. Peak memory ∝ the largest cascade.

    Keys are *directories* — the depth-`k` prefixes of rows deeper than `k`
    (a file at exactly depth `k` is its own prefix and would be a one-file
    partition; spec `mgu-scale-a3-gate.md` ask 1) — and are packed in key
    order into cascades of up to `partition_files` files
    (:func:`_batch_partitions`), so a fleet whose keys are one huge subtree
    plus thousands of tiny ones runs as a few cascades, not thousands. Each
    cascade costs ~50 ms of SQL on top of its data.

    `files_sql_for(where)` returns the canonical file-row SELECT restricted by
    a predicate over the raw listing columns. Returns `(cascades, keys)`,
    excluding top.
    """
    part_of_path = _part_expr('path', k, min_nseg=k + 1)
    nseg_p = _NSEG_EXPR.format(col='p')
    parent_of_p = _PARENT_EXPR.format(col='p')
    group_cols = labels.cols if labels is not None else ()
    max_cols = side.cols if side is not None else ()
    keys = ''.join(f', {c}' for c in group_cols)

    # One pass over the listing: the partition keys and their row counts.
    parts = con.execute(f"""
        SELECT {part_of_path} AS part, COUNT(*) AS n
        FROM ({files_sql_for('')})
        GROUP BY 1
        ORDER BY 1 NULLS FIRST
    """).fetchall()
    n_top = next((n for p, n in parts if p is None), 0)
    keyed = [(p, n) for p, n in parts if p is not None]
    part_keys = [p for p, _ in keyed]
    batches = _batch_partitions(keyed, partition_files)
    _stage(f"partition depth {k}: {len(part_keys)} dir keys → {len(batches)} cascades"
           f" (largest key {max((n for _, n in keyed), default=0)} files, batch ≤ {partition_files} files),"
           f" {n_top} files at depth ≤ {k}")

    # Dirty keys (`a//b`) canonicalize to a different sort position than their
    # raw name, so the per-partition `name` range can miss them. They are rare;
    # gather them once and give every partition its share by exact key.
    con.execute(f"""
        CREATE OR REPLACE TABLE dirty AS
        {files_sql_for(' AND name <> ' + "rtrim(regexp_replace(name, '/+', '/', 'g'), '/')")}
    """)
    n_dirty = con.execute("SELECT COUNT(*) FROM dirty").fetchone()[0]
    _stage(f"{n_dirty} dirty keys held aside")
    # Folder placeholders (`…/` objects) are their directory's own row, in
    # whichever cascade their path falls; no cascade may synthesize that row.
    con.execute(f"""
        CREATE OR REPLACE TABLE placeholders AS
        SELECT DISTINCT path FROM ({files_sql_for('')}) WHERE kind = 'dir'
    """)
    exclude = "(SELECT path FROM placeholders)"

    con.execute("CREATE OR REPLACE TABLE partitions (part VARCHAR)")
    if part_keys:
        con.executemany("INSERT INTO partitions VALUES (?)", [(key,) for key in part_keys])

    # Accumulators: every cascade appends its (level, path) rows here.
    con.execute("DROP TABLE IF EXISTS dirs_all")
    con.execute("DROP TABLE IF EXISTS n_children_parts")

    def _append(first: bool) -> None:
        if first:
            con.execute("CREATE TABLE dirs_all AS SELECT * FROM dirs_all_p")
            con.execute("CREATE TABLE n_children_parts AS SELECT * FROM n_children_p")
        else:
            con.execute("INSERT INTO dirs_all SELECT * FROM dirs_all_p")
            con.execute("INSERT INTO n_children_parts SELECT * FROM n_children_p")
        con.execute("DROP TABLE dirs_all_p")
        con.execute("DROP TABLE n_children_p")

    for i, batch in enumerate(batches):
        first, last = _sql_lit(batch[0]), _sql_lit(batch[-1])
        lits = ', '.join(_sql_lit(key) for key in batch)
        # Clean rows (name == canonical) under a key sit in the contiguous name
        # range [key/, key0) ('/' + 1 == '0'); a batch of consecutive keys is
        # one range [first/, last0). Rows between two keys (files at depth ≤ k
        # whose prefix sorts between them) fall in the range too — the exact
        # `part IN (…)` predicate on top is what makes the range merely a hint.
        clean = files_sql_for(
            f" AND name = rtrim(regexp_replace(name, '/+', '/', 'g'), '/')"
            f" AND name >= {first} || '/' AND name < {last} || '0'"
        )
        con.execute(f"""
            CREATE OR REPLACE TABLE inputs_p AS
            SELECT * FROM ({clean}) WHERE {part_of_path} IN ({lits})
            UNION ALL
            SELECT * FROM dirty WHERE {part_of_path} IN ({lits})
        """)
        tag = f"[{i + 1}/{len(batches)} {batch[0]}" + (f" … {batch[-1]} ({len(batch)} keys)" if len(batch) > 1 else '') + '] '
        # Ancestors inside the partition only (depth ≥ k); shallower ones are
        # the top cascade's, so their n_desc=1 is counted exactly once.
        con.execute(f"""
            CREATE OR REPLACE TABLE dir_paths_p AS
            WITH RECURSIVE anc(p) AS (
                SELECT DISTINCT parent FROM inputs_p
                UNION
                SELECT {parent_of_p} FROM anc WHERE {nseg_p} > {k}
            )
            SELECT DISTINCT p AS path FROM anc WHERE {nseg_p} >= {k}
        """)
        con.execute(_dir_rows_insert('inputs_p', 'dir_paths_p', extra_dir_cols, labels, side, exclude=exclude))
        con.execute("DROP TABLE dir_paths_p")
        _build_dirs_cascade(
            con, sum_cols=sum_cols, mean_mtime=mean_mtime,
            src='inputs_p', out='dirs_all_p', n_children='n_children_p',
            tag=tag, group_cols=group_cols, max_cols=max_cols,
        )
        _append(first=i == 0)
    con.execute("DROP TABLE dirty")

    # Top cascade: files shallower than k, plus every shallow dir — the
    # ancestors of the partition keys and of the shallow files, and the root.
    con.execute(f"""
        CREATE OR REPLACE TABLE inputs_p AS
        SELECT * FROM ({files_sql_for('')}) WHERE {part_of_path} IS NULL
    """)
    con.execute(f"""
        CREATE OR REPLACE TABLE dir_paths_p AS
        WITH RECURSIVE anc(p) AS (
            SELECT DISTINCT parent FROM inputs_p
            UNION
            SELECT DISTINCT {_PARENT_EXPR.format(col='part')} FROM partitions
            UNION
            SELECT '' AS p
            UNION
            SELECT {parent_of_p} FROM anc WHERE p != ''
        )
        SELECT DISTINCT p AS path FROM anc
    """)
    con.execute(_dir_rows_insert('inputs_p', 'dir_paths_p', extra_dir_cols, labels, side, exclude=exclude))
    con.execute("DROP TABLE dir_paths_p")
    con.execute("DROP TABLE partitions")
    con.execute("DROP TABLE placeholders")
    _build_dirs_cascade(
        con, sum_cols=sum_cols, mean_mtime=mean_mtime,
        src='inputs_p', out='dirs_all_p', n_children='n_children_p', tag='[top] ',
        group_cols=group_cols, max_cols=max_cols,
    )
    _append(first=not part_keys)
    con.execute("DROP TABLE inputs_p")

    # A shallow dir's children are split across cascades (its depth-k children
    # each count from their own partition); fold to one row per path — the
    # join below multiplies rows otherwise.
    con.execute(f"""
        CREATE OR REPLACE TABLE n_children_tbl AS
        SELECT path{keys}, SUM(n_children)::BIGINT AS n_children
        FROM n_children_parts
        GROUP BY path{keys}
    """)
    con.execute("DROP TABLE n_children_parts")
    return len(batches), len(part_keys)


def aggregate_listing_to_parquet(
    listing_sql: str,
    bucket: str,
    scheme: str,
    out_parquet: str,
    con: "duckdb.DuckDBPyConnection | None" = None,
    memory_limit: str = '8GB',
    temp_dir: str | None = None,
    max_temp_size: str | None = None,
    pivot_sums: tuple[str, ...] = (),
    mean_mtime: bool = False,
    db: str | None = None,
    partition_depth: int = 0,
    label: str | None = None,
    label_cols: tuple[str, ...] = (),
    side: str | None = None,
    max_cols: tuple[str, ...] = (),
    size_hist: bool = False,
    partition_files: int = DEFAULT_PARTITION_FILES,
) -> dict:
    """Out-of-core: layer-1 listing (via `listing_sql`) → layer-2 parquet on disk.

    `listing_sql` is a parenthesized SELECT (from :func:`disk_tree.listing.prepare_listing`)
    exposing `bucket, name, size_bytes, created, storage_class_id`.

    Never materializes the full input in pandas. Intermediate DuckDB tables spill
    to `temp_dir` under `memory_limit`. Writes the final canonical DataFrame
    (files + synthesized dirs, n_desc/n_children/depth attached) as a single
    parquet at `out_parquet`. Returns a small stats dict for the caller.

    Fleet-scale knobs (spec mgu-scale-unification.md, item A):

    - `db`: run the cascade in a file-backed database (a `.duckdb` path, or a
      directory to create a temporary one in) so the level tables live in the
      buffer pool and page to disk under `memory_limit`, instead of being
      pinned in RAM as an in-memory database's base tables are. `con` is then
      only used to read the listing's schema.
    - `partition_depth`: cascade each distinct depth-k *directory* prefix
      separately (see :func:`_build_partitioned`) — peak memory ∝ the largest
      cascade rather than the whole listing. 0 = one cascade over everything.
    - `partition_files`: pack partitions, in key order, into cascades of up
      to this many files (≤ 0: one cascade per key). The memory knob: a
      cascade's peak is ∝ its files (~4.4 KB/file with every extension on).

    Output is byte-identical for every combination.

    Folder placeholders — listing names ending in `/` — are objects at the
    directory they name: that directory's own row carries them (`n_files`
    counts them, `size`/`mtime` are theirs); they are never file children
    (spec `mgu-scale-a3-gate.md` ask 2). `a//b` names collapse to `a/b`
    (ask 3: the deterministic, subtree-preserving policy is to read them as
    the path the user meant).

    `label` (item B): a parquet of `prefix → <label columns>` (`label_cols`;
    default: every column but `prefix`), joined onto every input row by
    deepest matching prefix (see :class:`_Labels`) and carried as extra group
    keys through the cascade — the output holds one row per
    `(path, *label_cols)`, sorted `(depth, path, *label_cols)`, label columns
    right after `path`; Σ over a path's slices equals its unlabeled row.

    `side` + `max_cols` (item D.4): a parquet keyed by `path` (`.` = root;
    optional `bucket`) whose `max_cols` are joined onto every row by exact
    path and folded with MAX through the cascade — a dir's value is the max
    over itself and its subtree (e.g. `last_ts` from `access state`). The
    columns land after the extension columns, before `uri`.

    `size_hist` (item E): per path, a log2 histogram of descendant files by
    size as `size_hist_n` / `size_hist_bytes` (LIST(BIGINT) of
    `SIZE_HIST_BINS` each; see `agg_ext.size_bin`). Additive, so it rides the
    cascade as one count and one byte column per bin.
    """
    import duckdb as _duckdb
    if partition_depth < 0:
        raise ValueError(f"partition_depth must be >= 0; got {partition_depth}")
    db_con, db_tmp = _open_db(db)
    if db_con is not None:
        con = db_con
    elif con is None:
        con = _duckdb.connect()
    con.execute(f"SET memory_limit = '{memory_limit}'")
    con.execute("SET preserve_insertion_order = false")
    # Fewer threads → fewer concurrent per-operator buffers (sort runs +
    # parquet-writer row groups scale with thread count and sit largely
    # outside `memory_limit` accounting).
    con.execute("SET threads = 8")
    # Per-invocation spill dir: DuckDB's default temp_directory is a *relative*
    # `.tmp/`, so concurrent imports sharing a cwd corrupt each other's spill
    # files. On failure the dir is left in place (spill files may still be
    # referenced by the connection); on success it's removed.
    import tempfile
    spill_dir = temp_dir or tempfile.mkdtemp(prefix='disk-tree-spill-')
    con.execute(f"SET temp_directory = '{spill_dir}'")
    # DuckDB auto-caps spill at free-disk-at-launch; a concurrent writer
    # shrinking that snapshot kills the sort even when disk frees up later.
    if max_temp_size:
        con.execute(f"SET max_temp_directory_size = '{max_temp_size}'")

    from disk_tree.backends.url import canonical
    scan_root = canonical(f'{scheme}://{bucket}')  # `file` roots → the bare path

    # Extension columns (see find/agg_ext.py): per-pivot-value byte sums as
    # file-level contributions, plus the exact HUGEINT Σ mtime·size partial.
    from .agg_ext import MT_WSUM, MTIME_MEAN, check_pivot_values, pivot_col
    sum_cols: list[str] = []
    extra_file_cols = ''
    for col in pivot_sums:
        vals = check_pivot_values(col, [
            r[0] for r in con.execute(
                f"SELECT DISTINCT {col} FROM {listing_sql} "
                f"WHERE bucket = '{bucket}' AND {col} IS NOT NULL ORDER BY 1"
            ).fetchall()
        ])
        for v in vals:
            name = pivot_col(col, v)
            extra_file_cols += (
                f", CASE WHEN {col} = {_sql_lit(v)} THEN size_bytes ELSE 0 END::BIGINT AS {name}"
            )
            sum_cols.append(name)
    if mean_mtime:
        extra_file_cols += (
            f", (COALESCE(floor(epoch(created)), 0)::BIGINT::HUGEINT * size_bytes::HUGEINT) AS {MT_WSUM}"
        )
    hist_n: list[str] = []
    hist_b: list[str] = []
    if size_hist:
        from .agg_ext import SIZE_HIST_BINS, size_hist_cols
        hist_n, hist_b = size_hist_cols()
        # bit_length(size) for size > 0; log2 is exact at powers of two, and
        # `floor(log2(2^k − 1))` = k−1 for k ≤ 53 — the test locks both edges.
        bin_expr = (
            "CASE WHEN size_bytes <= 0 THEN 0 "
            f"ELSE LEAST(floor(log2(size_bytes))::INTEGER + 1, {SIZE_HIST_BINS - 1}) END"
        )
        for b, (cn, cb) in enumerate(zip(hist_n, hist_b)):
            extra_file_cols += (
                f", CASE WHEN {bin_expr} = {b} THEN 1 ELSE 0 END::BIGINT AS {cn}"
                f", CASE WHEN {bin_expr} = {b} THEN size_bytes ELSE 0 END::BIGINT AS {cb}"
            )
        sum_cols.extend([*hist_n, *hist_b])
    pivot_pass = ''.join(f', {col}' for col in pivot_sums)
    extra_dir_cols = ''.join(f', 0::BIGINT AS {c}' for c in sum_cols)
    if mean_mtime:
        extra_dir_cols += f', 0::HUGEINT AS {MT_WSUM}'

    labels = _Labels(con, label, label_cols) if label is not None else None
    group_cols: tuple[str, ...] = labels.cols if labels is not None else ()
    keys = ''.join(f', {c}' for c in group_cols)
    if labels is not None:
        _stage(f"labels: {labels.n} prefixes at depths {sorted(labels.depths)} → {list(group_cols)}")

    side_tbl = _Side(con, side, max_cols, bucket) if side is not None else None
    if side_tbl is None and max_cols:
        raise ValueError("--max-col needs --side")
    max_names = ''.join(f', {c}' for c in max_cols)
    if side_tbl is not None:
        _stage(f"side: {side_tbl.n} rows for {bucket} → MAX({list(max_cols)})")

    def files_sql_for(where: str) -> str:
        sql = _files_select(listing_sql, bucket, extra_file_cols, pivot_pass, where=where)
        if labels is not None:
            sql = labels.join(sql)
        if side_tbl is not None:
            sql = side_tbl.join(sql)
        return sql

    con.execute("DROP TABLE IF EXISTS inputs")
    con.execute("DROP VIEW IF EXISTS files_src")
    if partition_depth:
        # The listing is never materialized whole: each partition (and the
        # final COPY's file leg) reads it back through `files_src`.
        con.execute(f"CREATE VIEW files_src AS {files_sql_for('')}")
        n_files = con.execute("SELECT COUNT(*) FROM files_src").fetchone()[0]
        _stage(f"listing: {n_files} file rows")
        if n_files == 0:
            raise ValueError(f"no rows for bucket {bucket!r}")
        n_partitions, n_keys = _build_partitioned(
            con, files_sql_for, partition_depth,
            sum_cols=tuple(sum_cols), mean_mtime=mean_mtime, extra_dir_cols=extra_dir_cols,
            labels=labels, side=side_tbl, partition_files=partition_files,
        )
    else:
        n_partitions = n_keys = 0
        # Build the `inputs` table without a pandas roundtrip: files first, then
        # synthesized dir rows for every unique ancestor path.
        con.execute(f"CREATE TABLE inputs AS {files_sql_for('')}")
        n_files = con.execute("SELECT COUNT(*) FROM inputs").fetchone()[0]
        _stage(f"inputs table: {n_files} file rows")
        if n_files == 0:
            raise ValueError(f"no rows for bucket {bucket!r}")

        # Synthesized dir rows: recursively enumerate all unique ancestor paths (incl. '' root).
        parent_of_p = _PARENT_EXPR.format(col='p')
        con.execute(f"""
            CREATE OR REPLACE TABLE dir_paths AS
            WITH RECURSIVE anc(p) AS (
                SELECT DISTINCT parent FROM inputs
                UNION
                SELECT {parent_of_p} FROM anc WHERE p != ''
            )
            SELECT DISTINCT p AS path FROM anc
        """)
        con.execute(_dir_rows_insert('inputs', 'dir_paths', extra_dir_cols, labels, side_tbl))
        con.execute("DROP TABLE dir_paths")
        _stage("dir rows synthesized")

        # SQL-only tail: the pandas tail in `_aggregate_shared` materializes every
        # file row (path/parent/uri object strings) — ~60GB RSS at 92.7M rows.
        # Here the normalization + concat + sort stay inside DuckDB (spillable) and
        # the output goes straight to parquet via COPY.
        _build_dirs_cascade(
            con, sum_cols=tuple(sum_cols), mean_mtime=mean_mtime,
            group_cols=group_cols, max_cols=max_cols,
        )
        con.execute("CREATE VIEW files_src AS SELECT * FROM inputs WHERE kind = 'file'")
    _stage("cascade done")
    parent_of_agg = _PARENT_EXPR.format(col='path')
    # Extension outputs mirror `_aggregate_shared`'s: pivot sums pass through as
    # BIGINT; mt_wsum divides into mtime_mean (HUGEINT-exact until the division).
    # Column order (…, parent, <extras>, uri, depth) matches the pandas-tail
    # concat order so all engines stay byte-identical.
    extra_out = ''.join(f', SUM(d.{c})::BIGINT AS {c}' for c in sum_cols)
    if mean_mtime:
        extra_out += (
            f", CASE WHEN SUM(d.size) > 0"
            # HUGEINT::DOUBLE is not correctly rounded (1-ULP drift past 2^64 vs
            # Python's int->float in the other engines' `mean_of`); the VARCHAR
            # parse is, keeping `mtime_mean` byte-identical across engines.
            f" THEN SUM(d.{MT_WSUM})::VARCHAR::DOUBLE / SUM(d.size)::DOUBLE"
            f" END AS {MTIME_MEAN}"
        )
    extra_out += ''.join(f', MAX(d.{c}) AS {c}' for c in max_cols)
    # The histogram's per-bin cascade columns pack into two LIST columns on
    # the way out; the pivot sums stay as they are.
    hist_set = set(hist_n) | set(hist_b)
    plain_sums = [c for c in sum_cols if c not in hist_set]
    hist_pack = ''
    if size_hist:
        from .agg_ext import SIZE_HIST_BYTES, SIZE_HIST_N
        hist_pack = (
            f", [{', '.join(hist_n)}]::BIGINT[] AS {SIZE_HIST_N}"
            f", [{', '.join(hist_b)}]::BIGINT[] AS {SIZE_HIST_BYTES}"
        )
    # `dirs_final` packs the histogram; the COPY then reads the packed names.
    base_names = ''.join(f', {c}' for c in [*plain_sums, *([MTIME_MEAN] if mean_mtime else [])])
    extra_final = base_names + hist_pack + max_names
    extra_names = base_names + (f', {SIZE_HIST_N}, {SIZE_HIST_BYTES}' if size_hist else '') + max_names
    file_extra = ''.join(f', {c}' for c in plain_sums)
    if mean_mtime:
        file_extra += f', mtime::DOUBLE AS {MTIME_MEAN}'
    file_extra += hist_pack + max_names
    # Materialize the (small — one row per dir) aggregated dir table first, so
    # the join + group-by run alone; the giant COPY below is then a pure
    # union + sort + write. One operator at a time: v3/v4 post-mortems showed
    # the combined pipeline allocating far past `memory_limit` (63-95GB RSS
    # under 32-50GB caps) with no way to tell which operator was responsible.
    d_keys = ''.join(f', d.{c}' for c in group_cols)
    nc_on = ''.join(f' AND d.{c} IS NOT DISTINCT FROM nc.{c}' for c in group_cols)
    con.execute(f"""
        CREATE OR REPLACE TABLE dirs_final AS
        WITH dirs_agg AS (
            SELECT
                d.path AS path{d_keys},
                SUM(d.size)::BIGINT AS size,
                MAX(d.mtime)::BIGINT AS mtime,
                SUM(d.n_desc)::BIGINT AS n_desc,
                SUM(d.n_files)::BIGINT AS n_files,
                COALESCE(MAX(nc.n_children), 0)::BIGINT AS n_children{extra_out}
            FROM dirs_all d
            LEFT JOIN n_children_tbl nc ON d.path = nc.path{nc_on}
            GROUP BY d.path{d_keys}
        )
        -- pandas-tail parity: root row '' → path '.', parent ''; other
        -- dirs whose computed parent is '' (top-level) get parent '.'
        SELECT
            CASE WHEN path = '' THEN '.' ELSE path END AS path{keys},
            size, mtime, n_desc, n_files, n_children,
            'dir' AS kind,
            CASE
                WHEN path = '' THEN ''
                WHEN {parent_of_agg} = '' THEN '.'
                ELSE {parent_of_agg}
            END AS parent{extra_final},
            CASE WHEN path = '' THEN '{scan_root}' ELSE '{scan_root}/' || path END AS uri
        FROM dirs_agg
    """)
    n_dirs = con.execute("SELECT COUNT(*) FROM dirs_final").fetchone()[0]
    con.execute("DROP TABLE dirs_all")
    con.execute("DROP TABLE n_children_tbl")
    _stage(f"dirs_final: {n_dirs} dirs")

    # COPY straight from the union query — materializing it as a table first
    # doubles the on-disk footprint (all 92.7M file rows a second time) and
    # exhausted a ~95GiB spill budget at CW scale. `uri` is derived here rather
    # than stored per input row for the same reason.
    # Bounded row groups (DuckDB's default is ~120K rows): the read side —
    # server and Pages Functions alike — decodes every group overlapping a
    # listing's (depth, path-prefix) range, and the measured optimum is 64K
    # (`storage/base.py`). A Worker reading a 7M-row scan can't afford 120K-row
    # groups either.
    from disk_tree.storage.base import BLOB_ROW_GROUP_SIZE
    tmp_out = out_parquet + '.tmp'
    con.execute(f"""
        COPY (
        WITH unioned AS (
            SELECT path{keys}, size, mtime, n_desc, n_files, n_children, kind, parent{extra_names}, uri FROM dirs_final
            UNION ALL
            SELECT path{keys}, size, mtime,
                   1::BIGINT AS n_desc, 1::BIGINT AS n_files, 0::BIGINT AS n_children,
                   kind, parent{file_extra},
                   ('{scan_root}/' || path)::VARCHAR AS uri
            FROM files_src
            WHERE kind = 'file'
        )
        SELECT *,
               CASE WHEN path = '.' THEN 0
                    ELSE length(path) - length(replace(path, '/', '')) + 1
               END::BIGINT AS depth
        FROM unioned
        ORDER BY depth, path{''.join(f', {c} NULLS FIRST' for c in group_cols)}
        ) TO '{tmp_out}' (FORMAT PARQUET, ROW_GROUP_SIZE {BLOB_ROW_GROUP_SIZE})
    """)
    _stage("COPY done")
    rows = con.execute(f"SELECT COUNT(*) FROM read_parquet('{tmp_out}')").fetchone()[0]
    os.replace(tmp_out, out_parquet)

    # Root stats: with labels the root is one row per slice — sum them.
    root = con.execute(f"""
        SELECT SUM(size)::BIGINT AS size, MAX(mtime)::BIGINT AS mtime,
               SUM(n_desc)::BIGINT AS n_desc, SUM(n_files)::BIGINT AS n_files,
               SUM(n_children)::BIGINT AS n_children
        FROM read_parquet('{out_parquet}')
        WHERE path = '.'
    """).df()
    root = root.iloc[0] if len(root) and root.iloc[0]['size'] is not None and not pd.isna(root.iloc[0]['size']) else None
    con.execute("DROP VIEW files_src")
    con.execute("DROP TABLE IF EXISTS inputs")
    con.execute("DROP TABLE dirs_final")
    con.execute("DROP TABLE IF EXISTS labels")
    con.execute("DROP TABLE IF EXISTS side")
    if temp_dir is None:
        import shutil
        shutil.rmtree(spill_dir, ignore_errors=True)
    if db_con is not None:
        db_con.close()
        if db_tmp is not None:
            for p in (db_tmp, db_tmp + '.wal'):
                if os.path.exists(p):
                    os.remove(p)
    max_rss_mb = _max_rss_mb()
    _stage(f"done: peak RSS {max_rss_mb:,.0f} MiB")
    return {
        'rows': int(rows),
        'files': int(n_files),
        'root_size': int(root['size']) if root is not None else 0,
        'root_n_desc': int(root['n_desc']) if root is not None else 0,
        'root_n_files': int(root['n_files']) if root is not None else 0,
        'root_n_children': int(root['n_children']) if root is not None else 0,
        'root_mtime': int(root['mtime']) if root is not None else 0,
        'partitions': int(n_partitions),
        'partition_keys': int(n_keys),
        'max_rss_mb': round(max_rss_mb, 1),
    }
