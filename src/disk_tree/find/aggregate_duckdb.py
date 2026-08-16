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
from typing import TYPE_CHECKING

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


def _stage(msg: str) -> None:
    """Stage-boundary log line (stderr): OOM post-mortems need to know which
    statement was in flight — a SIGKILL leaves no traceback."""
    print(f"[agg {datetime.now().isoformat(timespec='seconds')}] {msg}", file=sys.stderr, flush=True)


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
            uri::VARCHAR AS uri
        FROM inputs_df
    """)
    con.unregister('inputs_df')


def _build_dirs_cascade(
    con: "duckdb.DuckDBPyConnection",
    sum_cols: tuple[str, ...] = (),
    mean_mtime: bool = False,
) -> None:
    """Bottom-up group-by cascade over `inputs` → `dirs_all` + `n_children_tbl` tables.

    `sum_cols` are extra monoid columns on `inputs` (per-file contributions;
    0 on dir rows) summed through the cascade unchanged. With `mean_mtime`,
    `inputs` must carry `mt_wsum` (HUGEINT Σ mtime·size partials — exact, no
    float-summation order sensitivity); consumers divide it into the
    `mtime_mean` output column (see find/agg_ext.py).
    """
    from .agg_ext import MT_WSUM
    cascade_cols = [*sum_cols, *([MT_WSUM] if mean_mtime else [])]
    extra_sel = ''.join(f', {c}' for c in cascade_cols)
    # SUM(BIGINT) widens to HUGEINT in DuckDB — cast the pivot sums back down
    # (they're bounded by total size); mt_wsum genuinely needs the width.
    extra_sum = ''.join(
        f", SUM({c})::{'HUGEINT' if c == MT_WSUM else 'BIGINT'} AS {c}"
        for c in cascade_cols
    )

    parent_of_path = _PARENT_EXPR.format(col='path')

    # dirs0 = walk-emitted / synthesized dir rows; each contributes n_desc=1
    # and n_files=0 (dirs don't count as files) at its own path.
    con.execute(f"""
        CREATE OR REPLACE TABLE dirs0 AS
        SELECT path, size, mtime, 1::BIGINT AS n_desc, 0::BIGINT AS n_files{extra_sel}
        FROM inputs
        WHERE kind = 'dir'
    """)

    # n_children per parent: count of input rows (files + dirs) whose parent is this path.
    # Faithful to pandas `grouped.size()` at level 0 (only). Rows with path='' don't count
    # (that's the scan root; it has no parent).
    con.execute(f"""
        CREATE OR REPLACE TABLE n_children_tbl AS
        SELECT parent AS path, COUNT(*)::BIGINT AS n_children
        FROM inputs
        WHERE path != ''
        GROUP BY parent
    """)

    # `cur` seed: all input rows with n_desc=1; n_files=1 for files, 0 for dirs.
    # Loop group-by-parent to synthesize each subsequent level's dir rows
    # (bottom-up). Stop when nothing left to promote.
    con.execute(f"""
        CREATE OR REPLACE TABLE level_cur AS
        SELECT path, size, mtime,
               1::BIGINT AS n_desc,
               CASE WHEN kind = 'file' THEN 1 ELSE 0 END::BIGINT AS n_files{extra_sel}
        FROM inputs
    """)
    level_tables: list[str] = []
    level = 0
    while True:
        next_tbl = f'level_{level}'
        con.execute(f"""
            CREATE OR REPLACE TABLE {next_tbl} AS
            SELECT {parent_of_path} AS path,
                   SUM(size)::BIGINT AS size,
                   MAX(mtime)::BIGINT AS mtime,
                   SUM(n_desc)::BIGINT AS n_desc,
                   SUM(n_files)::BIGINT AS n_files{extra_sum}
            FROM level_cur
            WHERE path != ''
            GROUP BY 1
        """)
        cnt = con.execute(f"SELECT COUNT(*) FROM {next_tbl}").fetchone()[0]
        _stage(f"cascade level {level}: {cnt} rows")
        if cnt == 0:
            break
        level_tables.append(next_tbl)
        # promote for next iteration
        con.execute(f"CREATE OR REPLACE TABLE level_cur AS SELECT * FROM {next_tbl}")
        level += 1

    # Union all dir levels + dirs0, group by path, attach n_children.
    base_cols = f"path, size, mtime, n_desc, n_files{extra_sel}"
    if level_tables:
        levels_union = " UNION ALL ".join(
            f"SELECT {base_cols} FROM {t}" for t in level_tables
        )
        con.execute(f"""
            CREATE OR REPLACE TABLE dirs_all AS
            SELECT {base_cols} FROM dirs0
            UNION ALL
            {levels_union}
        """)
    else:
        con.execute(f"""
            CREATE OR REPLACE TABLE dirs_all AS
            SELECT {base_cols} FROM dirs0
        """)

    # The level tables are folded into dirs_all; keeping them alive doubles the
    # cascade's disk footprint (spill exhaustion at the 92.7M-row scale).
    for t in ['dirs0', 'level_cur', *level_tables]:
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
            f" THEN SUM(d.{MT_WSUM})::DOUBLE / SUM(d.size)::DOUBLE"
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
) -> dict:
    """Out-of-core: layer-1 listing (via `listing_sql`) → layer-2 parquet on disk.

    `listing_sql` is a parenthesized SELECT (from :func:`disk_tree.listing.prepare_listing`)
    exposing `bucket, name, size_bytes, created, storage_class_id`.

    Never materializes the full input in pandas. Intermediate DuckDB tables spill
    to `temp_dir` under `memory_limit`. Writes the final canonical DataFrame
    (files + synthesized dirs, n_desc/n_children/depth attached) as a single
    parquet at `out_parquet`. Returns a small stats dict for the caller.
    """
    import duckdb as _duckdb
    if con is None:
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

    scan_root = f'{scheme}://{bucket}'
    # Collapse consecutive slashes + strip trailing slashes so `a//b` reads as
    # `a/b`. Keys with empty path components exist in real listings (marin's
    # 2026-08-14 west4 scan has `tokenized/…//.artifact.json`); leaving the
    # trailing slash on the intermediate dir breaks the parent-of regex below
    # (regexp_extract fails to match a trailing-`/` string → returns '' → the
    # dir gets hoisted to the tree root, moving bytes across subtrees).
    canonical_name = "rtrim(regexp_replace(name, '/+', '/', 'g'), '/')"
    parent_of_name = _PARENT_EXPR.format(col='canonical')

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
    pivot_pass = ''.join(f', {col}' for col in pivot_sums)

    # Build the `inputs` table without a pandas roundtrip: files first, then
    # synthesized dir rows for every unique ancestor path.
    con.execute(f"""
        DROP TABLE IF EXISTS inputs;
        CREATE TABLE inputs AS
        WITH canon AS (
            SELECT
                {canonical_name} AS canonical,
                size_bytes,
                created{pivot_pass}
            FROM {listing_sql}
            WHERE bucket = '{bucket}'
        )
        SELECT
            canonical AS path,
            size_bytes::BIGINT AS size,
            -- floor, not ::BIGINT (which rounds): epoch-seconds truncate by
            -- convention, and the stream engine's pyarrow int-division floors —
            -- rounding here skewed ~50% of real (sub-second) timestamps +1s
            COALESCE(floor(epoch(created)), 0)::BIGINT AS mtime,
            'file'::VARCHAR AS kind,
            {parent_of_name} AS parent{extra_file_cols}
        FROM canon
    """)

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
    extra_dir_cols = ''.join(f', 0::BIGINT AS {c}' for c in sum_cols)
    if mean_mtime:
        extra_dir_cols += f', 0::HUGEINT AS {MT_WSUM}'
    con.execute(f"""
        INSERT INTO inputs
        SELECT
            path,
            0::BIGINT AS size,
            0::BIGINT AS mtime,
            'dir'::VARCHAR AS kind,
            {_PARENT_EXPR.format(col='path')} AS parent{extra_dir_cols}
        FROM dir_paths
    """)
    con.execute("DROP TABLE dir_paths")
    _stage("dir rows synthesized")

    # SQL-only tail: the pandas tail in `_aggregate_shared` materializes every
    # file row (path/parent/uri object strings) — ~60GB RSS at 92.7M rows.
    # Here the normalization + concat + sort stay inside DuckDB (spillable) and
    # the output goes straight to parquet via COPY.
    _build_dirs_cascade(con, sum_cols=tuple(sum_cols), mean_mtime=mean_mtime)
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
            f" THEN SUM(d.{MT_WSUM})::DOUBLE / SUM(d.size)::DOUBLE"
            f" END AS {MTIME_MEAN}"
        )
    extra_names = ''.join(f', {c}' for c in [*sum_cols, *([MTIME_MEAN] if mean_mtime else [])])
    file_extra = ''.join(f', {c}' for c in sum_cols)
    if mean_mtime:
        file_extra += f', mtime::DOUBLE AS {MTIME_MEAN}'
    # Materialize the (small — one row per dir) aggregated dir table first, so
    # the join + group-by run alone; the giant COPY below is then a pure
    # union + sort + write. One operator at a time: v3/v4 post-mortems showed
    # the combined pipeline allocating far past `memory_limit` (63-95GB RSS
    # under 32-50GB caps) with no way to tell which operator was responsible.
    con.execute(f"""
        CREATE OR REPLACE TABLE dirs_final AS
        WITH dirs_agg AS (
            SELECT
                d.path AS path,
                SUM(d.size)::BIGINT AS size,
                MAX(d.mtime)::BIGINT AS mtime,
                SUM(d.n_desc)::BIGINT AS n_desc,
                SUM(d.n_files)::BIGINT AS n_files,
                COALESCE(MAX(nc.n_children), 0)::BIGINT AS n_children{extra_out}
            FROM dirs_all d
            LEFT JOIN n_children_tbl nc ON d.path = nc.path
            GROUP BY d.path
        )
        -- pandas-tail parity: root row '' → path '.', parent ''; other
        -- dirs whose computed parent is '' (top-level) get parent '.'
        SELECT
            CASE WHEN path = '' THEN '.' ELSE path END AS path,
            size, mtime, n_desc, n_files, n_children,
            'dir' AS kind,
            CASE
                WHEN path = '' THEN ''
                WHEN {parent_of_agg} = '' THEN '.'
                ELSE {parent_of_agg}
            END AS parent{extra_names},
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
    tmp_out = out_parquet + '.tmp'
    con.execute(f"""
        COPY (
        WITH unioned AS (
            SELECT path, size, mtime, n_desc, n_files, n_children, kind, parent{extra_names}, uri FROM dirs_final
            UNION ALL
            SELECT path, size, mtime,
                   1::BIGINT AS n_desc, 1::BIGINT AS n_files, 0::BIGINT AS n_children,
                   kind, parent{file_extra},
                   ('{scan_root}/' || path)::VARCHAR AS uri
            FROM inputs WHERE kind = 'file'
        )
        SELECT *,
               CASE WHEN path = '.' THEN 0
                    ELSE length(path) - length(replace(path, '/', '')) + 1
               END::BIGINT AS depth
        FROM unioned
        ORDER BY depth, path
        ) TO '{tmp_out}' (FORMAT PARQUET)
    """)
    _stage("COPY done")
    rows = con.execute(f"SELECT COUNT(*) FROM read_parquet('{tmp_out}')").fetchone()[0]
    os.replace(tmp_out, out_parquet)

    root = con.execute(f"""
        SELECT * FROM read_parquet('{out_parquet}')
        WHERE path = '.' LIMIT 1
    """).df()
    root = root.iloc[0] if len(root) else None
    if temp_dir is None:
        import shutil
        shutil.rmtree(spill_dir, ignore_errors=True)
    return {
        'rows': int(rows),
        'files': int(n_files),
        'root_size': int(root['size']) if root is not None else 0,
        'root_n_desc': int(root['n_desc']) if root is not None else 0,
        'root_n_files': int(root['n_files']) if root is not None else 0,
        'root_n_children': int(root['n_children']) if root is not None else 0,
        'root_mtime': int(root['mtime']) if root is not None else 0,
    }
