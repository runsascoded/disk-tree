"""Layer-2a: per-path per-day per-op rollups from canonical access rows.

Same bottom-up parent-synthesis machinery as size scans
(:mod:`disk_tree.find.aggregate_duckdb`), but the accumulators are
``n_ops`` / ``bytes_out`` / ``n_requesters`` per (path, day, op) instead of
``size`` / ``n_desc``.

Structural upshot: the widgets (treemap, series, diff) work over ops and
egress *the same way* they work over bytes-at-rest — a treemap sized by
``bytes_out`` shows hot-egress paths; a time series over ``n_ops`` shows
request-rate shifts.

Output schema (canonical layer-2a parquet):

::

    path, parent, depth, kind             -- tree mechanics (same as scans)
    day                                    -- UTC day (DATE)
    op                                     -- GET|PUT|LIST|HEAD|DELETE|OTHER
    n_ops                                  -- request count under this path/day/op
    bytes_out, bytes_in                    -- summed byte totals
    n_requesters                           -- DISTINCT COUNT(requester)
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import duckdb


# Parent-of expression identical to find/aggregate_duckdb.py — trailing
# slashes already collapsed by ingest (see find/import_listing._canonicalize).
_PARENT_EXPR = (
    "CASE WHEN position('/' IN {col}) > 0 "
    "THEN regexp_extract({col}, '^(.*)/[^/]+$', 1) "
    "ELSE '' END"
)


def aggregate_access(
    con: "duckdb.DuckDBPyConnection",
    raw_sql: str,
    out_parquet: str,
) -> dict:
    """Aggregate canonical access rows → layer-2a parquet.

    ``raw_sql`` is a parenthesized SELECT exposing the canonical row shape
    (typically from a parser: ``(SELECT * FROM parser_relation)``). Handles
    unlimited row counts out-of-core; intermediate tables spill under the
    connection's memory limit.

    Returns a small stats dict (rows_in / paths_out / days / total_bytes_out).
    """
    # Canonicalize input into a `access_in` table so subsequent SQL can spill.
    con.execute(f"""
        CREATE OR REPLACE TABLE access_in AS
        SELECT
            date_trunc('day', ts)::DATE AS day,
            store, bucket,
            -- Reuse the same `//` collapse policy as size-scan ingest so
            -- access and scan trees line up path-for-path.
            rtrim(regexp_replace(path, '/+', '/', 'g'), '/') AS path,
            op, bytes_out, bytes_in, requester
        FROM {raw_sql}
    """)

    n_rows = con.execute("SELECT COUNT(*) FROM access_in").fetchone()[0]
    if n_rows == 0:
        raise ValueError("no access rows in input")

    # Leaf rollup: per (path, day, op). This is the "level 0" grain.
    con.execute("""
        CREATE OR REPLACE TABLE access_leaf AS
        SELECT
            path, day, op,
            COUNT(*)::BIGINT AS n_ops,
            SUM(bytes_out)::BIGINT AS bytes_out,
            SUM(bytes_in)::BIGINT AS bytes_in,
            COUNT(DISTINCT requester)::BIGINT AS n_requesters
        FROM access_in
        GROUP BY path, day, op
    """)

    # Bottom-up parent synthesis. Group current level's rows by parent path
    # (+ day + op) to produce the next level, until nothing left.
    con.execute("CREATE OR REPLACE TABLE level_cur AS SELECT * FROM access_leaf")
    level = 0
    level_tables: list[str] = []
    parent_expr = _PARENT_EXPR.format(col='path')
    while True:
        next_tbl = f'access_level_{level}'
        con.execute(f"""
            CREATE OR REPLACE TABLE {next_tbl} AS
            SELECT
                {parent_expr} AS path,
                day, op,
                SUM(n_ops)::BIGINT AS n_ops,
                SUM(bytes_out)::BIGINT AS bytes_out,
                SUM(bytes_in)::BIGINT AS bytes_in,
                -- n_requesters at parent level is a bounded overestimate: we
                -- can't recover exact distinct-count across children without
                -- rebuilding from raw. Sum bounds it above; a HyperLogLog
                -- sketch is the fix if this ever needs to be exact.
                SUM(n_requesters)::BIGINT AS n_requesters
            FROM level_cur
            WHERE path != ''
            GROUP BY 1, 2, 3
        """)
        cnt = con.execute(f"SELECT COUNT(*) FROM {next_tbl}").fetchone()[0]
        if cnt == 0:
            break
        level_tables.append(next_tbl)
        con.execute(f"CREATE OR REPLACE TABLE level_cur AS SELECT * FROM {next_tbl}")
        level += 1

    # Union all levels + attach kind/parent/depth for tree mechanics.
    unions = " UNION ALL ".join(
        f"SELECT path, day, op, n_ops, bytes_out, bytes_in, n_requesters FROM {t}"
        for t in [*level_tables, 'access_leaf']
    )
    con.execute(f"""
        CREATE OR REPLACE TABLE access_agg AS
        WITH all_levels AS ({unions})
        SELECT
            CASE WHEN path = '' THEN '.' ELSE path END AS path,
            day, op,
            SUM(n_ops)::BIGINT AS n_ops,
            SUM(bytes_out)::BIGINT AS bytes_out,
            SUM(bytes_in)::BIGINT AS bytes_in,
            SUM(n_requesters)::BIGINT AS n_requesters,
            -- 'dir' for the tree root and any synthesized parent; 'file' for
            -- leaf paths present in access_leaf (i.e. object keys that
            -- actually appeared in the raw log).
            CASE
                WHEN path = '' THEN 'dir'
                WHEN path IN (SELECT DISTINCT path FROM access_leaf) THEN 'file'
                ELSE 'dir'
            END AS kind,
            CASE
                WHEN path = '' THEN ''
                ELSE {_PARENT_EXPR.format(col='CASE WHEN path IS NULL THEN NULL ELSE path END')}
            END AS parent,
            CASE WHEN path = '' THEN 0 ELSE (length(path) - length(replace(path, '/', ''))) + 1 END AS depth
        FROM all_levels
        GROUP BY path, day, op
    """)

    # Root-level normalization: rows with parent='' + path!='' → parent='.'
    # (matches size-scan convention).
    con.execute("""
        UPDATE access_agg SET parent = '.' WHERE parent = '' AND path != '.'
    """)

    con.execute(f"""
        COPY (SELECT path, parent, depth, kind, day, op,
                     n_ops, bytes_out, bytes_in, n_requesters
              FROM access_agg
              ORDER BY depth, path, day, op)
        TO '{out_parquet}' (FORMAT PARQUET)
    """)

    n_paths = con.execute("SELECT COUNT(DISTINCT path) FROM access_agg").fetchone()[0]
    n_days = con.execute("SELECT COUNT(DISTINCT day) FROM access_agg").fetchone()[0]
    total_out = con.execute(
        "SELECT COALESCE(SUM(bytes_out), 0) FROM access_agg WHERE path = '.'"
    ).fetchone()[0]
    return {
        'rows_in': int(n_rows),
        'paths_out': int(n_paths),
        'days': int(n_days),
        'total_bytes_out': int(total_out),
    }
