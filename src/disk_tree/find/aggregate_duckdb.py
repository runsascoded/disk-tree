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


def _aggregate_shared(
    con: "duckdb.DuckDBPyConnection",
    scan_root: str,
) -> pd.DataFrame:
    """The core SQL cascade. Assumes an `inputs` table with the schema above."""

    parent_of_path = _PARENT_EXPR.format(col='path')

    # dirs0 = walk-emitted / synthesized dir rows; each contributes n_desc=1 at its own path.
    con.execute("""
        CREATE OR REPLACE TABLE dirs0 AS
        SELECT path, size, mtime, 1::BIGINT AS n_desc
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

    # `cur` seed: all input rows with n_desc=1. Loop group-by-parent to synthesize
    # each subsequent level's dir rows (bottom-up). Stop when nothing left to promote.
    con.execute("""
        CREATE OR REPLACE TABLE level_cur AS
        SELECT path, size, mtime, 1::BIGINT AS n_desc FROM inputs
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
                   SUM(n_desc)::BIGINT AS n_desc
            FROM level_cur
            WHERE path != ''
            GROUP BY 1
        """)
        cnt = con.execute(f"SELECT COUNT(*) FROM {next_tbl}").fetchone()[0]
        if cnt == 0:
            break
        level_tables.append(next_tbl)
        # promote for next iteration
        con.execute(f"CREATE OR REPLACE TABLE level_cur AS SELECT * FROM {next_tbl}")
        level += 1

    # Union all dir levels + dirs0, group by path, attach n_children.
    if level_tables:
        levels_union = " UNION ALL ".join(
            f"SELECT path, size, mtime, n_desc FROM {t}" for t in level_tables
        )
        con.execute(f"""
            CREATE OR REPLACE TABLE dirs_all AS
            SELECT path, size, mtime, n_desc FROM dirs0
            UNION ALL
            {levels_union}
        """)
    else:
        con.execute("""
            CREATE OR REPLACE TABLE dirs_all AS
            SELECT path, size, mtime, n_desc FROM dirs0
        """)

    parent_of_agg = _PARENT_EXPR.format(col='d.path')
    dirs_agg = con.execute(f"""
        SELECT
            d.path AS path,
            SUM(d.size)::BIGINT AS size,
            MAX(d.mtime)::BIGINT AS mtime,
            SUM(d.n_desc)::BIGINT AS n_desc,
            COALESCE(MAX(nc.n_children), 0)::BIGINT AS n_children,
            'dir' AS kind,
            {parent_of_agg} AS parent
        FROM dirs_all d
        LEFT JOIN n_children_tbl nc ON d.path = nc.path
        GROUP BY d.path
        ORDER BY d.path
    """).df()

    if dirs_agg.empty:
        # Single-file corner case: no dir rows anywhere. Manufacture the root
        # stub in the same shape as pandas `aggregate()` does at find/index.py:76-90.
        dirs_agg = pd.DataFrame([{
            'path': '.', 'size': 0, 'mtime': 0, 'n_desc': 0, 'n_children': 0,
            'kind': 'dir', 'parent': '', 'uri': scan_root,
        }])
    else:
        # Pandas final normalization (find/index.py:164-166):
        # - dir rows with parent='' get parent='.' (except the root itself)
        # - the root dir has path='.', parent=''
        # - uri = scan_root for root, else f'{scan_root}/{path}'
        root_mask = dirs_agg['path'] == ''
        dirs_agg.loc[dirs_agg['parent'] == '', 'parent'] = '.'
        dirs_agg.loc[root_mask, ['path', 'parent']] = ['.', '']
        dirs_agg['uri'] = dirs_agg['path'].apply(lambda p: scan_root if p == '.' else f'{scan_root}/{p}')

    # Files: pulled from inputs with n_desc=1, n_children=0 (per pandas)
    files = con.execute("""
        SELECT path, size, mtime, 'file'::VARCHAR AS kind, parent, uri,
               1::BIGINT AS n_desc,
               0::BIGINT AS n_children
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


def aggregate_listing_to_parquet(
    listing_sql: str,
    bucket: str,
    scheme: str,
    out_parquet: str,
    con: "duckdb.DuckDBPyConnection | None" = None,
    memory_limit: str = '8GB',
    temp_dir: str | None = None,
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
    if temp_dir:
        con.execute(f"SET temp_directory = '{temp_dir}'")

    scan_root = f'{scheme}://{bucket}'
    parent_of_name = _PARENT_EXPR.format(col='name')

    # Build the `inputs` table without a pandas roundtrip: files first, then
    # synthesized dir rows for every unique ancestor path.
    con.execute(f"""
        DROP TABLE IF EXISTS inputs;
        CREATE TABLE inputs AS
        SELECT
            name AS path,
            size_bytes::BIGINT AS size,
            COALESCE(epoch(created), 0)::BIGINT AS mtime,
            'file'::VARCHAR AS kind,
            {parent_of_name} AS parent,
            ('{scan_root}/' || name)::VARCHAR AS uri
        FROM {listing_sql}
        WHERE bucket = '{bucket}'
    """)

    n_files = con.execute("SELECT COUNT(*) FROM inputs").fetchone()[0]
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
    con.execute(f"""
        INSERT INTO inputs
        SELECT
            path,
            0::BIGINT AS size,
            0::BIGINT AS mtime,
            'dir'::VARCHAR AS kind,
            {_PARENT_EXPR.format(col='path')} AS parent,
            CASE WHEN path = '' THEN '{scan_root}' ELSE '{scan_root}/' || path END AS uri
        FROM dir_paths
    """)

    df = _aggregate_shared(con, scan_root)
    df.to_parquet(out_parquet, index=False)

    root = df[df['path'] == '.'].iloc[0] if not df[df['path'] == '.'].empty else None
    return {
        'rows': int(len(df)),
        'files': int(n_files),
        'root_size': int(root['size']) if root is not None else 0,
        'root_n_desc': int(root['n_desc']) if root is not None else 0,
        'root_n_children': int(root['n_children']) if root is not None else 0,
        'root_mtime': int(root['mtime']) if root is not None else 0,
    }
