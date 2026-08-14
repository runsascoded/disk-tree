"""`dt access` — canonicalize + aggregate access logs; hot-prefix report.

Three subcommands mirror the size-scan flow:

- ``dt access import``  — layer-1a: raw provider logs → canonical parquet
- ``dt access agg``     — layer-2a: canonical rows → per-path per-day per-op tree
- ``dt access top``     — hot-prefix table (the auto-report primitive)
"""

from __future__ import annotations

from click import Choice, argument, group, option
from utz import err

from disk_tree.cli.base import cli


@cli.group('access')
def access():
    """Ingest + aggregate + query access logs (layer-1a / 2a)."""


@access.command('import')
@option('-o', '--out', 'out_parquet', required=True, help='Output canonical parquet path')
@option('-s', '--store', type=Choice(['gcs', 's3', 'r2']), default='gcs', help='Log provider')
@option('-M', '--memory-limit', default='4GB')
@option('-T', '--temp-dir', default=None)
@argument('input_glob')
def access_import_cmd(out_parquet: str, store: str, memory_limit: str, temp_dir: str | None, input_glob: str):
    """Parse raw provider logs at INPUT_GLOB → canonical layer-1a parquet at --out."""
    import duckdb
    from disk_tree.access.parsers import parser_for

    con = duckdb.connect()
    con.execute(f"SET memory_limit = '{memory_limit}'")
    # Log rows carry no meaningful order (agg sorts downstream); preserving
    # insertion order makes the parquet writer buffer whole row groups in RAM
    # and OOMs on multi-10GB imports.
    con.execute("SET preserve_insertion_order = false")
    if temp_dir:
        con.execute(f"SET temp_directory = '{temp_dir}'")

    parse = parser_for(store)
    rel = parse(input_glob, store=store, con=con)
    rel.write_parquet(out_parquet)
    n = con.execute(f"SELECT COUNT(*) FROM read_parquet('{out_parquet}')").fetchone()[0]
    err(f"wrote {n:,} canonical access rows → {out_parquet}")


@access.command('agg')
@option('-o', '--out', 'out_parquet', required=True, help='Output layer-2a parquet path')
@option('-M', '--memory-limit', default='4GB')
@option('-T', '--temp-dir', default=None)
@argument('raw_glob')
def access_agg_cmd(out_parquet: str, memory_limit: str, temp_dir: str | None, raw_glob: str):
    """Aggregate canonical rows at RAW_GLOB → layer-2a per-path per-day per-op tree at --out."""
    import duckdb
    from disk_tree.access.aggregate import aggregate_access

    con = duckdb.connect()
    con.execute(f"SET memory_limit = '{memory_limit}'")
    # Log rows carry no meaningful order (agg sorts downstream); preserving
    # insertion order makes the parquet writer buffer whole row groups in RAM
    # and OOMs on multi-10GB imports.
    con.execute("SET preserve_insertion_order = false")
    if temp_dir:
        con.execute(f"SET temp_directory = '{temp_dir}'")

    stats = aggregate_access(con, f"(SELECT * FROM read_parquet('{raw_glob}'))", out_parquet)
    err(
        f"aggregated {stats['rows_in']:,} rows over {stats['days']} day(s) → "
        f"{stats['paths_out']:,} paths, {stats['total_bytes_out']:,} bytes_out at root "
        f"→ {out_parquet}"
    )


@access.command('top')
@option('-d', '--depth', default=1, help='Report at this tree depth (1 = top-level prefixes)')
@option('-n', '--limit', default=20, help='Show top N rows')
@option('-p', '--op', 'op_filter', default=None, help='Filter by op (GET / PUT / LIST / …)')
@option('-b', '--by', 'sort_by', type=Choice(['ops', 'bytes']), default='bytes', help='Sort by n_ops or bytes_out')
@argument('agg_glob')
def access_top_cmd(depth: int, limit: int, op_filter: str | None, sort_by: str, agg_glob: str):
    """Hot-prefix report from layer-2a AGG_GLOB — the auto-report primitive."""
    import duckdb

    con = duckdb.connect()
    con.register_filesystem  # ensure fsspec is loaded if needed
    order_col = 'n_ops' if sort_by == 'ops' else 'bytes_out'
    where = ["depth = ?"]
    params: list = [depth]
    if op_filter:
        where.append("op = ?")
        params.append(op_filter.upper())
    where_sql = ' AND '.join(where)

    rows = con.execute(f"""
        SELECT path, op,
               SUM(n_ops)::BIGINT AS n_ops,
               SUM(bytes_out)::BIGINT AS bytes_out,
               SUM(bytes_in)::BIGINT AS bytes_in,
               MAX(n_requesters)::BIGINT AS n_requesters
        FROM read_parquet('{agg_glob}')
        WHERE {where_sql}
        GROUP BY path, op
        ORDER BY {order_col} DESC
        LIMIT ?
    """, [*params, limit]).fetchall()

    if not rows:
        print(f"(no rows at depth={depth}"
              + (f", op={op_filter}" if op_filter else "") + ")")
        return

    w_path = max(4, max(len(r[0]) for r in rows))
    hdr = f"{'path':{w_path}}  {'op':6}  {'n_ops':>12}  {'bytes_out':>15}  {'bytes_in':>15}  {'n_req':>8}"
    print(hdr)
    print('-' * len(hdr))
    for path, op, n_ops, bytes_out, bytes_in, n_req in rows:
        print(f"{path:{w_path}}  {op:6}  {n_ops:>12,}  {bytes_out:>15,}  {bytes_in:>15,}  {n_req:>8,}")
