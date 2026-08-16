"""`disk-tree import` — ingest pre-made object listings.

Three aggregation engines:

- `--engine pandas` (default) — fine for laptop-scale inputs or subsampled
  cloud listings; the whole layer-1 frame + intermediates live in RAM.
- `--engine duckdb` — out-of-core, spills to `--temp-dir` under
  `--memory-limit`. Handles unsorted / mixed-schema listings.
- `--engine stream` — O(depth) streaming rollup over sorted raw listings
  (bulk-list output); KBs of working state, one bounded final sort. The
  100M+-row path (spec: streaming-aggregation.md).

All produce byte-identical canonical layer-2 output.
"""

import os
import tempfile
from datetime import datetime, timezone

from click import Choice, argument, option
from dateutil.parser import parse as parse_dt
from utz import err

from disk_tree.cli.base import cli


@cli.command('import')
@option('-e', '--engine', type=Choice(['pandas', 'duckdb', 'stream']), default='pandas', help='Aggregation engine: `pandas` (in-memory; small), `duckdb` (out-of-core; big), or `stream` (O(depth) over sorted listings; biggest)')
@option('-l', '--listing', 'listings', required=True, multiple=True, help='Listing parquet glob(s) — raw / SII / S3-Inventory; repeatable, earlier sources win per bucket')
@option('-b', '--bucket', 'buckets', multiple=True, help='Bucket to import as one scan; repeatable. Default: every distinct bucket in the listings')
@option('-j', '--jobs', default=1, help='Stream engine only: partition the keyspace into N ranges streamed by parallel worker processes (0 = all cores). Output is byte-identical for any value.')
@option('-M', '--memory-limit', default='8GB', help='DuckDB memory cap (duckdb engine only). Excess spills to `--temp-dir`.')
@option('-m', '--mean-mtime', is_flag=True, help='Emit `mtime_mean` (size-weighted mean mtime over descendant files) per path')
@option('-p', '--pivot-sum', 'pivot_sums', multiple=True, help='Emit per-value byte-sum columns `sum_<col>_<v>` for this layer-1 column (e.g. storage_class_id); repeatable')
@option('-s', '--scheme', default='gcs', help='URI scheme for the scan root (gcs / s3 / r2)')
@option('-T', '--temp-dir', default=None, help='DuckDB spill directory (duckdb engine only; the stream engine is sort-free). Default: fresh per-invocation temp dir (safe under concurrent imports).')
@option('-t', '--time', 'time_str', default=None, help='Snapshot time (ISO 8601) recorded on each Scan; default: now')
@option('-x', '--max-temp-size', default=None, help="DuckDB `max_temp_directory_size` (duckdb engine only; e.g. `500GiB`). Default: DuckDB's auto-cap = free disk at launch, a stale snapshot under concurrent writers.")
def import_cmd(
    engine: str,
    listings: tuple[str, ...],
    buckets: tuple[str, ...],
    jobs: int,
    memory_limit: str,
    mean_mtime: bool,
    pivot_sums: tuple[str, ...],
    scheme: str,
    temp_dir: str | None,
    time_str: str | None,
    max_temp_size: str | None,
):
    """Import one or more buckets from listing parquet(s) as canonical scans."""
    import duckdb
    from disk_tree.find.import_listing import list_buckets
    from disk_tree.sqla.db import init
    from disk_tree.storage import get_backend

    db = init()
    db.create_all()

    snap_time = parse_dt(time_str) if time_str else datetime.now().astimezone()
    if snap_time.tzinfo is None:
        snap_time = snap_time.replace(tzinfo=timezone.utc)

    con = duckdb.connect()
    if not buckets:
        buckets = tuple(list_buckets(listings, con=con))
        err(f"discovered {len(buckets)} bucket(s): {', '.join(buckets)}")

    storage = get_backend()
    for bucket in buckets:
        err(f"importing {bucket} (engine={engine})…")
        import_bucket(
            db=db, storage=storage, con=con,
            engine=engine, listings=listings, bucket=bucket, scheme=scheme,
            snap_time=snap_time, memory_limit=memory_limit, temp_dir=temp_dir,
            max_temp_size=max_temp_size, jobs=jobs,
            pivot_sums=pivot_sums, mean_mtime=mean_mtime,
        )


def import_bucket(
    db,
    storage,
    con,
    engine: str,
    listings: tuple[str, ...],
    bucket: str,
    scheme: str,
    snap_time: datetime,
    memory_limit: str = '8GB',
    temp_dir: str | None = None,
    max_temp_size: str | None = None,
    jobs: int = 1,
    pivot_sums: tuple[str, ...] = (),
    mean_mtime: bool = False,
    replace=None,
):
    """Aggregate one bucket's listing → blob + Scan row.

    `replace`: an existing Scan row to update in place (same path+time)
    instead of inserting a new one — used by `disk-tree pull --force`.
    Returns the Scan.
    """
    from disk_tree.sqla.model import Scan

    scan_path = f'{scheme}://{bucket}'

    if engine == 'pandas':
        from disk_tree.find.import_listing import import_listing
        df = import_listing(
            listings, bucket=bucket, scheme=scheme, con=con,
            pivot_sums=pivot_sums, mean_mtime=mean_mtime,
        ).df
        blob_ref = storage.save(df, scan_path)
        root_size = _root_stat(df, 'size')
        root_n_children = _root_stat(df, 'n_children')
        root_n_desc = _root_stat(df, 'n_desc')
        root_mtime = _root_stat(df, 'mtime')
        n_rows = len(df)
    else:
        # Aggregate straight to a parquet in a temp location, then have the storage
        # backend adopt it — mirrors what a `save-from-file` API would do if we had one.
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as fh:
            out_parquet = fh.name
        try:
            if engine == 'duckdb':
                from disk_tree.find.aggregate_duckdb import aggregate_listing_to_parquet
                from disk_tree.listing import prepare_listing
                src = prepare_listing(con, listings)
                stats = aggregate_listing_to_parquet(
                    src, bucket=bucket, scheme=scheme, out_parquet=out_parquet,
                    con=con, memory_limit=memory_limit, temp_dir=temp_dir,
                    max_temp_size=max_temp_size,
                    pivot_sums=pivot_sums, mean_mtime=mean_mtime,
                )
            else:  # stream
                from disk_tree.find.aggregate_stream import aggregate_stream
                stats = aggregate_stream(
                    listings, bucket=bucket, scheme=scheme, out_parquet=out_parquet,
                    con=con, memory_limit=memory_limit, temp_dir=temp_dir,
                    max_temp_size=max_temp_size, jobs=jobs,
                    pivot_sums=pivot_sums, mean_mtime=mean_mtime,
                )
            # Hand the file itself to the storage backend — reading a
            # 92.7M-object bucket's layer-2 (185M rows) back into pandas
            # here OOM-killed a 64GB node after the aggregation had
            # already succeeded.
            blob_ref = storage.adopt_parquet(out_parquet, scan_path)
            root_size = stats['root_size']
            root_n_children = stats['root_n_children']
            root_n_desc = stats['root_n_desc']
            root_mtime = stats['root_mtime']
            n_rows = stats['rows']
        finally:
            if os.path.exists(out_parquet):
                os.remove(out_parquet)

    if replace is not None:
        scan = replace
        scan.blob = blob_ref
        scan.size = root_size
        scan.n_children = root_n_children
        scan.n_desc = root_n_desc
        scan.mtime = root_mtime
    else:
        scan = Scan(
            path=scan_path,
            time=snap_time,
            blob=blob_ref,
            error_count=None,
            error_paths=None,
            size=root_size,
            n_children=root_n_children,
            n_desc=root_n_desc,
            mtime=root_mtime,
        )
        db.session.add(scan)
    db.session.commit()
    err(f"  {scan_path}: {n_rows:,} rows @ {snap_time.isoformat()} → {blob_ref}")
    return scan


def _root_stat(df, col: str) -> int | None:
    root = df[df['parent'] == '']
    if root.empty:
        return None
    v = root.iloc[0][col]
    return int(v) if v is not None else None
