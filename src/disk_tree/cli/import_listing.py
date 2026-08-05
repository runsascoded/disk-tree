"""`disk-tree import` — ingest pre-made object listings.

Two aggregation engines:

- `--engine pandas` (default) — fine for laptop-scale inputs or subsampled
  cloud listings; the whole layer-1 frame + intermediates live in RAM.
- `--engine duckdb` — out-of-core, spills to `--temp-dir` under
  `--memory-limit`. The multi-PB path (spec Item B).

Both produce byte-identical canonical layer-2 output.
"""

import os
import tempfile
from datetime import datetime, timezone

from click import Choice, argument, option
from dateutil.parser import parse as parse_dt
from utz import err

from disk_tree.cli.base import cli


@cli.command('import')
@option('-e', '--engine', type=Choice(['pandas', 'duckdb']), default='pandas', help='Aggregation engine: `pandas` (in-memory; small) or `duckdb` (out-of-core; big)')
@option('-l', '--listing', 'listings', required=True, multiple=True, help='Listing parquet glob(s) — raw / SII / S3-Inventory; repeatable, earlier sources win per bucket')
@option('-b', '--bucket', 'buckets', multiple=True, help='Bucket to import as one scan; repeatable. Default: every distinct bucket in the listings')
@option('-M', '--memory-limit', default='8GB', help='DuckDB memory cap (duckdb engine only). Excess spills to `--temp-dir`.')
@option('-s', '--scheme', default='gcs', help='URI scheme for the scan root (gcs / s3 / r2)')
@option('-T', '--temp-dir', default=None, help='DuckDB spill directory (duckdb engine only). Default: system tmp.')
@option('-t', '--time', 'time_str', default=None, help='Snapshot time (ISO 8601) recorded on each Scan; default: now')
def import_cmd(
    engine: str,
    listings: tuple[str, ...],
    buckets: tuple[str, ...],
    memory_limit: str,
    scheme: str,
    temp_dir: str | None,
    time_str: str | None,
):
    """Import one or more buckets from listing parquet(s) as canonical scans."""
    import duckdb
    from disk_tree.find.import_listing import list_buckets
    from disk_tree.sqla.db import init
    from disk_tree.sqla.model import Scan
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
        scan_path = f'{scheme}://{bucket}'

        if engine == 'pandas':
            from disk_tree.find.import_listing import import_listing
            df = import_listing(listings, bucket=bucket, scheme=scheme, con=con).df
            blob_ref = storage.save(df, scan_path)
            root_size = _root_stat(df, 'size')
            root_n_children = _root_stat(df, 'n_children')
            root_n_desc = _root_stat(df, 'n_desc')
            root_mtime = _root_stat(df, 'mtime')
            n_rows = len(df)
        else:
            from disk_tree.find.aggregate_duckdb import aggregate_listing_to_parquet
            from disk_tree.listing import prepare_listing
            src = prepare_listing(con, listings)
            # Aggregate straight to a parquet in a temp location, then have the storage
            # backend adopt it — mirrors what a `save-from-file` API would do if we had one.
            with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as fh:
                out_parquet = fh.name
            try:
                stats = aggregate_listing_to_parquet(
                    src, bucket=bucket, scheme=scheme, out_parquet=out_parquet,
                    con=con, memory_limit=memory_limit, temp_dir=temp_dir,
                )
                # Storage backend expects a DataFrame today — a follow-up wire could avoid
                # this round-trip. For 588M-row scale this materialization is the last
                # in-memory step before disk; if it's a problem we'll chunk it in the
                # save path (see storage/hybrid.py's chunking pattern).
                import pandas as pd
                df = pd.read_parquet(out_parquet)
                blob_ref = storage.save(df, scan_path)
                root_size = stats['root_size']
                root_n_children = stats['root_n_children']
                root_n_desc = stats['root_n_desc']
                root_mtime = stats['root_mtime']
                n_rows = stats['rows']
            finally:
                if os.path.exists(out_parquet):
                    os.remove(out_parquet)

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


def _root_stat(df, col: str) -> int | None:
    root = df[df['parent'] == '']
    if root.empty:
        return None
    v = root.iloc[0][col]
    return int(v) if v is not None else None
