"""`disk-tree import` — ingest pre-made object listings (Phase-0 pandas path)."""

import json
from datetime import datetime, timezone

from click import argument, option
from dateutil.parser import parse as parse_dt
from utz import err

from disk_tree.cli.base import cli


@cli.command('import')
@option('-l', '--listing', 'listings', required=True, multiple=True, help='Listing parquet glob(s) — raw / SII / S3-Inventory; repeatable, earlier sources win per bucket')
@option('-b', '--bucket', 'buckets', multiple=True, help='Bucket to import as one scan; repeatable. Default: every distinct bucket in the listings')
@option('-s', '--scheme', default='gcs', help='URI scheme for the scan root (gcs / s3 / r2)')
@option('-t', '--time', 'time_str', default=None, help='Snapshot time (ISO 8601) recorded on each Scan; default: now')
def import_cmd(listings: tuple[str, ...], buckets: tuple[str, ...], scheme: str, time_str: str | None):
    """Import one or more buckets from listing parquet(s) as canonical scans.

    Phase-0 uses in-memory pandas aggregation — fine for laptop-scale inputs
    or subsampled cloud listings. The multi-PB regime waits on out-of-core
    DuckDB aggregation (spec item B).
    """
    import duckdb
    from disk_tree.find.import_listing import import_listing, list_buckets
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
        err(f"importing {bucket}…")
        result = import_listing(listings, bucket=bucket, scheme=scheme, con=con)
        df = result.df
        scan_path = f'{scheme}://{bucket}'
        blob_ref = storage.save(df, scan_path)

        root_rows = df[df['parent'] == '']
        root = root_rows.iloc[0] if not root_rows.empty else None
        scan = Scan(
            path=scan_path,
            time=snap_time,
            blob=blob_ref,
            error_count=None,
            error_paths=None,
            size=int(root['size']) if root is not None else None,
            n_children=int(root['n_children']) if root is not None else None,
            n_desc=int(root['n_desc']) if root is not None else None,
            mtime=int(root['mtime']) if root is not None else None,
        )
        db.session.add(scan)
        db.session.commit()
        err(f"  {scan_path}: {len(df):,} rows @ {snap_time.isoformat()} → {blob_ref}")
