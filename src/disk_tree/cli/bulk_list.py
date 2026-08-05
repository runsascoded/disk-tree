"""`disk-tree bulk-list` — sharded live listing of a cloud bucket to canonical
listing parquet (layer-1).

Feed the result into ``disk-tree import`` to get canonical per-path scans
(layer-2). Split so each stage stays independently retriable and cacheable:

    # 1. list bucket → shards under out_dir/*.parquet (+ _SUCCESS.json)
    disk-tree bulk-list gcs://marin-us-central1 -o gs://oa-dvx/listing/2026-08-05/marin-us-central1

    # 2. aggregate into a canonical scan
    disk-tree import -e duckdb -l 'gs://oa-dvx/listing/2026-08-05/marin-us-central1/*.parquet' -b marin-us-central1
"""

from __future__ import annotations

from click import Choice, argument, option
from utz import err

from disk_tree.cli.base import cli


@cli.command('bulk-list')
@option('-o', '--out', 'out_dir', required=True, help='Output dir for listing shards (local path or fsspec URL such as `gs://...`)')
@option('-p', '--prefix', default=None, help='Restrict listing to this bucket-relative prefix')
@option('-P', '--procs', default=6, help='Worker processes (default 6). Marin measured 32 vCPU / 24 procs / 10 threads as the sweet spot for GCS.')
@option('-w', '--threads', default=8, help='Threads per worker (default 8)')
@option('-W', '--weights-from', default=None, help='Glob to a prior listing parquet used to bin-pack + range-split hot prefixes')
@option('-x', '--exists', type=Choice(['error', 'clear', 'reuse']), default='error', help='Behavior when --out already has shards: error/clear/reuse')
@argument('uri')
def bulk_list_cmd(
    out_dir: str,
    prefix: str | None,
    procs: int,
    threads: int,
    weights_from: str | None,
    exists: str,
    uri: str,
):
    """Bulk-list `URI` (e.g. `gcs://bucket`) to sharded listing parquet at `--out`."""
    from disk_tree.backends.url import parse_url

    parsed = parse_url(uri)
    if parsed.scheme == 'gcs':
        from disk_tree.find.bulk_gcs import list_gcs_bucket_to_parquet
        total = list_gcs_bucket_to_parquet(
            bucket=parsed.host,
            out_dir=out_dir,
            procs=procs, threads=threads,
            prefix=prefix if prefix is not None else parsed.path.strip('/') or None,
            exists=exists, weights_from=weights_from,
        )
    elif parsed.scheme in ('s3', 'r2'):
        # A.2 continued — the S3-compatible bulk lister lives in a follow-up
        # commit (marin only uses GCS today, so this ships first and s3/r2
        # can pin against the interface once the port lands).
        raise NotImplementedError(
            f"bulk-list for scheme={parsed.scheme!r} isn't wired yet;"
            " GCS is the first backend (see disk_tree.find.bulk_gcs).")
    else:
        raise ValueError(f"bulk-list requires a cloud URI (gcs://, s3://, r2://); got {uri!r}")

    err(f"listed {total:,} objects to {out_dir}")
