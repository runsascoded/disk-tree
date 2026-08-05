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
@option('-E', '--endpoint-url', default=None, help='S3-compatible endpoint URL (required for r2://; also usable for MinIO / non-AWS S3)')
@option('-o', '--out', 'out_dir', required=True, help='Output dir for listing shards (local path or fsspec URL such as `gs://...`)')
@option('-p', '--prefix', default=None, help='Restrict listing to this bucket-relative prefix')
@option('-P', '--procs', default=6, help='Worker processes (default 6). Marin measured 32 vCPU / 24 procs / 10 threads as the sweet spot for GCS.')
@option('-r', '--region', default=None, help='AWS region name (S3 backend only)')
@option('-w', '--threads', default=8, help='Threads per worker (default 8)')
@option('-W', '--weights-from', default=None, help='Glob to a prior listing parquet used to bin-pack + range-split hot prefixes')
@option('-x', '--exists', type=Choice(['error', 'clear', 'reuse']), default='error', help='Behavior when --out already has shards: error/clear/reuse')
@argument('uri')
def bulk_list_cmd(
    endpoint_url: str | None,
    out_dir: str,
    prefix: str | None,
    procs: int,
    region: str | None,
    threads: int,
    weights_from: str | None,
    exists: str,
    uri: str,
):
    """Bulk-list `URI` (e.g. `gcs://bucket`) to sharded listing parquet at `--out`."""
    from disk_tree.backends.url import parse_url

    parsed = parse_url(uri)
    eff_prefix = prefix if prefix is not None else (parsed.path.strip('/') or None)
    if parsed.scheme == 'gcs':
        from disk_tree.find.bulk_gcs import list_gcs_bucket_to_parquet
        total = list_gcs_bucket_to_parquet(
            bucket=parsed.host,
            out_dir=out_dir,
            procs=procs, threads=threads,
            prefix=eff_prefix,
            exists=exists, weights_from=weights_from,
        )
    elif parsed.scheme in ('s3', 'r2'):
        if parsed.scheme == 'r2' and not endpoint_url:
            raise ValueError("r2:// requires --endpoint-url (Cloudflare R2's S3-compatible endpoint)")
        from disk_tree.find.bulk_s3 import list_s3_bucket_to_parquet
        total = list_s3_bucket_to_parquet(
            bucket=parsed.host,
            out_dir=out_dir,
            procs=procs, threads=threads,
            prefix=eff_prefix,
            exists=exists, weights_from=weights_from,
            endpoint_url=endpoint_url,
            region_name=region,
            scheme=parsed.scheme,
        )
    else:
        raise ValueError(f"bulk-list requires a cloud URI (gcs://, s3://, r2://); got {uri!r}")

    err(f"listed {total:,} objects to {out_dir}")
