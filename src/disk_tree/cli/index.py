from contextlib import nullcontext

from click import UsageError, argument, option

from disk_tree import time
from disk_tree.cli.base import cli
from disk_tree.sqla.db import init
from humanize import naturalsize
from utz import err, iec


@cli.command
@option('-C', '--no-cache-read', is_flag=True)
@option('-g', '--gc', is_flag=True)
@option('-m', '--mean-mtime', is_flag=True, help='Emit `mtime_mean` (size-weighted mean mtime over descendants) per path')
@option('-M', '--measure-memory', is_flag=True)
@option('-q', '--no-progress', is_flag=True, help='Suppress the tqdm scan progress bar (for scheduled/redirected runs — keeps logs small)')
@option('-t', '--to', default=None, help='Write this scan\'s blob to a local dir or an fsspec URL (`r2://bucket/prefix`, `s3://…`, `gs://…`) instead of the configured write dir; it joins the search path for this run')
@argument('url')
def index(
    no_cache_read: bool,
    gc: bool,
    mean_mtime: bool,
    measure_memory: bool,
    no_progress: bool,
    to: str | None,
    url: str,
):
    """Index an `s3://` or `r2://` URL, persisting data to a SQLite DB."""
    from disk_tree.backends import UnsupportedBackend, backend_for
    url = url.rstrip('/')
    backend = backend_for(url)
    if isinstance(backend, UnsupportedBackend):
        raise UsageError(backend.refusal())
    db = init()
    from disk_tree.sqla.model import Scan
    db.create_all()
    if to:
        from disk_tree import config as _config
        target = _config.set_write_target(to)
        err(f"--to: writing blobs to {target}")
    if measure_memory:
        try:
            from utz.mem import Tracker
        except ImportError as e:  # utz.mem needs memray (the `mem` dependency group)
            raise SystemExit(f"--measure-memory needs the `mem` dependency group (`uv sync --group mem`): {e}")
        mem = Tracker()
        ctx = mem
    else:
        mem = None
        ctx = nullcontext()

    with ctx, time("scan"):
        if no_cache_read:
            scan, df = Scan.create(url, gc=gc, mean_mtime=mean_mtime, progress=not no_progress)
        else:
            scan, df = Scan.load_or_create(url, gc=gc, mean_mtime=mean_mtime, progress=not no_progress)

    elapsed = time['scan']
    # Find root row: try 'path == "."', fallback to 'parent == ""'
    root_rows = df[df['path'] == '.']
    if root_rows.empty:
        root_rows = df[df['parent'] == '']
    res = root_rows.iloc[0]
    n_desc = res.n_desc
    size = res['size']
    speed = n_desc / elapsed

    if mem:
        peak_mem = mem.peak_mem
        err(f"Peak memory use: {peak_mem:,} ({naturalsize(peak_mem, binary=True, format='%.3g')})")

    print("Timings:")
    for k, v in time.fmt().items():
        print(f"  {k}: {v}s")
    summary = f"{n_desc:,} descendents ({elapsed:.3g}s, {round(speed):,d}/s), {naturalsize(size, binary=True, format='%.3g')}"
    if scan.error_count:
        summary += f", {scan.error_count} listing errors"
    print(summary)
    # Blobs may live on any read dir, so resolve via the search path — not a
    # naive join with the *write* dir, which stats a path that need not exist.
    from disk_tree import blobfs
    from disk_tree.resolve import resolve_blob
    blob_path = resolve_blob(scan.blob)
    if blobfs.exists(blob_path):
        print(f"Scan cached path: {blob_path} ({iec(blobfs.size(blob_path))})")
    else:
        print(f"Scan blob: {scan.blob}")
    if to and blobfs.is_url(blob_path):
        # A remote blob's metadata travels with it.
        from disk_tree.scan_manifest import write_scan_manifest
        print(f"Scan manifest: {write_scan_manifest(scan, blob_path)}")
        # Precompute the footer as a `.groups.json` sidecar so the serverless
        # reader (`site/functions/_lib/index.ts`) plans range reads without a cold thrift-footer parse.
        from disk_tree.find.groups import write_groups_sidecar
        gs = write_groups_sidecar(blob_path)
        if gs:
            print(f"Scan groups: {gs.path} ({gs.n_groups} groups, {iec(gs.n_bytes)})")
    if scan.error_count:
        import json
        error_paths = json.loads(scan.error_paths) if scan.error_paths else []
        if error_paths:
            print(f"\nListing errors (showing first {len(error_paths)}):")
            for p in error_paths[:10]:
                print(f"  {p}")
            if len(error_paths) > 10:
                print(f"  ... and {len(error_paths) - 10} more")
