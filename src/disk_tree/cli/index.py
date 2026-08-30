import os
from contextlib import nullcontext
from os import getcwd

from click import argument, option

from disk_tree import time
from disk_tree.cli.base import cli
from disk_tree.sqla.db import init
from humanize import naturalsize
from utz import err, iec


@cli.command
@option('-C', '--no-cache-read', is_flag=True)
@option('-D', '--no-diff', is_flag=True, help="Skip building the diff index against the path's previous scan")
@option('-g', '--gc', is_flag=True)
@option('-m', '--mean-mtime', is_flag=True, help='Emit `mtime_mean` (size-weighted mean mtime over descendants) per path')
@option('-M', '--measure-memory', is_flag=True)
@option('-s', '--sudo', is_flag=True, help='Run `find` as sudo')
@option('-x', '--extents', is_flag=True, help='Also map physical extents → per-dir reclaimable bytes (APFS clones/hardlinks; writes a .reclaim sidecar). macOS, local scans only; exact when the scan root contains the sharing sources (home/full scan)')
@argument('url', required=False)
def index(
    no_cache_read: bool,
    no_diff: bool,
    gc: bool,
    mean_mtime: bool,
    measure_memory: bool,
    sudo: bool,
    extents: bool,
    url: str | None,
):
    """Index a directory, persisting data to a SQLite DB."""
    db = init()
    from disk_tree.sqla.model import Scan
    db.create_all()
    url = url or getcwd()
    url = url.rstrip('/') or '/'
    if measure_memory:
        from utz.mem import Tracker
        mem = Tracker()
        ctx = mem
    else:
        mem = None
        ctx = nullcontext()

    with ctx, time("scan"):
        if no_cache_read:
            scan, df = Scan.create(url, gc=gc, sudo=sudo, mean_mtime=mean_mtime)
        else:
            scan, df = Scan.load_or_create(url, gc=gc, sudo=sudo, mean_mtime=mean_mtime)

    elapsed = time['scan']
    if not no_diff and not gc:
        # Overnight prep: the "what changed since last time" view is a slice,
        # not a walk, by the time anyone asks. (`--gc` deleted the previous
        # scan, so there's nothing to diff against.)
        from disk_tree.cli.diff_index import build_previous
        build_previous(scan.id)
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
        summary += f", {scan.error_count} permission errors"
    print(summary)
    from os.path import isabs, join
    from disk_tree.config import SCANS_DIR
    blob_path = scan.blob if isabs(scan.blob) else join(SCANS_DIR, scan.blob)
    stat = os.stat(blob_path)
    print(f"Scan cached path: {blob_path} ({iec(stat.st_size)})")
    if scan.error_count:
        import json
        error_paths = json.loads(scan.error_paths) if scan.error_paths else []
        if error_paths:
            print(f"\nPermission errors (showing first {len(error_paths)}):")
            for p in error_paths[:10]:
                print(f"  {p}")
            if len(error_paths) > 10:
                print(f"  ... and {len(error_paths) - 10} more")
        print(f"\nTip: Run with --sudo for full access: disk-tree index --sudo {url}")

    if extents:
        _build_reclaim_sidecar(url, blob_path)


def _build_reclaim_sidecar(url: str, blob_path: str):
    """Map physical extents of the just-scanned tree → per-dir reclaimable bytes.

    Walks the live filesystem (not the blob), so it only applies to a local path
    that still exists. The blob's paths are relative to `url`, and
    `reclaimable_by_dir` returns the same scheme, so the sidecar joins directly.
    """
    import sys
    from os.path import isdir
    from disk_tree.extents import SUPPORTED, reclaimable_by_dir, write_reclaim_sidecar

    if not SUPPORTED:
        err(f"--extents is macOS-only (got {sys.platform}); skipping")
        return
    if url.startswith(('s3://', 'gcs://', 'r2://', 'ssh://')) or not isdir(url):
        err(f"--extents needs a local directory that still exists; skipping ({url})")
        return
    with time("extents"):
        recl, n_err = reclaimable_by_dir(url)
    out = write_reclaim_sidecar(blob_path, recl)
    root = recl.get('.', 0)
    err(f"extents: reclaim sidecar → {out} ({time['extents']:.1f}s"
        + (f", {n_err} unreadable" if n_err else "") + ")")
    from humanize import naturalsize as _ns
    top = sorted(((k, v) for k, v in recl.items() if k != '.'), key=lambda kv: -kv[1])[:8]
    print(f"Reclaimable (rm -rf frees): {_ns(root, binary=True, format='%.3g')} total")
    for k, v in top:
        print(f"  {_ns(v, binary=True, format='%.3g'):>9}  {k}")
