import os
from contextlib import nullcontext
from os import getcwd

from click import argument, option

from disk_tree import time
from disk_tree.cli.base import cli
from disk_tree.sqla.db import init
from humanize import naturalsize
from utz import err, iec

_CLOUD = ('s3://', 'gcs://', 'r2://')
LOW_SPACE_VAR = 'DISK_TREE_LOW_SPACE_BYTES'
REMOTE_TARGET_VAR = 'DISK_TREE_REMOTE_SCAN_TARGET'
DEFAULT_LOW_SPACE_BYTES = 5 * 2**30


def _check_local_space(auto_remote: bool) -> None:
    """Warn — or, with `--auto-remote`, redirect — when the local write target is
    low on space: the crisis a remote target exists for (spec
    `remote-scan-targets.md`). A URL target is never checked; only the
    persistent blob moves.
    """
    from shutil import disk_usage
    from disk_tree import blobfs, config as _config

    wd = _config.scan_write_dir()
    if blobfs.is_url(wd):
        return
    probe = wd
    while not os.path.exists(probe):  # the dir may not exist yet; its volume does
        probe = os.path.dirname(probe)
    free = disk_usage(probe).free
    low = int(os.environ.get(LOW_SPACE_VAR, DEFAULT_LOW_SPACE_BYTES))
    if free >= low:
        return
    remote = os.environ.get(REMOTE_TARGET_VAR)
    if auto_remote and remote:
        target = _config.set_write_target(remote)
        err(f"low space: {iec(free)} free on {wd} (< {iec(low)}); --auto-remote: writing blobs to {target}")
        return
    hint = f"--to {remote}" if remote else f"--to r2://<bucket>/<prefix> (or set {REMOTE_TARGET_VAR})"
    tail = " — pass -R/--auto-remote to redirect automatically" if remote else ""
    err(f"warning: only {iec(free)} free on {wd} (< {iec(low)}); consider {hint}{tail}")


@cli.command
@option('-C', '--no-cache-read', is_flag=True)
@option('-g', '--gc', is_flag=True)
@option('-m', '--mean-mtime', is_flag=True, help='Emit `mtime_mean` (size-weighted mean mtime over descendants) per path')
@option('-M', '--measure-memory', is_flag=True)
@option('-q', '--no-progress', is_flag=True, help='Suppress the tqdm scan progress bar (for scheduled/redirected runs — keeps logs small)')
@option('-R', '--auto-remote', is_flag=True, help=f'If the local write target is low on space (< ${LOW_SPACE_VAR}, default 5 GiB) and ${REMOTE_TARGET_VAR} is set, write the blob there instead (default: warn and suggest `--to`)')
@option('-s', '--sudo', is_flag=True, help='Run `find` as sudo')
@option('-t', '--to', default=None, help='Write this scan\'s blob to a local dir or an fsspec URL (`r2://bucket/prefix`, `s3://…`, `gs://…`) instead of the configured write dir; it joins the search path for this run')
@argument('url', required=False)
def index(
    no_cache_read: bool,
    gc: bool,
    mean_mtime: bool,
    measure_memory: bool,
    no_progress: bool,
    auto_remote: bool,
    sudo: bool,
    to: str | None,
    url: str | None,
):
    """Index a directory, persisting data to a SQLite DB."""
    db = init()
    from disk_tree.sqla.model import Scan
    db.create_all()
    url = url or getcwd()
    url = url.rstrip('/') or '/'
    if to:
        from disk_tree import config as _config
        target = _config.set_write_target(to)
        err(f"--to: writing blobs to {target}")
    elif not url.startswith(_CLOUD):
        _check_local_space(auto_remote)
    # `load_or_create` returns any existing scan unconditionally (no freshness
    # check) and can't tell a sudo scan from a plain one — so without this,
    # `index --sudo` silently re-serves a cached *non*-sudo scan and never
    # elevates. Asking for sudo means you want a fresh, privileged walk.
    if sudo and not no_cache_read:
        err("--sudo forces a fresh scan (-C)")
        no_cache_read = True
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
            scan, df = Scan.create(url, gc=gc, sudo=sudo, mean_mtime=mean_mtime, progress=not no_progress)
        else:
            scan, df = Scan.load_or_create(url, gc=gc, sudo=sudo, mean_mtime=mean_mtime, progress=not no_progress)

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
        summary += f", {scan.error_count} permission errors"
    print(summary)
    # Blobs may live on any read dir (external volume incl.), so resolve via the
    # search path — not a naive join with the *write* dir, which stats a path
    # that need not exist (e.g. blob on the boot disk, write target on X6).
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
            print(f"\nPermission errors (showing first {len(error_paths)}):")
            for p in error_paths[:10]:
                print(f"  {p}")
            if len(error_paths) > 10:
                print(f"  ... and {len(error_paths) - 10} more")
        print(f"\nTip: Run with --sudo for full access: disk-tree index --sudo {url}")
