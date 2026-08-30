import json
from dataclasses import asdict
from sys import stdout

from click import argument, group, option
from utz import err, Encoder

from disk_tree.cli.base import cli
from disk_tree.sqla import init, Scan


@cli.group()
def scans():
    """Inspect and manage scans."""
    pass


@scans.command('list')
def scans_list():
    """List all scans (JSON, one per line)."""
    db = init()
    all_scans = db.session.query(Scan).all()
    for scan in all_scans:
        json.dump(asdict(scan), stdout, cls=Encoder)
        print()


@scans.command('move')
@option('-f', '--from', 'src_dir', default=None, help='Source dir (default: every other dir on the read path)')
@option('-L', '--no-keep-latest', is_flag=True, help='Also move the newest scan of each path (breaks browsing while the volume is out)')
@option('-n', '--dry-run', is_flag=True, help='List what would move, then stop')
@argument('dest', required=False)
def scans_move(src_dir: str | None, no_keep_latest: bool, dry_run: bool, dest: str | None):
    """Move scan blobs to DEST (default: the current write dir).

    Blobs are referenced by basename, so relocating them needs no DB rewrite —
    `scan_read_dirs()` finds them wherever they land.

    The newest scan of each path stays put by default: an external volume is
    removable, and the common case (browse the latest scan) should not depend on
    it being plugged in. History, which is the bulk, goes to the volume.
    """
    from os import listdir, makedirs
    from os.path import basename, exists, getsize, isdir, join
    from shutil import move as mv

    from disk_tree import config

    dest = dest or config.scan_write_dir()
    if not config._volume_mounted(dest):
        raise SystemExit(f'{dest} is on an unmounted volume')
    sources = [src_dir] if src_dir else [d for d in config.scan_read_dirs() if d != dest]
    sources = [d for d in sources if isdir(d)]
    if not sources:
        raise SystemExit(f'no source dirs to move from (dest={dest})')

    keep = set()
    if not no_keep_latest:
        db = init()
        latest = {}
        for scan in db.session.query(Scan).filter(Scan.blob.isnot(None)):
            cur = latest.get(scan.path)
            if cur is None or scan.time > cur.time:
                latest[scan.path] = scan
        # A hybrid scan's root blob only points at its chunks, so keeping the
        # root alone would still leave the latest scan unbrowsable with the
        # volume out. Keep the whole closure.
        from disk_tree.diff import _chunk_map, resolve_blob
        keep, queue = set(), [basename(s.blob) for s in latest.values()]
        while queue:
            ref = queue.pop()
            if ref in keep:
                continue
            keep.add(ref)
            try:
                chunks = _chunk_map(resolve_blob(ref))
            except (OSError, ValueError):
                continue
            if chunks:
                queue.extend(basename(c) for c in chunks.values())
        # A kept blob's sidecars (vocab/reclaim) must stay beside it, not be
        # left behind on the source volume.
        keep |= {f'{n[:-len(".parquet")]}{sfx}'
                 for n in list(keep) for sfx in ('.vocab.parquet', '.reclaim.parquet')}
        err(f'keeping {len(keep)} files in place ({len(latest)} newest scans + chunks + sidecars)')

    # Sidecars are not scans, so they are never counted as "blobs", but they move
    # with (or stay with) the blob they annotate — handled by the same keep set.
    moves = [
        (join(d, name), join(dest, name))
        for d in sources
        for name in sorted(listdir(d))
        if name.endswith('.parquet') and name not in keep and not name.startswith('._')
    ]
    total = sum(getsize(s) for s, _ in moves if exists(s))
    err(f'{len(moves)} blobs, {total / 2**30:.2f} GiB: {", ".join(sources)} → {dest}')
    if dry_run:
        for s, _ in moves:
            print(s)
        return

    makedirs(dest, exist_ok=True)
    moved = 0
    for src, dst in moves:
        if exists(dst):
            err(f'skip (exists at dest): {basename(src)}')
            continue
        mv(src, dst)
        moved += 1
    err(f'moved {moved} blobs to {dest}')


@scans.command('dirs')
def scans_dirs():
    """Show where blobs are written and searched for."""
    from os.path import isdir, join
    from glob import glob

    from disk_tree import config

    print(f'write: {config.SCANS_DIR}')
    for d in config.scan_read_dirs():
        n = len(glob(join(d, '*.parquet'))) if isdir(d) else 0
        mark = '*' if d == config.SCANS_DIR else ' '
        state = f'{n} blobs' if isdir(d) else 'absent'
        print(f'  {mark} {d}  ({state})')


@scans.command('chunks')
@argument('path')
@option('-a', '--all', 'show_all', is_flag=True, help="Show all scans for path, not just most recent")
def scans_chunks(path: str, show_all: bool):
    """Show chunk structure for a scan.

    PATH can be a scan path (e.g. /Users/ryan) or a blob ref (full parquet path).
    """
    from os.path import isfile
    from disk_tree.storage import get_backend

    backend = get_backend()
    if backend.name != 'hybrid':
        err(f"Chunks only available for hybrid backend (current: {backend.name})")
        return

    # Check if path is a blob ref (file path) or scan path
    if isfile(path):
        blob_ref = path
        stats = backend.get_chunk_stats(blob_ref)
        print(json.dumps(stats, indent=2, default=str))
    else:
        # Look up scan(s) by path
        db = init()
        query = db.session.query(Scan).filter(Scan.path == path).order_by(Scan.time.desc())
        if not show_all:
            query = query.limit(1)
        matching = query.all()

        if not matching:
            err(f"No scans found for path: {path}")
            return

        for scan in matching:
            if len(matching) > 1:
                print(f"=== Scan {scan.id} ({scan.time}) ===")
            stats = backend.get_chunk_stats(scan.blob)
            print(json.dumps(stats, indent=2, default=str))
            if len(matching) > 1:
                print()


@scans.command('info')
@argument('path')
def scans_info(path: str):
    """Show detailed info for a scan path (most recent scan)."""
    from os.path import isfile
    import pandas as pd

    db = init()
    scan = db.session.query(Scan).filter(Scan.path == path).order_by(Scan.time.desc()).first()

    if not scan:
        err(f"No scans found for path: {path}")
        return

    print(f"Scan ID:      {scan.id}")
    print(f"Path:         {scan.path}")
    print(f"Time:         {scan.time}")
    print(f"Blob:         {scan.blob}")
    print(f"Size:         {scan.size:,}" if scan.size else "Size:         (unknown)")
    print(f"Children:     {scan.n_children:,}" if scan.n_children else "Children:     (unknown)")
    print(f"Descendants:  {scan.n_desc:,}" if scan.n_desc else "Descendants:  (unknown)")
    print(f"Errors:       {scan.error_count or 0}")

    if isfile(scan.blob):
        # Show parquet file size
        from os.path import getsize
        blob_size = getsize(scan.blob)
        print(f"Blob size:    {blob_size:,} bytes")

        # Check for chunks
        df = pd.read_parquet(scan.blob)
        if 'child_scan_id' in df.columns:
            chunks = df[df['child_scan_id'].notna()]
            if not chunks.empty:
                print(f"Chunks:       {len(chunks)}")
                for _, row in chunks.iterrows():
                    chunk_size = getsize(row['child_scan_id']) if isfile(row['child_scan_id']) else 0
                    print(f"  - {row['path']}: {row['n_desc']:,} descendants, {chunk_size:,} bytes")
