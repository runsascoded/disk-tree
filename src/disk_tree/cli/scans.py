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
