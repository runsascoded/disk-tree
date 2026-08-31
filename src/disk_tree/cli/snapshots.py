"""`disk-tree snapshots` — publish scans as a static snapshot library.

The no-live-Python tier of the file-tree integration (spec
`specs/file-tree-integration.md`, Half B1): a `snapshots.json` index plus one
self-contained `snapshots/<id>/tree.parquet` per scan, which file-tree's
`snapshotTreeSource` reads straight from a bucket. Chunked (hybrid) scans are
materialized into a single parquet, rows sorted `(depth, path)` in 64K row
groups so the same depth/path-prefix pushdowns work on the published copy.
"""
import json
from os import makedirs
from os.path import dirname, exists, join

from click import argument, option
from utz import err

from disk_tree.cli.base import cli
from disk_tree.diff_index import index_path
from disk_tree.storage import get_backend
from disk_tree.storage.base import BLOB_ROW_GROUP_SIZE
from disk_tree.sqla import Scan, init

#: Bumped when the published layout or row contract changes.
SNAPSHOT_LAYOUT_VERSION = 1

#: Core row schema of every `tree.parquet` — the public contract file-tree maps
#: snake→camel at the boundary (`storage/base.py` `save`). Internal columns
#: (`child_scan_id` chunk refs, `n_files`) are projected out so a published tree
#: is self-contained.
CORE_COLUMNS = ['path', 'size', 'mtime', 'kind', 'parent', 'uri', 'n_desc', 'n_children', 'depth']
#: Emitted too when the scan carries it (indexed with `--mean-mtime`).
OPTIONAL_COLUMNS = ['mtime_mean']
ROW_COLUMNS = [*CORE_COLUMNS, 'mtime_mean?']


def _publish_columns(df) -> list[str]:
    """Public columns present in `df`, in contract order (drops internal columns)."""
    return [c for c in [*CORE_COLUMNS, *OPTIONAL_COLUMNS] if c in df.columns]


def _latest_per_path(scans: list[Scan]) -> list[Scan]:
    latest: dict[str, Scan] = {}
    for s in scans:
        cur = latest.get(s.path)
        if cur is None or s.time > cur.time:
            latest[s.path] = s
    return list(latest.values())


@cli.command('snapshots')
@option('-a', '--all', 'all_scans', is_flag=True, help='Publish every scan (default: the newest per path)')
@option('-d', '--diffs', is_flag=True, help='Also publish existing diff-index blobs between consecutive snapshots of a path')
@option('-n', '--dry-run', is_flag=True, help='List what would publish, then stop')
@option('-s', '--scan', 'scan_ids', multiple=True, type=int, help='Publish only these scan ids (repeatable)')
@argument('dest')
def snapshots(all_scans: bool, diffs: bool, dry_run: bool, scan_ids: tuple[int, ...], dest: str):
    """Publish scans as a static snapshot library under DEST (a local directory).

    Writes `DEST/snapshots.json` (index) + `DEST/snapshots/<id>/tree.parquet`
    per scan. Upload the tree with `aws s3 sync`/`gsutil rsync` to serve it as a
    no-live-Python `snapshotTreeSource`. See spec `file-tree-integration.md` B1.
    """
    if '://' in dest:
        raise SystemExit(f'DEST must be a local dir; publish locally then sync to your bucket (got {dest!r})')

    db = init()
    have = db.session.query(Scan).filter(Scan.blob.isnot(None)).all()
    if scan_ids:
        want = set(scan_ids)
        selected = [s for s in have if s.id in want]
        missing = want - {s.id for s in selected}
        if missing:
            raise SystemExit(f'no such scan id(s): {sorted(missing)}')
    elif all_scans:
        selected = have
    else:
        selected = _latest_per_path(have)
    selected.sort(key=lambda s: (s.path, s.time))

    backend = get_backend()
    published: dict[str, list[Scan]] = {}  # path -> its published scans, time order
    index = []
    for s in selected:
        published.setdefault(s.path, []).append(s)
        entry = {
            'id': s.id,
            'path': s.path,
            'time': s.time.isoformat(),
            'size': s.size,
            'n_desc': s.n_desc,
            'n_children': s.n_children,
            'tree': f'snapshots/{s.id}/tree.parquet',
        }
        out = join(dest, 'snapshots', str(s.id), 'tree.parquet')
        err(f'snapshot {s.id}: {s.path} ({s.n_desc} desc) -> {out}')
        if not dry_run:
            makedirs(dirname(out), exist_ok=True)
            df = backend.load(s.blob, follow_refs=True)
            df = df[_publish_columns(df)].sort_values(['depth', 'path'])
            df.to_parquet(out, index=False, row_group_size=BLOB_ROW_GROUP_SIZE)

        if diffs:
            prev = published[s.path][:-1]
            if prev:
                a = prev[-1]
                src = index_path(a.id, s.id)
                if exists(src):
                    rel = f'snapshots/{s.id}/diffs/{a.id}-{s.id}.parquet'
                    entry['diffs'] = [{'from': a.id, 'to': s.id, 'blob': rel}]
                    err(f'  diff {a.id}->{s.id} -> {join(dest, rel)}')
                    if not dry_run:
                        import shutil
                        dst = join(dest, rel)
                        makedirs(dirname(dst), exist_ok=True)
                        shutil.copyfile(src, dst)
        index.append(entry)

    manifest = {
        'version': SNAPSHOT_LAYOUT_VERSION,
        'columns': ROW_COLUMNS,
        'row_group_size': BLOB_ROW_GROUP_SIZE,
        'snapshots': index,
    }
    idx_path = join(dest, 'snapshots.json')
    err(f'{len(index)} snapshot{"" if len(index) == 1 else "s"} -> {idx_path}')
    if not dry_run:
        makedirs(dest, exist_ok=True)
        with open(idx_path, 'w') as f:
            json.dump(manifest, f, indent=2)
            f.write('\n')
