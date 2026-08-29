"""`disk-tree du` — top-N heaviest children per level of a cached scan.

The `du -d N | sort -rh` of an *indexed* tree: no filesystem walk, just a
depth-pushed-down parquet read, so "what is eating my disk" is answerable in
milliseconds against a scan taken hours ago.
"""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime

from click import argument, option
from humanize import naturalsize

from disk_tree.cli.base import cli
from disk_tree.config import SQLITE_PATH


def _fmt(size: int, human: bool) -> str:
    return naturalsize(size, binary=True, format='%.3g') if human else str(size)


@cli.command('du')
@option('-a', '--all-kinds', is_flag=True, help='Include files (default: directories only)')
@option('-d', '--depth', default=1, help='Levels below URI to descend (default 1)')
@option('-H', '--no-human', is_flag=True, help='Print raw bytes instead of human-readable sizes')
@option('-j', '--json', 'as_json', is_flag=True, help='Emit JSON')
@option('-n', '--top', default=15, help='Max children shown per level (0 = all)')
@option('-s', '--scan-id', default=None, help='Use a specific scan id (default: freshest covering URI)')
@argument('uri')
def du_cmd(all_kinds: bool, depth: int, no_human: bool, as_json: bool, top: int, scan_id: str | None, uri: str):
    """Biggest children of URI, per level, from the freshest covering scan."""
    from disk_tree.diff import resolve_chunk_for_path
    from disk_tree.filter import rebase_frame
    from disk_tree.registry import freshest_scan_covering
    from disk_tree.storage import get_backend

    uri = uri.rstrip('/') or '/'
    con = sqlite3.connect(SQLITE_PATH)
    con.row_factory = sqlite3.Row
    scan = freshest_scan_covering(con, uri, scan_id)
    con.close()
    if not scan:
        raise SystemExit(f"no scan covering {uri!r}")

    rel = '.' if scan['path'] == uri else uri[len(scan['path'].rstrip('/') + '/'):]
    blob, rebased = resolve_chunk_for_path(scan['blob'], rel)
    df = get_backend().load(blob, follow_refs=True, path_prefix=rebased if rebased != '.' else None)
    root_size = int(df.loc[df['path'] == rebased, 'size'].iloc[0]) if (df['path'] == rebased).any() else 0
    df = rebase_frame(df, rebased)
    # Depth 0 is the subtree root itself (present only when `rel` is the scan root).
    df = df[(df['depth'] >= 1) & (df['depth'] <= depth)]
    if not all_kinds:
        df = df[df['kind'] == 'dir']

    kids: dict[str, list[dict]] = {}
    for row in df.sort_values('size', ascending=False).itertuples():
        parent = row.path.rsplit('/', 1)[0] if '/' in row.path else ''
        kids.setdefault(parent, []).append({
            'path': row.path,
            'size': int(row.size),
            'mtime': float(row.mtime),
            'kind': row.kind,
            'n_desc': int(row.n_desc),
        })

    def walk(parent: str) -> list[dict]:
        rows = kids.get(parent, [])
        shown = rows if top == 0 else rows[:top]
        out = []
        for r in shown:
            out.append({**r, 'children': walk(r['path'])})
        rest = rows[len(shown):]
        if rest:
            out.append({
                'path': f'{parent}/…' if parent else '…',
                'size': sum(r['size'] for r in rest),
                'mtime': 0.0,
                'kind': 'rest',
                'n_desc': sum(r['n_desc'] for r in rest),
                'children': [],
                'n_rest': len(rest),
            })
        return out

    tree = walk('')
    if as_json:
        print(json.dumps({'uri': uri, 'scan_id': scan['id'], 'time': scan['time'], 'size': root_size, 'rows': tree}, indent=2))
        return

    human = not no_human
    print(f"{uri} — {_fmt(root_size, human)} (scan {scan['id']} of {scan['path']}, {str(scan['time'])[:19]})")

    def emit(rows: list[dict], indent: int):
        for r in rows:
            name = r['path'].rsplit('/', 1)[-1]
            if r['kind'] == 'rest':
                print(f"{_fmt(r['size'], human):>9}  {'':>10}  {'  ' * indent}… {r['n_rest']} more")
                continue
            when = datetime.fromtimestamp(r['mtime']).strftime('%Y-%m-%d') if r['mtime'] else ''
            suffix = '/' if r['kind'] == 'dir' else ''
            print(f"{_fmt(r['size'], human):>9}  {when:>10}  {'  ' * indent}{name}{suffix}")
            emit(r['children'], indent + 1)

    emit(tree, 0)
