"""`disk-tree series` — chronological list of scans of a URI (spec Item C)."""

from __future__ import annotations

import sqlite3
from os.path import dirname

from click import argument, option
from humanize import naturalsize

from disk_tree.cli.base import cli
from disk_tree.config import SQLITE_PATH


@cli.command('series')
@option('-a', '--ancestors', is_flag=True, help='Include scans of ancestor paths (matches /api/scans/history)')
@option('-H', '--no-human', is_flag=True, help='Raw bytes instead of human-readable')
@option('-n', '--limit', default=None, type=int, help='Show only the newest N scans')
@argument('uri')
def series_cmd(ancestors: bool, no_human: bool, limit: int | None, uri: str):
    """Print all scans of URI (or its ancestors with -a) in chronological order."""
    uri = uri.rstrip('/') or '/'
    paths = [uri]
    if ancestors:
        p = uri
        while p and p != '/':
            q = dirname(p)
            if q == p:
                break
            if q:
                paths.append(q)
            p = q
        if not uri.startswith(('s3://', 'gcs://', 'r2://', 'ssh://')) and '/' not in paths:
            paths.append('/')

    con = sqlite3.connect(SQLITE_PATH)
    con.row_factory = sqlite3.Row
    placeholders = ','.join('?' * len(paths))
    rows = con.execute(
        f'SELECT id, path, time, size, n_children, n_desc FROM scan '
        f'WHERE path IN ({placeholders}) ORDER BY time DESC',
        paths,
    ).fetchall()
    con.close()

    if not rows:
        print(f"(no scans of {uri} found)")
        return

    def sz(n) -> str:
        if n is None:
            return '—'
        return naturalsize(n, binary=True, format='%.3g') if not no_human else f'{n:,}'

    display = rows[:limit] if limit else rows
    w_path = max(4, max(len(r['path']) for r in display))
    w_time = max(len(r['time']) for r in display)

    hdr = f"{'id':>5}  {'time':{w_time}}  {'path':{w_path}}  {'size':>10}  {'n_desc':>10}  {'n_children':>10}"
    print(hdr)
    print('-' * len(hdr))
    for r in display:
        print(f"{r['id']:>5}  {r['time']:{w_time}}  {r['path']:{w_path}}  {sz(r['size']):>10}  {(f'{r['n_desc']:,}' if r['n_desc'] is not None else '—'):>10}  {(f'{r['n_children']:,}' if r['n_children'] is not None else '—'):>10}")
    if limit and len(rows) > limit:
        print(f"(… {len(rows) - limit} older scans)")
