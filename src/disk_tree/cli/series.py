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
    # NB: we use `.format()` here instead of nested-quote f-strings — PEP 701
    # relaxations (same-quote nesting inside f-strings) landed in Python 3.12,
    # and this project supports 3.11.
    def _fmt_count(v):
        return f'{v:,}' if v is not None else '—'
    for r in display:
        rid, rtime, rpath, rsize = r['id'], r['time'], r['path'], sz(r['size'])
        rdesc, rkids = _fmt_count(r['n_desc']), _fmt_count(r['n_children'])
        print(f"{rid:>5}  {rtime:{w_time}}  {rpath:{w_path}}  {rsize:>10}  {rdesc:>10}  {rkids:>10}")
    if limit and len(rows) > limit:
        print(f"(… {len(rows) - limit} older scans)")
