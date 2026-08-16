"""`disk-tree histogram` — byte-weighted mtime distribution per child (spec: viz-widgets.md §4).

CLI twin of `/api/histogram`: same computation, terminal-shaped output. Each
child's row is a sparkline of *bytes* per mtime bin, oldest bin on the left, so
a directory whose weight sits in the old bins is visible at a glance.
"""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime

from click import argument, option
from humanize import naturalsize
from utz import err

from disk_tree.cli.base import cli
from disk_tree.config import SQLITE_PATH
from disk_tree.histogram import age_histograms

BLOCKS = ' ▁▂▃▄▅▆▇█'


def sparkline(values: list[int], peak: int) -> str:
    """One block char per bin, scaled against a *shared* peak so rows compare."""
    if peak <= 0:
        return ' ' * len(values)
    return ''.join(BLOCKS[min(len(BLOCKS) - 1, round(v / peak * (len(BLOCKS) - 1)))] for v in values)


@cli.command('histogram')
@option('-b', '--bins', default=24, help='Number of mtime bins')
@option('-j', '--json', 'as_json', is_flag=True, help='Emit JSON (same shape as /api/histogram)')
@option('-n', '--limit', default=20, help='Max children, biggest-first; 0 for all')
@option('-s', '--scan-id', default=None, type=int, help='Specific scan id (default: freshest covering scan)')
@option('-S', '--shared-scale', is_flag=True, help='Scale all sparklines against one peak (bar heights comparable across rows, but one huge child flattens the rest)')
@argument('uri')
def histogram_cmd(bins: int, as_json: bool, limit: int, scan_id: int | None, shared_scale: bool, uri: str):
    """Byte-weighted mtime histogram for each child of URI."""
    uri = uri.rstrip('/') or '/'
    con = sqlite3.connect(SQLITE_PATH)
    con.row_factory = sqlite3.Row
    if scan_id is not None:
        row = con.execute('SELECT * FROM scan WHERE id = ?', (scan_id,)).fetchone()
    else:
        # Freshest scan of the URI or any ancestor of it.
        paths, p = [uri], uri
        while p and p != '/':
            from os.path import dirname
            q = dirname(p)
            if q == p:
                break
            if q:
                paths.append(q)
            p = q
        placeholders = ','.join('?' * len(paths))
        row = con.execute(
            f'SELECT * FROM scan WHERE path IN ({placeholders}) ORDER BY time DESC LIMIT 1', paths,
        ).fetchone()
    con.close()
    if not row:
        raise SystemExit(f"no scan found covering {uri}")

    scan_path = row['path']
    rel_path = '.' if scan_path == uri else uri[len(scan_path.rstrip('/')) + 1:]

    from disk_tree.storage import get_backend
    df = get_backend().load(row['blob'])
    hist = age_histograms(df, rel_path=rel_path, bins=bins, limit=None if limit == 0 else limit)

    if as_json:
        print(json.dumps({'uri': uri, 'scan_path': scan_path, 'time': row['time'], **hist.to_dict()}))
        return

    if not hist.children:
        err(f"(no files under {uri} in scan {row['id']} of {scan_path})")
        return

    def when(ts: int) -> str:
        return datetime.fromtimestamp(ts).strftime('%Y-%m-%d')

    # Per-row scaling by default: the `size` column already carries magnitude,
    # so the sparkline's job is *shape* — and one 2 GB child otherwise renders
    # every other row blank.
    shared_peak = max(max(c.bytes) for c in hist.children)
    w_path = max(4, max(len(c.path) for c in hist.children))
    scale_note = 'shared scale' if shared_scale else 'per-row scale'
    print(f"{uri} — scan {row['id']} of {scan_path} ({row['time']})")
    print(f"{'child':{w_path}}  {'size':>9}  {'files':>8}  {when(hist.edges[0])} → {when(hist.edges[-1])} ({scale_note})")
    for c in hist.children:
        size = naturalsize(c.total_bytes, binary=True, format='%.3g')
        peak = shared_peak if shared_scale else max(c.bytes)
        print(f"{c.path:{w_path}}  {size:>9}  {c.n_files:>8,}  {sparkline(c.bytes, peak)}")
    if hist.omitted:
        omitted_size = naturalsize(hist.omitted_bytes, binary=True, format='%.3g')
        print(f"(… {hist.omitted:,} more children, {omitted_size})")
