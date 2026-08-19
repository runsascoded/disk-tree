"""`disk-tree diff` — per-path Δ table between two scans (CLI wrapper on the same
logic `/api/compare` runs; spec Item C).

Argument shapes:
- `dt diff SCAN1_ID SCAN2_ID` — compare two scans by id
- `dt diff URI` — compare the two most recent scans of `URI` (auto-picks ids)
"""

from __future__ import annotations

import sqlite3
from os.path import dirname

import pandas as pd
from click import argument, option
from humanize import naturalsize
from utz import err

from disk_tree.cli.base import cli
from disk_tree.config import SQLITE_PATH


def _connect() -> sqlite3.Connection:
    con = sqlite3.connect(SQLITE_PATH)
    con.row_factory = sqlite3.Row
    return con


def _resolve_scans(con: sqlite3.Connection, args: tuple[str, ...]) -> tuple[sqlite3.Row, sqlite3.Row, str]:
    """Return (scan_a, scan_b, uri). Accepts one URI arg (auto-picks 2 most recent)
    or two scan-id args (uses those ids and picks their common path)."""
    if len(args) == 1:
        uri = args[0].rstrip('/') or '/'
        rows = con.execute(
            'SELECT * FROM scan WHERE path = ? ORDER BY time DESC LIMIT 2', (uri,),
        ).fetchall()
        if len(rows) < 2:
            raise ValueError(f"need ≥2 scans of {uri!r}; found {len(rows)}")
        # Most recent as scan_b, older as scan_a
        return rows[1], rows[0], uri
    if len(args) == 2:
        try:
            id_a, id_b = int(args[0]), int(args[1])
        except ValueError:
            raise ValueError(f"two-arg form requires numeric scan ids; got {args!r}")
        scan_a = con.execute('SELECT * FROM scan WHERE id = ?', (id_a,)).fetchone()
        scan_b = con.execute('SELECT * FROM scan WHERE id = ?', (id_b,)).fetchone()
        if not scan_a or not scan_b:
            raise ValueError(f"scan id not found: {id_a if not scan_a else id_b}")
        # URI defaults to the common (identical) scan path
        if scan_a['path'] != scan_b['path']:
            raise ValueError(f"scans have different roots: {scan_a['path']!r} vs {scan_b['path']!r}; pass --path to disambiguate")
        return scan_a, scan_b, scan_a['path']
    raise ValueError(f"expected 1 URI or 2 scan ids; got {args!r}")


def _children_at(blob: str, uri: str, scan_path: str, depth: int) -> pd.DataFrame:
    """Load rows at `uri`'s child-depth from a scan blob. Same shape as
    server.py:1151 (`get_children`)."""
    from disk_tree.storage import get_backend
    backend = get_backend()
    if scan_path == uri:
        rel_prefix = ''
        depth_offset = 0
    else:
        rel_prefix = uri[len(scan_path):].lstrip('/')
        depth_offset = rel_prefix.count('/') + 1
    target_depth = depth_offset + depth
    df = backend.load(blob, max_depth=target_depth, min_depth=target_depth, path_prefix=rel_prefix or None)
    if scan_path == uri:
        # Root: dirs have parent='.', root-level files have parent='' — match /api/compare fix.
        children = df[(df['parent'] == '.') | ((df['parent'] == '') & (df['path'] != '.'))].copy()
        children['rel_path'] = children['path']
    else:
        children = df[df['parent'] == rel_prefix].copy()
        children['rel_path'] = children['path'].str.split('/').str[-1]
    return children


def _delta_rows(children1: pd.DataFrame, children2: pd.DataFrame) -> list[dict]:
    p1 = set(children1['rel_path']) if len(children1) else set()
    p2 = set(children2['rel_path']) if len(children2) else set()
    # Index by rel_path: a boolean mask per child inside the loops is O(C²)
    # total — ruinous on 100k-wide cloud prefixes.
    by_rel1 = children1.set_index('rel_path', drop=False) if len(children1) else children1
    by_rel2 = children2.set_index('rel_path', drop=False) if len(children2) else children2
    by_rel1 = by_rel1[~by_rel1.index.duplicated()] if len(by_rel1) else by_rel1
    by_rel2 = by_rel2[~by_rel2.index.duplicated()] if len(by_rel2) else by_rel2
    out: list[dict] = []
    for rp in p2 - p1:
        row = by_rel2.loc[rp]
        out.append({
            'path': rp, 'status': 'added',
            'size_a': 0, 'size_b': int(row['size'] or 0),
            'n_desc_a': 0, 'n_desc_b': int(row.get('n_desc', 0) or 0),
        })
    for rp in p1 - p2:
        row = by_rel1.loc[rp]
        out.append({
            'path': rp, 'status': 'removed',
            'size_a': int(row['size'] or 0), 'size_b': 0,
            'n_desc_a': int(row.get('n_desc', 0) or 0), 'n_desc_b': 0,
        })
    for rp in p1 & p2:
        r1 = by_rel1.loc[rp]
        r2 = by_rel2.loc[rp]
        sa, sb = int(r1['size'] or 0), int(r2['size'] or 0)
        na, nb = int(r1.get('n_desc', 0) or 0), int(r2.get('n_desc', 0) or 0)
        out.append({
            'path': rp,
            'status': 'changed' if (sa != sb or na != nb) else 'unchanged',
            'size_a': sa, 'size_b': sb, 'n_desc_a': na, 'n_desc_b': nb,
        })
    for r in out:
        r['size_delta'] = r['size_b'] - r['size_a']
        r['n_desc_delta'] = r['n_desc_b'] - r['n_desc_a']
    out.sort(key=lambda r: -abs(r['size_delta']))
    return out


def _fmt_signed(n: int) -> str:
    return f'{n:+,}'


@cli.command('diff')
@option('-b', '--budget', default=100, help='Recursive mode: max directory expansions (default 100)')
@option('-d', '--depth', default=1, help='Compare depth (children at this level; default 1). In recursive mode: deepest level to descend to (0 = unlimited)')
@option('-H', '--no-human', is_flag=True, help='Print raw bytes / counts instead of human-readable sizes')
@option('-n', '--top', default=30, help='Show top-N rows by |Δsize|')
@option('-p', '--path', 'path_override', default=None, help='Path within the scans to compare (default: scan root)')
@option('-r', '--recursive', is_flag=True, help='Walk changed spines best-first (|Δsize| priority) and print the delta frontier; added/removed dirs are not descended, stats-equal dirs are pruned')
@option('-u', '--unchanged', is_flag=True, help='Include unchanged rows')
@argument('args', nargs=-1, required=True)
def diff_cmd(budget: int, depth: int, no_human: bool, top: int, path_override: str | None, recursive: bool, unchanged: bool, args: tuple[str, ...]):
    """Print a per-path Δ table between two scans.

    Args: `<URI>` (picks the two most-recent scans) or `<SCAN1_ID> <SCAN2_ID>`.
    """
    con = _connect()
    scan_a, scan_b, uri = _resolve_scans(con, args)
    if path_override:
        uri = path_override.rstrip('/') or '/'

    truncated = False
    if recursive:
        from disk_tree.diff import ScanSource, recursive_diff, resolve_chunk_for_path
        from disk_tree.storage import get_backend
        backend = get_backend()
        src_a = ScanSource(scan_a['blob'], scan_a['path'], uri, backend.load, resolve=resolve_chunk_for_path)
        src_b = ScanSource(scan_b['blob'], scan_b['path'], uri, backend.load, resolve=resolve_chunk_for_path)
        # -d 1 is the non-recursive default, not a meaningful recursion cap
        max_depth = depth if depth > 1 else None
        result = recursive_diff(src_a, src_b, budget=budget, max_depth=max_depth, include_unchanged=unchanged)
        truncated = result.truncated
        rows = [
            {
                'path': r.path + (' …' if r.pruned else ''),
                'depth': r.depth,
                'status': r.status,
                'size_a': r.size_a, 'size_b': r.size_b,
                'size_delta': r.size_delta, 'n_desc_delta': r.n_desc_delta,
            }
            for r in result.rows
        ]
    else:
        children_a = _children_at(scan_a['blob'], uri, scan_a['path'], depth)
        children_b = _children_at(scan_b['blob'], uri, scan_b['path'], depth)
        rows = _delta_rows(children_a, children_b)
        if not unchanged:
            rows = [r for r in rows if r['status'] != 'unchanged']

    err(f"a: scan {scan_a['id']} @ {scan_a['time']}  ({scan_a['path']})")
    err(f"b: scan {scan_b['id']} @ {scan_b['time']}  ({scan_b['path']})")
    mode = f"recursive budget={budget}" if recursive else f"depth={depth}"
    err(f"uri={uri}  {mode}  {len(rows)} rows" + ("  (truncated: '…' rows have unexplored change below)" if truncated else ""))

    def sz(n: int) -> str:
        return naturalsize(n, binary=True, format='%.3g') if not no_human else f'{n:,}'
    def dsz(n: int) -> str:
        s = sz(abs(n))
        return f'+{s}' if n > 0 else (f'-{s}' if n < 0 else s)

    keys = rows[:top] if len(rows) > top else rows
    if not keys:
        print("(no differing rows)")
        return
    w = max(4, max(len(r['path']) for r in keys))
    hdr = f"{'path':{w}}  {'status':8}  {'size_a':>10}  {'size_b':>10}  {'Δsize':>11}  {'Δn_desc':>10}"
    print(hdr)
    print('-' * len(hdr))
    for r in keys:
        print(f"{r['path']:{w}}  {r['status']:8}  {sz(r['size_a']):>10}  {sz(r['size_b']):>10}  {dsz(r['size_delta']):>11}  {_fmt_signed(r['n_desc_delta']):>10}")
    if len(rows) > top:
        print(f"(… {len(rows) - top} more rows)")
    # Recursive rows span depths, and a frontier row's Δ is already included in
    # its ancestors' — sum only depth-1 rows there (deltas propagate up, so
    # they carry the whole subtree total).
    total = sum(r['size_delta'] for r in rows if r.get('depth', 1) == 1)
    print('-' * len(hdr))
    print(f"{'TOTAL':{w}}  {'':8}  {'':>10}  {'':>10}  {dsz(total):>11}")
