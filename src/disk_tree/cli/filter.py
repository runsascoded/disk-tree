"""`disk-tree filter` — recursive filter with true re-aggregation (spec:
diff-and-search.md §4 v1): total sizes of everything matching QUERY under URI,
rolled up to a display-depth slice. Matches are outermost-only, so totals
never double-count."""

from __future__ import annotations

import json
import sqlite3

from click import argument, option
from humanize import naturalsize
from utz import err

from disk_tree.cli.base import cli
from disk_tree.config import SQLITE_PATH


@cli.command('filter')
@option('-c', '--case-sensitive', is_flag=True, help='Match case-sensitively')
@option('-d', '--depth', default=4, help='Display depth of the rollup slice (default 4); totals always cover every depth')
@option('-H', '--no-human', is_flag=True, help='Print raw bytes instead of human-readable sizes')
@option('-j', '--json', 'as_json', is_flag=True, help='Emit JSON')
@option('-n', '--top', default=40, help='Show top-N rows by size (0 = all; default 40)')
@option('-s', '--scan-id', default=None, help='Use a specific scan id (default: freshest covering URI)')
@argument('uri')
@argument('query')
def filter_cmd(case_sensitive: bool, depth: int, no_human: bool, as_json: bool, top: int, scan_id: str | None, uri: str, query: str):
    """Recursively filter a scan: sizes of everything under URI matching QUERY.

    QUERY: `/…/flags` is a regex, anything else a substring (case-insensitive
    by default; an invalid regex degrades to a substring match).
    """
    from disk_tree.diff import resolve_chunk_for_path
    from disk_tree.filter import filter_scan, rebase_frame
    from disk_tree.registry import freshest_scan_covering
    from disk_tree.storage import get_backend

    uri = uri.rstrip('/') or '/'
    con = sqlite3.connect(SQLITE_PATH)
    con.row_factory = sqlite3.Row
    scan = freshest_scan_covering(con, uri, scan_id)
    if not scan:
        raise SystemExit(f"no scan covering {uri!r}")

    if scan['path'] == uri:
        rel = '.'
    else:
        rel = uri[len(scan['path'].rstrip('/') + '/'):]

    backend = get_backend()
    # Resolve into the chunk holding `uri` first (loading the root blob with
    # follow_refs would expand *every* chunk); follow_refs then only expands
    # chunks nested inside the resolved subtree — a search must not silently
    # miss them.
    blob, rebased = resolve_chunk_for_path(scan['blob'], rel)
    df = backend.load(blob, follow_refs=True, path_prefix=rebased if rebased != '.' else None)
    df = rebase_frame(df, rebased)
    result = filter_scan(df, query, display_depth=depth, case_sensitive=case_sensitive)

    if as_json:
        print(json.dumps({
            'uri': uri,
            'scan_id': scan['id'],
            'query': query,
            'total_size': result.total_size,
            'n_matches': result.n_matches,
            'max_depth_scanned': result.max_depth_scanned,
            'rows': [vars(n) for n in result.nodes],
        }, indent=2))
        return

    err(f"scan {scan['id']} @ {scan['time']}  ({scan['path']})")
    err(f"uri={uri}  query={query!r}  {result.n_matches} matches, depth<={depth} slice: {len(result.nodes)} rows")

    def sz(n: int) -> str:
        return f'{n:,}' if no_human else naturalsize(n, binary=True, format='%.3g')

    if not result.nodes:
        print("(no matches)")
        return
    nodes = result.nodes
    if top and len(nodes) > top:
        nodes = sorted(nodes, key=lambda n: -n.size)[:top]
        nodes.sort(key=lambda n: (n.depth, n.path))
    w = max(4, max(len(n.path) for n in nodes)) + 2  # room for the ' *' marker
    hdr = f"{'path':{w}}  {'size':>10}  {'matches':>8}"
    print(hdr)
    print('-' * len(hdr))
    for n in nodes:
        marker = ' *' if n.matched else ''
        print(f"{n.path + marker:{w}}  {sz(n.size):>10}  {n.n_matches:>8}")
    if len(nodes) < len(result.nodes):
        print(f"(… {len(result.nodes) - len(nodes)} more rows; -n 0 for all)")
    print('-' * len(hdr))
    print(f"{'TOTAL':{w}}  {sz(result.total_size):>10}  {result.n_matches:>8}")
    err("(*) = the row itself matched; other rows are ancestors carrying rolled-up matched bytes")
