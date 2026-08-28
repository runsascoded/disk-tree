"""`disk-tree diff-index`: build persisted scan-pair diff indexes (spec: diff-index.md)."""
import sqlite3

from click import argument, option
from utz import err

from disk_tree.cli.base import cli
from disk_tree.config import SQLITE_PATH


def _connect() -> sqlite3.Connection:
    con = sqlite3.connect(SQLITE_PATH)
    con.row_factory = sqlite3.Row
    return con


def build_pair(scan_a: int, scan_b: int, force: bool = False) -> dict | None:
    """Build (or reuse) the index for two scan ids; logs to stderr."""
    from disk_tree.diff_index import build_and_record, get_index
    if not force:
        existing = get_index(scan_a, scan_b)
        if existing and existing['status'] == 'done':
            err(f"diff {scan_a}→{scan_b}: already indexed ({existing['n_rows']:,} rows, {existing['blob']})")
            return existing
    with _connect() as con:
        a = con.execute('SELECT * FROM scan WHERE id = ?', (scan_a,)).fetchone()
        b = con.execute('SELECT * FROM scan WHERE id = ?', (scan_b,)).fetchone()
    if a is None or b is None:
        raise ValueError(f"scan id not found: {scan_a if a is None else scan_b}")
    err(f"diff {scan_a}→{scan_b}: indexing {a['path']} ({a['time']} → {b['time']})")
    row = build_and_record(scan_a, a['blob'], scan_b, b['blob'])
    err(f"diff {scan_a}→{scan_b}: {row['n_rows']:,} rows (+{row['n_added']:,} −{row['n_removed']:,} ~{row['n_changed']:,} touched {row['n_touched']:,}) in {row['seconds']:.1f}s → {row['blob']}")
    return row


def build_latest(path: str, force: bool = False) -> dict | None:
    """Index the two most recent scans of `path` (no-op with fewer than two)."""
    with _connect() as con:
        rows = con.execute('SELECT id FROM scan WHERE path = ? ORDER BY time DESC LIMIT 2', (path,)).fetchall()
    if len(rows) < 2:
        err(f"{path}: fewer than two scans, nothing to diff")
        return None
    return build_pair(rows[1]['id'], rows[0]['id'], force=force)


@cli.command('diff-index')
@option('-a', '--all', 'all_paths', is_flag=True, help='Every scanned path: index its two most recent scans')
@option('-f', '--force', is_flag=True, help='Rebuild even if an index exists')
@argument('args', nargs=-1)
def diff_index(all_paths: bool, force: bool, args: tuple[str, ...]):
    """Build the persisted diff index for a scan pair.

    ARGS: two scan ids (`A B`), or one or more paths (each path's two most
    recent scans). With -a, every scanned path. `disk-tree index` builds the
    index against the previous scan automatically.
    """
    if all_paths:
        with _connect() as con:
            paths = [r['path'] for r in con.execute('SELECT DISTINCT path FROM scan ORDER BY path')]
        for p in paths:
            build_latest(p, force=force)
        return
    if len(args) == 2 and all(a.isdigit() for a in args):
        build_pair(int(args[0]), int(args[1]), force=force)
        return
    if not args:
        raise SystemExit('expected two scan ids, one or more paths, or -a')
    for p in args:
        build_latest(p.rstrip('/') or '/', force=force)
