"""`disk-tree vocab` — build the vocabulary sidecar for a scan's parquet blob
(spec: diff-and-search.md, index tiers 1–2). The sidecar accelerates
segment-local `disk-tree filter` / `/api/filter` queries and is the
static-consumable name index (sorted names, bounded row groups)."""

from __future__ import annotations

import json
import sqlite3

from click import argument, option
from humanize import naturalsize
from utz import err

from disk_tree.cli.base import cli
from disk_tree.config import SQLITE_PATH


@cli.command('vocab')
@option('-f', '--force', is_flag=True, help='Rebuild even if a fresh sidecar exists')
@option('-j', '--json', 'as_json', is_flag=True, help='Emit JSON stats')
@option('-s', '--scan-id', default=None, help='Use a specific scan id (default: freshest covering URI)')
@argument('uri')
def vocab_cmd(force: bool, as_json: bool, scan_id: str | None, uri: str):
    """Build the vocab sidecar (`<blob>.vocab.parquet`) for the scan covering URI."""
    from disk_tree.diff import resolve_blob
    from disk_tree.registry import freshest_scan_covering
    from disk_tree.sidecar import build_vocab_sidecar

    uri = uri.rstrip('/') or '/'
    con = sqlite3.connect(SQLITE_PATH)
    con.row_factory = sqlite3.Row
    scan = freshest_scan_covering(con, uri, scan_id)
    if not scan:
        raise SystemExit(f"no scan covering {uri!r}")
    blob = resolve_blob(scan['blob'])
    if blob.startswith(('ddb:', 'sqlite:')):
        raise SystemExit(f"scan {scan['id']} stores rows in {blob!r} — vocab sidecars require parquet blobs")

    stats = build_vocab_sidecar(blob, force=force)
    if as_json:
        print(json.dumps(vars(stats), indent=2))
        return
    err(f"scan {scan['id']} @ {scan['time']}  ({scan['path']})")
    print(f"{stats.path}")
    print(f"{stats.n_names:,} names over {stats.n_rows:,} rows "
          f"({stats.n_row_groups} row group{'s' if stats.n_row_groups != 1 else ''}), "
          f"{naturalsize(stats.size_bytes, binary=True, format='%.3g')}")
