"""Scan-registry queries shared by the server and the CLI (no Flask import)."""

from __future__ import annotations


def freshest_scan_covering(db, uri: str, scan_id: str | None = None) -> dict | None:
    """Newest scan whose path is `uri` or an ancestor of it (or `scan_id`, verified)."""
    from disk_tree.backends import url_parent
    if scan_id:
        row = db.execute('SELECT * FROM scan WHERE id = ?', (scan_id,)).fetchone()
        if not row:
            return None
        scan = dict(row)
        if uri == scan['path'] or uri.startswith(scan['path'].rstrip('/') + '/'):
            return scan
        return None
    candidates = []
    test_path = uri
    while test_path:
        row = db.execute(
            'SELECT * FROM scan WHERE path = ? ORDER BY time DESC LIMIT 1', (test_path,)
        ).fetchone()
        if row:
            candidates.append(dict(row))
        parent = url_parent(test_path)
        if parent is None or parent == test_path:
            break
        test_path = parent
    if not candidates:
        return None
    return max(candidates, key=lambda s: s['time'])
