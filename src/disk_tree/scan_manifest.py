"""A scan's metadata as a small JSON beside a *remote* blob — the portable
"DB row" that travels with a cloud-stored scan (a runner's own SQLite is thrown
away). The manifest route of `done/remote-scan-targets.md`.

Written by `index --to <url>`. A local blob needs none:
the local DB already has the row.
"""
from __future__ import annotations

import json

from disk_tree import blobfs

FORMAT = 'disk-tree-scan'
VERSION = 1
SUFFIX = '.scan.json'
#: The `Scan` columns a manifest carries (plus `time`, formatted).
FIELDS = ('path', 'blob', 'size', 'n_children', 'n_desc', 'mtime', 'error_count')


def manifest_path(blob_url: str) -> str:
    return blob_url + SUFFIX


def scan_manifest(scan) -> dict:
    # `Scan.time` is naive wall-clock time in the writer's zone (`index` stamps
    # `now().astimezone()`, SQLite drops the offset). The manifest crosses
    # machines (a UTC cloud runner writes it), so it carries the offset.
    m = {'format': FORMAT, 'version': VERSION, 'time': scan.time.astimezone().isoformat()}
    for f in FIELDS:
        m[f] = getattr(scan, f)
    # Stored as JSON text on the row; a manifest is JSON already.
    m['error_paths'] = json.loads(scan.error_paths) if scan.error_paths else None
    return m


def write_scan_manifest(scan, blob_url: str) -> str:
    path = manifest_path(blob_url)
    blobfs.write_text(path, json.dumps(scan_manifest(scan), indent=2) + '\n')
    return path
