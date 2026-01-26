import json
from os import listdir, makedirs, remove, stat
from os.path import abspath, dirname, exists, isdir, isfile, join
import shutil
import sqlite3
import subprocess
import threading
import time
import uuid
from datetime import datetime

import pandas as pd
from flask import Flask, jsonify, request, g, Response, send_from_directory
from flask_cors import CORS

from disk_tree.config import SQLITE_PATH
from disk_tree.storage import get_backend

app = Flask(__name__)
CORS(app)

DB_PATH = abspath(SQLITE_PATH)

# Default max_rows for treemap performance
DEFAULT_MAX_ROWS = 2000


def init_db():
    """Initialize the database with required tables if they don't exist."""
    from os.path import dirname
    makedirs(dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute('''
            CREATE TABLE IF NOT EXISTS scan (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                path TEXT NOT NULL,
                time DATETIME NOT NULL,
                blob TEXT NOT NULL,
                error_count INTEGER,
                error_paths TEXT,
                size INTEGER,
                n_children INTEGER,
                n_desc INTEGER
            )
        ''')
        conn.execute('''
            CREATE INDEX IF NOT EXISTS ix_scan_path_time ON scan (path, time)
        ''')
        conn.execute('''
            CREATE TABLE IF NOT EXISTS scan_progress (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                path TEXT NOT NULL UNIQUE,
                pid INTEGER NOT NULL,
                started DATETIME NOT NULL,
                items_found INTEGER NOT NULL DEFAULT 0,
                items_per_sec INTEGER,
                error_count INTEGER NOT NULL DEFAULT 0,
                status TEXT NOT NULL DEFAULT 'running'
            )
        ''')
        conn.commit()
    finally:
        conn.close()


# Initialize database on module load
init_db()


# Static file serving for bundled UI
# Check multiple locations: packaged static/, dev ui/dist/
_this_dir = dirname(abspath(__file__))
_static_candidates = [
    join(_this_dir, 'static'),           # Packaged: disk_tree/static/
    join(_this_dir, '..', '..', 'ui', 'dist'),  # Dev: ../../ui/dist from src/disk_tree/
]
STATIC_DIR = None
for candidate in _static_candidates:
    if exists(join(candidate, 'index.html')):
        STATIC_DIR = abspath(candidate)
        break

# Track in-progress scans: {job_id: {path, status, started, output, error}}
running_scans: dict[str, dict] = {}

# Simple TTL cache for expensive operations
_cache: dict[str, tuple[float, any]] = {}
CACHE_TTL = 60  # seconds

def load_scan_data(
    blob_ref: str,
    max_depth: int | None = None,
    min_depth: int | None = None,
    follow_refs: bool = False,
) -> pd.DataFrame:
    """Load scan data via the storage backend with optional depth filtering."""
    backend = get_backend()
    return backend.load(blob_ref, max_depth=max_depth, min_depth=min_depth, follow_refs=follow_refs)


def resolve_chunk_for_path(blob_ref: str, rel_path: str) -> tuple[str, str]:
    """Resolve the actual blob_ref and rebased rel_path for a path that may be in a chunk.

    If rel_path maps to a chunked subtree, returns (chunk_blob_ref, rebased_path).
    Otherwise returns (blob_ref, rel_path) unchanged.
    """
    if not rel_path or rel_path == '.':
        return blob_ref, rel_path

    # Load the root parquet to check for child_scan_id
    df = pd.read_parquet(blob_ref)
    if 'child_scan_id' not in df.columns:
        return blob_ref, rel_path

    # Check if any ancestor of rel_path has a child_scan_id
    parts = rel_path.split('/')
    for i in range(len(parts)):
        ancestor = '/'.join(parts[:i+1])
        match = df[df['path'] == ancestor]
        if not match.empty:
            row = match.iloc[0]
            if pd.notna(row.get('child_scan_id')):
                # This ancestor is chunked - resolve to the chunk
                chunk_ref = row['child_scan_id']
                if exists(chunk_ref):
                    # Rebase the remaining path relative to chunk root
                    remaining = '/'.join(parts[i+1:]) if i + 1 < len(parts) else '.'
                    # Recursively resolve in case of nested chunks
                    return resolve_chunk_for_path(chunk_ref, remaining)

    return blob_ref, rel_path


def cached(key: str, ttl: int = CACHE_TTL):
    """Decorator for caching expensive function results with TTL."""
    def decorator(fn):
        def wrapper(*args, **kwargs):
            now = time.time()
            if key in _cache:
                cached_time, cached_result = _cache[key]
                if now - cached_time < ttl:
                    return cached_result
            result = fn(*args, **kwargs)
            _cache[key] = (now, result)
            return result
        return wrapper
    return decorator


def invalidate_cache(key: str):
    """Remove a key from the cache."""
    _cache.pop(key, None)


def clear_cache():
    """Clear all cached data."""
    _cache.clear()


def get_db():
    """Get database connection for current request."""
    if 'db' not in g:
        g.db = sqlite3.connect(DB_PATH)
        g.db.row_factory = sqlite3.Row
    return g.db


@app.teardown_appcontext
def close_db(error):
    """Close database connection at end of request."""
    db = g.pop('db', None)
    if db is not None:
        db.close()


def _fetch_scans_data():
    """Fetch all scans with stats from SQLite (denormalized columns)."""
    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row
    try:
        # Get most recent scan for each path with denormalized stats
        cursor = db.execute('''
            SELECT s.*
            FROM scan s
            INNER JOIN (
                SELECT path, MAX(time) as max_time
                FROM scan
                GROUP BY path
            ) latest ON s.path = latest.path AND s.time = latest.max_time
            ORDER BY s.time DESC
        ''')
        result = []
        for row in cursor:
            result.append({
                'id': row['id'],
                'path': row['path'],
                'time': row['time'],
                'blob': row['blob'],
                'error_count': row['error_count'],
                'error_paths': row['error_paths'],
                'size': row['size'],
                'n_children': row['n_children'],
                'n_desc': row['n_desc'],
            })
        return result
    finally:
        db.close()


@app.route('/api/scans')
def get_scans():
    """Return list of most recent scan per path, with root stats."""
    # Check cache first (60 second TTL)
    cache_key = 'scans_list'
    now = time.time()
    if cache_key in _cache:
        cached_time, cached_result = _cache[cache_key]
        if now - cached_time < CACHE_TTL:
            return jsonify(cached_result)

    result = _fetch_scans_data()
    _cache[cache_key] = (now, result)
    return jsonify(result)


def _fetch_s3_buckets_data():
    """Fetch S3 buckets with scan stats (expensive, should be cached)."""
    result = subprocess.run(
        ['aws', 's3', 'ls'],
        capture_output=True,
        text=True,
        timeout=30,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr or 'Failed to list buckets')

    buckets = []
    for line in result.stdout.strip().split('\n'):
        if not line:
            continue
        # Format: "2024-01-01 12:00:00 bucket-name"
        parts = line.split(None, 2)
        if len(parts) >= 3:
            buckets.append({
                'name': parts[2],
                'created': f'{parts[0]} {parts[1]}',
            })

    # Get scan info and stats for each bucket (use denormalized columns)
    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row
    try:
        for bucket in buckets:
            cursor = db.execute(
                'SELECT time, size, n_children, n_desc FROM scan WHERE path = ? OR path LIKE ? ORDER BY time DESC LIMIT 1',
                (f's3://{bucket["name"]}', f's3://{bucket["name"]}/%')
            )
            row = cursor.fetchone()
            bucket['last_scanned'] = row['time'] if row else None
            bucket['size'] = row['size'] if row else None
            bucket['n_children'] = row['n_children'] if row else None
            bucket['n_desc'] = row['n_desc'] if row else None
    finally:
        db.close()

    return buckets


@app.route('/api/s3/buckets')
def list_s3_buckets():
    """Return list of S3 buckets accessible to the current AWS profile."""
    # Check cache first (60 second TTL)
    cache_key = 's3_buckets'
    now = time.time()
    if cache_key in _cache:
        cached_time, cached_result = _cache[cache_key]
        if now - cached_time < CACHE_TTL:
            return jsonify(cached_result)

    try:
        buckets = _fetch_s3_buckets_data()
        _cache[cache_key] = (now, buckets)
        return jsonify(buckets)
    except subprocess.TimeoutExpired:
        return jsonify({'error': 'Timeout listing buckets'}), 504
    except Exception as e:
        return jsonify({'error': str(e)}), 500


def list_fs_children(path: str) -> list[dict]:
    """List filesystem children of a directory with basic stats."""
    if not isdir(path):
        return []

    children = []
    try:
        for name in listdir(path):
            child_path = join(path, name)
            try:
                st = stat(child_path)
                children.append({
                    'path': name,
                    'uri': child_path,
                    'size': st.st_size if isfile(child_path) else None,
                    'mtime': int(st.st_mtime),
                    'kind': 'file' if isfile(child_path) else 'dir',
                    'n_children': None,
                    'n_desc': None,
                    'scanned': False,
                })
            except (OSError, PermissionError):
                continue
    except (OSError, PermissionError):
        pass

    return children


def list_s3_children(s3_uri: str) -> list[dict]:
    """List S3 children of a prefix (non-recursive, immediate children only)."""
    # aws s3 ls s3://bucket/prefix/ returns immediate children
    # Format: "PRE dirname/" for dirs, "2024-01-01 12:00:00 size filename" for files
    try:
        # Ensure trailing slash for prefix listing
        uri = s3_uri.rstrip('/') + '/'
        result = subprocess.run(
            ['aws', 's3', 'ls', uri],
            capture_output=True,
            text=True,
            timeout=30,
        )
        if result.returncode != 0:
            return []

        children = []
        for line in result.stdout.strip().split('\n'):
            if not line:
                continue
            if line.startswith('PRE '):
                # Directory: "PRE dirname/"
                name = line[4:].rstrip('/')
                children.append({
                    'path': name,
                    'uri': f'{s3_uri.rstrip("/")}/{name}',
                    'size': None,
                    'mtime': None,
                    'kind': 'dir',
                    'n_children': None,
                    'n_desc': None,
                    'scanned': False,
                })
            else:
                # File: "2024-01-01 12:00:00 size filename"
                parts = line.split(None, 3)
                if len(parts) >= 4:
                    from dateutil.parser import parse
                    mtime_str = f'{parts[0]} {parts[1]}'
                    try:
                        mtime = int(parse(mtime_str).timestamp())
                    except Exception:
                        mtime = None
                    size = int(parts[2]) if parts[2].isdigit() else None
                    name = parts[3]
                    children.append({
                        'path': name,
                        'uri': f'{s3_uri.rstrip("/")}/{name}',
                        'size': size,
                        'mtime': mtime,
                        'kind': 'file',
                        'n_children': None,
                        'n_desc': None,
                        'scanned': False,
                    })
        return children
    except Exception:
        return []


def get_scanned_paths(db) -> dict[str, dict]:
    """Get a mapping of scanned paths to their most recent scan info with denormalized stats."""
    cursor = db.execute('''
        SELECT s.path, s.time, s.blob, s.size, s.n_children, s.n_desc
        FROM scan s
        INNER JOIN (
            SELECT path, MAX(time) as max_time
            FROM scan
            GROUP BY path
        ) latest ON s.path = latest.path AND s.time = latest.max_time
    ''')
    return {row['path']: dict(row) for row in cursor}


@app.route('/api/scan')
def get_scan():
    """Return scan details for a given URI.

    Query params:
        uri: The path or s3:// URI to look up
        scan_id: Optional specific scan ID to use (for time-travel)
        depth: Max depth of children to return (default 2)
        max_rows: Max rows to return for treemap (default DEFAULT_MAX_ROWS, 0 for unlimited)
        expand_single: Auto-expand single-child directories (default true)
    """
    uri = request.args.get('uri', '/')
    scan_id = request.args.get('scan_id')
    depth = int(request.args.get('depth', 2))
    max_rows = int(request.args.get('max_rows', DEFAULT_MAX_ROWS))
    expand_single = request.args.get('expand_single', 'true').lower() != 'false'

    # Normalize URI
    uri = uri.rstrip('/')
    if not uri:
        uri = '/'

    # Find the best matching scan (exact match or ancestor)
    if uri.startswith('s3://'):
        search_path = uri
    else:
        search_path = uri if uri.startswith('/') else f'/{uri}'

    db = get_db()
    scan = None

    # If scan_id provided, use that specific scan
    if scan_id:
        cursor = db.execute('SELECT * FROM scan WHERE id = ?', (scan_id,))
        row = cursor.fetchone()
        if row:
            scan = dict(row)
            # Verify the scan covers the requested URI
            scan_path = scan['path']
            if not (search_path == scan_path or search_path.startswith(scan_path + '/')):
                return jsonify({'error': f"Scan {scan_id} does not cover path {uri}"}), 400
    else:
        # Find the most recent scan covering this path (exact or ancestor)
        # Collect all candidate scans, then pick the freshest
        candidate_scans = []
        test_path = search_path
        while test_path:
            cursor = db.execute(
                'SELECT * FROM scan WHERE path = ? ORDER BY time DESC LIMIT 1',
                (test_path,)
            )
            row = cursor.fetchone()
            if row:
                candidate_scans.append(dict(row))
            # Go up one directory
            if test_path == '/' or test_path == 's3://':
                break
            parent = dirname(test_path)
            # Avoid infinite loop: if dirname didn't change, we're at root
            if parent == test_path:
                break
            test_path = parent

        # Pick the most recent scan
        if candidate_scans:
            scan = max(candidate_scans, key=lambda s: s['time'])

    if not scan:
        # No ancestor scan - build from filesystem + any child scans
        if not search_path.startswith('s3://'):
            # Local path - list filesystem and merge with scan data
            fs_children = list_fs_children(search_path)

            # Get all scanned paths to check which children have scans
            scanned_paths = get_scanned_paths(db)

            # Find child scans under this path
            prefix = search_path.rstrip('/') + '/'
            scan_data_by_name = {}  # immediate child name -> scan stats

            for scanned_path, scan_info in scanned_paths.items():
                if scanned_path.startswith(prefix):
                    rel = scanned_path[len(prefix):]
                    immediate = rel.split('/')[0]
                    if immediate not in scan_data_by_name:
                        # Use denormalized stats from SQLite - no parquet load needed
                        if scan_info.get('size') is not None:
                            scan_data_by_name[immediate] = {
                                'size': scan_info['size'] or 0,
                                'mtime': 0,  # mtime not denormalized
                                'n_children': scan_info.get('n_children') or 0,
                                'n_desc': scan_info.get('n_desc') or 0,
                                'scan_time': scan_info['time'],
                            }

            # Merge filesystem with scan data
            children_by_name = {}
            for child in fs_children:
                name = child['path']
                child_uri = child['uri']

                # Check if this child has a direct scan
                if child_uri in scanned_paths:
                    scan_info = scanned_paths[child_uri]
                    # Use denormalized stats from SQLite - no parquet load needed
                    if scan_info.get('size') is not None:
                        child['size'] = scan_info['size'] or 0
                        child['mtime'] = 0  # mtime not denormalized
                        child['n_children'] = scan_info.get('n_children') or 0
                        child['n_desc'] = scan_info.get('n_desc') or 0
                        child['scanned'] = True
                        child['scan_time'] = scan_info['time']
                elif name in scan_data_by_name:
                    # Has a descendant scan - use that data
                    data = scan_data_by_name[name]
                    child['size'] = data['size']
                    child['mtime'] = data['mtime']
                    child['n_children'] = data['n_children']
                    child['n_desc'] = data['n_desc']
                    child['scanned'] = 'partial'  # Has child scans but not itself
                    child['scan_time'] = data['scan_time']

                children_by_name[name] = child

            children = list(children_by_name.values())
            # Sort: scanned first (by size desc), then unscanned (by name)
            children.sort(key=lambda x: (
                0 if x.get('scanned') else 1,
                -(x.get('size') or 0),
                x['path'],
            ))

            # Compute virtual root stats from scanned children only
            scanned_children = [c for c in children if c.get('scanned')]
            total_size = sum(c.get('size') or 0 for c in scanned_children)
            max_mtime = max((c.get('mtime') or 0 for c in scanned_children), default=0)
            total_desc = sum(c.get('n_desc') or 0 for c in scanned_children)

            return jsonify({
                'root': {
                    'path': '.',
                    'uri': search_path,
                    'size': total_size if scanned_children else None,
                    'mtime': max_mtime if scanned_children else None,
                    'kind': 'dir',
                    'n_children': len(children),
                    'n_desc': total_desc if scanned_children else None,
                    'parent': None,
                    'scanned': False,
                },
                'children': children,
                'rows': [c for c in children if c.get('scanned')],
                'time': None,
                'scan_path': None,
                'scan_status': 'partial' if scanned_children else 'none',
            })

        # S3 path - list bucket/prefix contents
        if search_path.startswith('s3://'):
            s3_children = list_s3_children(search_path)

            # Get all scanned paths to check which children have scans
            scanned_paths = get_scanned_paths(db)

            # Find child scans under this path
            prefix = search_path.rstrip('/') + '/'
            scan_data_by_name = {}  # immediate child name -> scan stats

            for scanned_path, scan_info in scanned_paths.items():
                if scanned_path.startswith(prefix):
                    rel = scanned_path[len(prefix):]
                    immediate = rel.split('/')[0]
                    if immediate not in scan_data_by_name:
                        # Use denormalized stats from SQLite - no parquet load needed
                        if scan_info.get('size') is not None:
                            scan_data_by_name[immediate] = {
                                'size': scan_info['size'] or 0,
                                'mtime': 0,  # mtime not denormalized
                                'n_children': scan_info.get('n_children') or 0,
                                'n_desc': scan_info.get('n_desc') or 0,
                                'scan_time': scan_info['time'],
                            }

            # Merge S3 listing with scan data
            children_by_name = {}
            for child in s3_children:
                name = child['path']
                child_uri = child['uri']

                # Check if this child has a direct scan
                if child_uri in scanned_paths:
                    scan_info = scanned_paths[child_uri]
                    # Use denormalized stats from SQLite - no parquet load needed
                    if scan_info.get('size') is not None:
                        child['size'] = scan_info['size'] or 0
                        child['mtime'] = 0  # mtime not denormalized
                        child['n_children'] = scan_info.get('n_children') or 0
                        child['n_desc'] = scan_info.get('n_desc') or 0
                        child['scanned'] = True
                        child['scan_time'] = scan_info['time']
                elif name in scan_data_by_name:
                    # Has a descendant scan - use that data
                    data = scan_data_by_name[name]
                    child['size'] = data['size']
                    child['mtime'] = data['mtime']
                    child['n_children'] = data['n_children']
                    child['n_desc'] = data['n_desc']
                    child['scanned'] = 'partial'
                    child['scan_time'] = data['scan_time']

                children_by_name[name] = child

            children = list(children_by_name.values())
            # Sort: scanned first (by size desc), then unscanned (by name)
            children.sort(key=lambda x: (
                0 if x.get('scanned') else 1,
                -(x.get('size') or 0),
                x['path'],
            ))

            # Compute virtual root stats from scanned children only
            scanned_children = [c for c in children if c.get('scanned')]
            total_size = sum(c.get('size') or 0 for c in scanned_children)
            max_mtime = max((c.get('mtime') or 0 for c in scanned_children), default=0)
            total_desc = sum(c.get('n_desc') or 0 for c in scanned_children)

            return jsonify({
                'root': {
                    'path': '.',
                    'uri': search_path,
                    'size': total_size if scanned_children else None,
                    'mtime': max_mtime if scanned_children else None,
                    'kind': 'dir',
                    'n_children': len(children),
                    'n_desc': total_desc if scanned_children else None,
                    'parent': None,
                    'scanned': False,
                },
                'children': children,
                'rows': [c for c in children if c.get('scanned')],
                'time': None,
                'scan_path': None,
                'scan_status': 'partial' if scanned_children else 'none',
            })

        return jsonify({'error': 'No scan found for path', 'uri': uri}), 404

    # Compute relative path from scan root to requested URI
    if scan['path'] == uri:
        relative_path = '.'
    else:
        scan_prefix = scan['path'].rstrip('/') + '/'
        relative_path = uri[len(scan_prefix):] if uri.startswith(scan_prefix) else ''

    # Resolve chunk: if the relative path is inside a chunked subtree, get the chunk blob
    effective_blob, rebased_path = resolve_chunk_for_path(scan['blob'], relative_path)
    is_chunked = effective_blob != scan['blob']

    # Compute max depth for predicate pushdown
    # viewed_path_depth is depth of rebased path (0 for '.', 1 for 'foo', etc.)
    viewed_path_depth = 0 if rebased_path == '.' else rebased_path.count('/') + 1
    max_depth = viewed_path_depth + depth

    # Load parquet with depth filter (only loads rows up to max_depth)
    df = load_scan_data(effective_blob, max_depth)

    # Filter to requested URI prefix
    prefix = uri.rstrip('/') + '/'

    # When viewing from a chunk, paths are already rebased relative to chunk root
    if is_chunked or rebased_path == '.':
        # Paths in df are relative to chunk/scan root
        # rebased_path is '.' for root, or 'subdir' for subdir within chunk
        if rebased_path == '.':
            root_mask = df['path'] == '.'
            children_mask = (df['parent'] == '.') | ((df['parent'] == '') & (df['path'] != '.'))
        else:
            root_mask = df['path'] == rebased_path
            children_prefix = rebased_path + '/'
            children_mask = df['path'].str.startswith(children_prefix)

            # Recompute relative paths for the viewed subdir within chunk
            df = df.copy()
            def make_relative(row):
                if row['path'] == rebased_path:
                    return '.'
                elif row['path'].startswith(children_prefix):
                    return row['path'][len(children_prefix):]
                return row['path']

            def make_relative_parent(row):
                orig_parent = row['parent']
                if orig_parent == rebased_path:
                    return '.'
                parent_prefix = rebased_path + '/'
                if orig_parent.startswith(parent_prefix):
                    return orig_parent[len(parent_prefix):]
                return orig_parent

            df['rel_path'] = df.apply(make_relative, axis=1)
            df['rel_parent'] = df.apply(make_relative_parent, axis=1)
    elif scan['path'] == uri:
        # Exact match - use '.' as root
        root_mask = df['path'] == '.'
        # Direct children have parent='.' (dirs) or parent='' (files)
        children_mask = (df['parent'] == '.') | ((df['parent'] == '') & (df['path'] != '.'))
    else:
        # Scan is an ancestor (non-chunked) - filter by URI
        root_mask = df['uri'] == uri
        children_mask = df['uri'].str.startswith(prefix)

        # Recompute relative paths and parents
        # The viewed dir path relative to scan root (e.g., '.dvc' when viewing s3://bucket/.dvc)
        root_row_for_rel = df[root_mask]
        viewed_dir_path = root_row_for_rel.iloc[0]['path'] if not root_row_for_rel.empty else ''

        def make_relative(row):
            if row['uri'] == uri:
                return '.'
            elif row['uri'].startswith(prefix):
                return row['uri'][len(prefix):]
            return row['path']

        def make_relative_parent(row):
            orig_parent = row['parent']
            # If parent is the viewed dir, it becomes '.'
            if orig_parent == viewed_dir_path:
                return '.'
            # If parent starts with viewed dir + '/', strip prefix
            parent_prefix = viewed_dir_path + '/'
            if orig_parent.startswith(parent_prefix):
                return orig_parent[len(parent_prefix):]
            # Fallback (shouldn't happen for rows under prefix)
            return orig_parent

        df = df.copy()
        df['rel_path'] = df.apply(make_relative, axis=1)
        df['rel_parent'] = df.apply(make_relative_parent, axis=1)
    root_row = df[root_mask]
    if root_row.empty:
        return jsonify({'error': 'URI not found in scan', 'uri': uri, 'scan_path': scan['path']}), 404

    root = root_row.iloc[0].to_dict()
    root['path'] = '.'
    root['parent'] = None

    # Get children up to requested depth
    # When at chunk root or exact scan match, paths in df are already correct (path column)
    # Otherwise, use rel_path column
    if scan['path'] == uri or (is_chunked and rebased_path == '.'):
        # Filter by path depth - paths are already relative to current root
        def get_depth(path):
            if path == '.':
                return 0
            return path.count('/') + 1

        df['depth'] = df['path'].apply(get_depth)
        children_df = df[(df['depth'] > 0) & (df['depth'] <= depth)]
        # Direct children have parent='.' (dirs) or parent='' (files)
        direct_children_df = df[(df['parent'] == '.') | ((df['parent'] == '') & (df['path'] != '.'))]
    else:
        # Filter by relative path depth (subdir within scan or chunk)
        def get_rel_depth(rel_path):
            if rel_path == '.':
                return 0
            return rel_path.count('/') + 1

        df['depth'] = df['rel_path'].apply(get_rel_depth)
        children_df = df[children_mask & (df['depth'] > 0) & (df['depth'] <= depth)]
        direct_children_df = df[children_mask & (df['depth'] == 1)]

    # Convert to list of dicts, handling numpy types
    # use_rel_path: if True, use 'rel_path' column as 'path' (when viewing subdir within scan/chunk)
    # At chunk root (rebased_path == '.'), paths are already correct, no transformation needed
    use_rel_path = 'rel_path' in df.columns

    def row_to_dict(row):
        d = row.to_dict()
        # Convert numpy types to Python types
        for k, v in d.items():
            if hasattr(v, 'item'):
                d[k] = v.item()
        # Use rel_path and rel_parent when viewing a subdir of a scan
        if use_rel_path:
            if 'rel_path' in d:
                d['path'] = d['rel_path']
            if 'rel_parent' in d:
                d['parent'] = d['rel_parent']
        return d

    children = [row_to_dict(row) for _, row in direct_children_df.iterrows()]

    # Load items from child scans for treemap completeness
    # For directories with child_scan_id, load their direct children (depth-1 items)
    # This ensures we show the top-level breakdown of each chunked directory
    if 'child_scan_id' in df.columns:
        child_scan_dfs = []
        for _, row in direct_children_df.iterrows():
            child_scan = row.get('child_scan_id')
            if pd.notna(child_scan) and exists(child_scan):
                try:
                    child_df = pd.read_parquet(child_scan)
                    # Only load direct children (depth=1) from child scans
                    # These become depth=2 in the parent context
                    child_df = child_df[child_df['depth'] == 1]
                    if len(child_df) > 0:
                        # Prefix paths with parent directory name
                        parent_path = row['path'] if not use_rel_path else row.get('rel_path', row['path'])
                        child_df = child_df.copy()
                        child_df['path'] = parent_path + '/' + child_df['path']
                        child_df['parent'] = parent_path  # All become children of this dir
                        # Adjust depth: depth-1 in child becomes depth-2 in parent
                        child_df['depth'] = 2
                        child_scan_dfs.append(child_df)
                except Exception as e:
                    print(f"Error loading child scan {child_scan}: {e}")
        if child_scan_dfs:
            children_df = pd.concat([children_df] + child_scan_dfs, ignore_index=True)

    # Limit rows for treemap performance
    if max_rows > 0 and len(children_df) > max_rows:
        # Sort by size descending and take top N
        sorted_df = children_df.sort_values('size', ascending=False).head(max_rows)
        # Ensure parent chain is included for valid treemap structure
        included_paths = set(sorted_df['path'].tolist())
        parents_to_add = []
        for _, row in sorted_df.iterrows():
            parent = row.get('parent') if use_rel_path else row.get('parent')
            while parent and parent != '.' and parent not in included_paths:
                parent_row = children_df[children_df['path'] == parent]
                if not parent_row.empty:
                    parents_to_add.append(parent_row.iloc[0])
                    included_paths.add(parent)
                    parent = parent_row.iloc[0].get('parent')
                else:
                    break
        if parents_to_add:
            sorted_df = pd.concat([sorted_df, pd.DataFrame(parents_to_add)], ignore_index=True)
        rows = [row_to_dict(row) for _, row in sorted_df.iterrows()]
    else:
        rows = [row_to_dict(row) for _, row in children_df.iterrows()]

    # Mark all children as scanned
    for c in children:
        c['scanned'] = True
        c['scan_time'] = scan['time']

    # Auto-expand single-child directories
    collapsed_rows = []  # List of row dicts for collapsed parent directories
    if expand_single:
        # Keep expanding while there's exactly 1 child that's a directory
        while len(children) == 1 and children[0].get('kind') == 'dir':
            single_child = children[0]
            single_child_path = single_child['path']

            # Find grandchildren (children of the single child)
            # They have parent == single_child_path
            if use_rel_path:
                grandchildren_df = children_df[children_df['rel_parent'] == single_child_path]
            else:
                grandchildren_df = children_df[children_df['parent'] == single_child_path]

            if grandchildren_df.empty:
                # No grandchildren data loaded (need deeper depth), stop expanding
                break

            # Convert grandchildren to list
            grandchildren = [row_to_dict(row) for _, row in grandchildren_df.iterrows()]
            for gc in grandchildren:
                gc['scanned'] = True
                gc['scan_time'] = scan['time']

            # Save full row data for the collapsed parent (before we modify it)
            # Include the original path for URI construction
            collapsed_row = dict(single_child)
            collapsed_row['original_path'] = single_child_path
            collapsed_rows.append(collapsed_row)

            # Update paths: strip the single_child_path prefix from grandchildren paths
            prefix_to_strip = single_child_path + '/'
            for gc in grandchildren:
                if gc['path'].startswith(prefix_to_strip):
                    gc['path'] = gc['path'][len(prefix_to_strip):]
                if gc.get('parent') == single_child_path:
                    gc['parent'] = '.'
                elif gc.get('parent', '').startswith(prefix_to_strip):
                    gc['parent'] = gc['parent'][len(prefix_to_strip):]

            # Update root to be the single child
            root = single_child
            root['path'] = '.'
            root['parent'] = None

            # Grandchildren become the new children
            children = grandchildren

            # Also update rows for treemap - strip prefix and filter out the collapsed path
            # (which becomes root, added separately by frontend)
            new_rows = []
            for r in rows:
                if r['path'] == single_child_path:
                    # Skip - this becomes root, which is added separately
                    continue
                if r['path'].startswith(prefix_to_strip):
                    r['path'] = r['path'][len(prefix_to_strip):]
                if r.get('parent') == single_child_path:
                    r['parent'] = '.'
                elif r.get('parent', '').startswith(prefix_to_strip):
                    r['parent'] = r['parent'][len(prefix_to_strip):]
                new_rows.append(r)
            rows = new_rows

    # Patch in fresher child scans: find direct child paths with newer scans
    scan_time = scan['time']
    child_uri_prefix = uri.rstrip('/') + '/'
    db = get_db()
    # Query for scans of direct children that are newer than parent scan
    # Direct children: path starts with parent prefix, no additional slashes
    fresher_scans = db.execute('''
        SELECT s1.*
        FROM scan s1
        INNER JOIN (
            SELECT path, MAX(time) as max_time
            FROM scan
            WHERE path LIKE ? ESCAPE '\\'
              AND path NOT LIKE ? ESCAPE '\\'
              AND time > ?
            GROUP BY path
        ) s2 ON s1.path = s2.path AND s1.time = s2.max_time
    ''', (
        child_uri_prefix.replace('%', '\\%').replace('_', '\\_') + '%',  # children
        child_uri_prefix.replace('%', '\\%').replace('_', '\\_') + '%/%',  # exclude grandchildren+
        scan_time,
    )).fetchall()

    # Build a map of child path -> fresher scan stats (using denormalized SQLite columns)
    fresher_stats = {}
    for child_scan in fresher_scans:
        # Use denormalized stats from SQLite - no need to load parquet
        if child_scan['size'] is not None:
            fresher_stats[child_scan['path']] = {
                'size': child_scan['size'],
                'mtime': None,  # mtime not denormalized, but not critical for patching
                'n_desc': child_scan['n_desc'],
                'n_children': child_scan['n_children'],
                'scan_time': child_scan['time'],
            }

    # Patch fresher stats into children
    for c in children:
        child_path = c.get('uri', '')
        if child_path in fresher_stats:
            stats = fresher_stats[child_path]
            c['size'] = stats['size']
            c['mtime'] = stats['mtime']
            c['n_desc'] = stats['n_desc']
            c['n_children'] = stats['n_children']
            c['scan_time'] = stats['scan_time']
            c['patched'] = True  # Mark that this was patched from a fresher scan

    # Add expand_preview for child directories that would auto-expand
    # This shows users what path they'll land on when clicking (e.g., "vms" shows "vms / 0")
    if expand_single:
        for c in children:
            if c.get('kind') != 'dir' or c.get('n_children') != 1:
                continue
            # Build the expand preview chain
            preview_parts = []
            current_path = c['path']
            while True:
                # Find children of current_path
                if use_rel_path:
                    sub_df = children_df[children_df['rel_parent'] == current_path]
                else:
                    sub_df = children_df[children_df['parent'] == current_path]
                if len(sub_df) != 1:
                    break
                sub_row = sub_df.iloc[0]
                if sub_row.get('kind') != 'dir':
                    break
                # Add to preview
                sub_path = sub_row['rel_path'] if use_rel_path else sub_row['path']
                sub_name = sub_path.split('/')[-1]
                preview_parts.append(sub_name)
                # Check if this also has exactly 1 child
                if sub_row.get('n_children') != 1:
                    break
                current_path = sub_path
            if preview_parts:
                c['expand_preview'] = '/'.join(preview_parts)

    return jsonify({
        'root': root,
        'children': sorted(children, key=lambda x: -x.get('size', 0)),
        'rows': rows,
        'time': scan['time'],
        'scan_path': scan['path'],
        'scan_status': 'full',
        'error_count': scan.get('error_count'),
        'error_paths': scan.get('error_paths'),
        'collapsed_rows': collapsed_rows if collapsed_rows else None,
    })


@app.route('/api/scans/history')
def get_scan_history():
    """Return all scans for a given path, including ancestor scans.

    Query params:
        uri: The path to look up

    Returns scans of the exact path AND scans of ancestor paths that contain
    data for the requested URI. Each scan includes 'scan_path' to indicate
    which path was actually scanned.
    """
    uri = request.args.get('uri', '/')
    uri = uri.rstrip('/')
    if not uri:
        uri = '/'

    db = get_db()

    # Build list of paths to check: the exact path and all ancestors
    paths_to_check = [uri]
    test_path = uri
    while test_path and test_path != '/':
        parent = dirname(test_path)
        if parent == test_path:
            break
        if parent:
            paths_to_check.append(parent)
        test_path = parent
    # Include root for local paths
    if not uri.startswith('s3://') and '/' not in paths_to_check:
        paths_to_check.append('/')

    # Query for scans of any of these paths
    placeholders = ','.join('?' * len(paths_to_check))
    scans = db.execute(
        f'SELECT id, path, time, size, n_children, n_desc FROM scan WHERE path IN ({placeholders}) ORDER BY time DESC',
        paths_to_check
    ).fetchall()

    # For ancestor scans, we need to get the stats for the specific subpath from the parquet
    results = []
    for s in scans:
        scan_dict = dict(s)
        scan_path = scan_dict['path']

        if scan_path == uri:
            # Exact match - use denormalized stats directly
            scan_dict['scan_path'] = scan_path
            results.append(scan_dict)
        else:
            # Ancestor scan - need to extract stats for the subpath from parquet
            # Look up the blob path
            blob_row = db.execute('SELECT blob FROM scan WHERE id = ?', (scan_dict['id'],)).fetchone()
            if blob_row:
                try:
                    # The path in parquet is relative to scan root
                    rel_path = uri[len(scan_path):].lstrip('/')
                    # Calculate depth of target path (e.g., 'Library' = depth 1, 'Library/Caches' = depth 2)
                    target_depth = rel_path.count('/') + 1
                    # Use depth filtering to only load rows up to target depth (much faster)
                    df = load_scan_data(blob_row['blob'], target_depth)
                    row = df[df['path'] == rel_path]
                    if not row.empty:
                        r = row.iloc[0]
                        scan_dict['size'] = int(r['size']) if pd.notna(r['size']) else None
                        scan_dict['n_children'] = int(r['n_children']) if pd.notna(r.get('n_children')) else None
                        scan_dict['n_desc'] = int(r['n_desc']) if pd.notna(r.get('n_desc')) else None
                        scan_dict['scan_path'] = scan_path
                        results.append(scan_dict)
                except Exception:
                    # Skip this scan if we can't load it
                    pass

    return jsonify(results)


@app.route('/api/compare')
def compare_scans():
    """Compare two scans of the same path.

    Query params:
        uri: The path to compare
        scan1: ID of first (older) scan
        scan2: ID of second (newer) scan
        depth: Max depth of children to compare (default 1)

    Scans can be of the exact URI or ancestor paths. When comparing ancestor
    scans, we extract the relevant subtree for the requested URI.
    """
    uri = request.args.get('uri', '/')
    scan1_id = request.args.get('scan1')
    scan2_id = request.args.get('scan2')
    depth = int(request.args.get('depth', 1))

    if not scan1_id or not scan2_id:
        return jsonify({'error': 'scan1 and scan2 parameters required'}), 400

    # Check response cache (compare results are expensive to compute)
    cache_key = f"compare:{uri}:{scan1_id}:{scan2_id}:{depth}"
    now = time.time()
    if cache_key in _cache:
        cached_time, cached_result = _cache[cache_key]
        if now - cached_time < CACHE_TTL:
            return jsonify(cached_result)

    db = get_db()

    # Load both scans
    scan1 = db.execute('SELECT * FROM scan WHERE id = ?', (scan1_id,)).fetchone()
    scan2 = db.execute('SELECT * FROM scan WHERE id = ?', (scan2_id,)).fetchone()

    if not scan1 or not scan2:
        return jsonify({'error': 'Scan not found'}), 404

    # Filter to direct children of the requested URI
    uri = uri.rstrip('/')
    if not uri:
        uri = '/'

    def get_children(scan, depth_limit):
        """Get direct children of the target URI from a scan.

        Handles both exact match scans and ancestor scans.
        """
        scan_path = scan['path']

        # Calculate depth offset for ancestor scans
        if scan_path == uri:
            rel_prefix = ''
            depth_offset = 0
        else:
            # URI is subdir of scan - calculate relative path
            rel_prefix = uri[len(scan_path):].lstrip('/')
            depth_offset = rel_prefix.count('/') + 1

        # Load with EXACT depth filter - we only need children at depth_offset + 1
        # This avoids loading millions of rows at other depths
        target_depth = depth_offset + depth_limit
        df = load_scan_data(scan['blob'], max_depth=target_depth, min_depth=target_depth)

        if scan_path == uri:
            # Direct match - children have parent='.'
            children = df[df['parent'] == '.'].copy()
            children['rel_path'] = children['path']
        else:
            # URI is subdir of scan - filter to children of the relative path
            # Children have parent == rel_prefix
            children = df[df['parent'] == rel_prefix].copy()
            # rel_path is just the filename (last component)
            children['rel_path'] = children['path'].str.split('/').str[-1]

        return children

    children1 = get_children(scan1, depth)
    children2 = get_children(scan2, depth)

    # Build comparison using rel_path (the child name relative to the target URI)
    paths1 = set(children1['rel_path'].tolist()) if 'rel_path' in children1.columns and len(children1) > 0 else set()
    paths2 = set(children2['rel_path'].tolist()) if 'rel_path' in children2.columns and len(children2) > 0 else set()

    added = paths2 - paths1
    removed = paths1 - paths2
    common = paths1 & paths2

    # Helper to convert numpy types to native Python types
    def to_native(v):
        if v is None:
            return None
        if hasattr(v, 'item'):
            return v.item()
        return v

    def row_to_dict(row):
        d = row.to_dict()
        for k, v in d.items():
            d[k] = to_native(v)
        return d

    # Build URI prefix for drill-down links
    uri_prefix = uri.rstrip('/') + '/'

    results = []

    # Added rows
    for rel_path in added:
        row = children2[children2['rel_path'] == rel_path].iloc[0]
        d = row_to_dict(row)
        d['path'] = rel_path  # Use rel_path as display path
        d['uri'] = uri_prefix + rel_path  # Build full URI for linking
        d['status'] = 'added'
        d['size_delta'] = to_native(d.get('size', 0) or 0)
        results.append(d)

    # Removed rows
    for rel_path in removed:
        row = children1[children1['rel_path'] == rel_path].iloc[0]
        d = row_to_dict(row)
        d['path'] = rel_path  # Use rel_path as display path
        d['uri'] = uri_prefix + rel_path  # Build full URI for linking
        d['status'] = 'removed'
        d['size_delta'] = -to_native(d.get('size', 0) or 0)
        results.append(d)

    # Changed rows
    for rel_path in common:
        row1 = children1[children1['rel_path'] == rel_path].iloc[0]
        row2 = children2[children2['rel_path'] == rel_path].iloc[0]
        d = row_to_dict(row2)
        d['path'] = rel_path  # Use rel_path as display path
        d['uri'] = uri_prefix + rel_path  # Build full URI for linking

        # Compute deltas
        size1 = to_native(row1.get('size', 0) or 0)
        size2 = to_native(row2.get('size', 0) or 0)
        d['size_delta'] = size2 - size1
        d['size_old'] = size1

        n_desc1 = to_native(row1.get('n_desc', 0) or 0)
        n_desc2 = to_native(row2.get('n_desc', 0) or 0)
        d['n_desc_delta'] = n_desc2 - n_desc1
        d['n_desc_old'] = n_desc1

        if d['size_delta'] != 0 or d['n_desc_delta'] != 0:
            d['status'] = 'changed'
        else:
            d['status'] = 'unchanged'

        results.append(d)

    # Sort by absolute size delta
    results.sort(key=lambda x: abs(x.get('size_delta', 0)), reverse=True)

    # Compute totals
    total_delta = sum(r.get('size_delta', 0) for r in results)

    # Get the subtree stats if comparing ancestor scans
    def get_subtree_stats(scan):
        scan_path = scan['path']
        if scan_path == uri:
            return {
                'size': to_native(scan['size']),
                'n_desc': to_native(scan['n_desc']) if 'n_desc' in scan.keys() else None,
            }
        # Load the parquet with depth filtering and find the row for the target URI
        rel_path = uri[len(scan_path):].lstrip('/')
        target_depth = rel_path.count('/') + 1
        df = load_scan_data(scan['blob'], max_depth=target_depth, min_depth=target_depth)
        row = df[df['path'] == rel_path]
        if not row.empty:
            r = row.iloc[0]
            return {
                'size': to_native(r['size']),
                'n_desc': to_native(r.get('n_desc')),
            }
        return {'size': None, 'n_desc': None}

    stats1 = get_subtree_stats(scan1)
    stats2 = get_subtree_stats(scan2)

    response = {
        'uri': uri,
        'scan1': {
            'id': to_native(scan1['id']),
            'time': scan1['time'],
            'size': stats1['size'],
            'n_desc': stats1['n_desc'],
            'scan_path': scan1['path'],
        },
        'scan2': {
            'id': to_native(scan2['id']),
            'time': scan2['time'],
            'size': stats2['size'],
            'n_desc': stats2['n_desc'],
            'scan_path': scan2['path'],
        },
        'rows': results,
        'summary': {
            'added': len(added),
            'removed': len(removed),
            'changed': len([r for r in results if r['status'] == 'changed']),
            'unchanged': len([r for r in results if r['status'] == 'unchanged']),
            'total_delta': to_native(total_delta),
        }
    }

    # Cache the response
    _cache[cache_key] = (time.time(), response)

    return jsonify(response)


def run_scan_job(job_id: str, path: str, force: bool = True):
    """Run disk-tree index in background and update job status."""
    try:
        running_scans[job_id]['status'] = 'running'
        cmd = ['disk-tree', 'index']
        if force:
            cmd.append('-C')  # Force fresh scan, don't use cache
        cmd.append(path)
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
        )
        running_scans[job_id]['output'] = result.stdout
        running_scans[job_id]['error'] = result.stderr
        running_scans[job_id]['status'] = 'completed' if result.returncode == 0 else 'failed'
        running_scans[job_id]['finished'] = datetime.now().isoformat()
    except Exception as e:
        running_scans[job_id]['status'] = 'failed'
        running_scans[job_id]['error'] = str(e)
        running_scans[job_id]['finished'] = datetime.now().isoformat()


@app.route('/api/scan/start', methods=['POST'])
def start_scan():
    """Start a new scan for a path.

    JSON body:
        path: The path or s3:// URI to scan
    """
    data = request.get_json() or {}
    path = data.get('path', '/')

    # Check if already scanning this path
    for job_id, job in running_scans.items():
        if job['path'] == path and job['status'] == 'running':
            return jsonify({'error': 'Scan already in progress', 'job_id': job_id}), 409

    job_id = str(uuid.uuid4())[:8]
    running_scans[job_id] = {
        'path': path,
        'status': 'pending',
        'started': datetime.now().isoformat(),
        'output': '',
        'error': '',
    }

    thread = threading.Thread(target=run_scan_job, args=(job_id, path))
    thread.daemon = True
    thread.start()

    return jsonify({'job_id': job_id, 'path': path, 'status': 'pending'})


@app.route('/api/scan/status/<job_id>')
def scan_status(job_id: str):
    """Get status of a scan job."""
    if job_id not in running_scans:
        return jsonify({'error': 'Job not found'}), 404
    return jsonify({'job_id': job_id, **running_scans[job_id]})


@app.route('/api/scans/running')
def running_scans_list():
    """Get list of all running/recent scans."""
    return jsonify([
        {'job_id': job_id, **job}
        for job_id, job in running_scans.items()
    ])


@app.route('/api/scans/progress')
def get_scans_progress():
    """Get current progress of all active scans (one-shot, no SSE)."""
    db = get_db()
    try:
        cursor = db.execute('SELECT * FROM scan_progress')
        result = []
        for row in cursor:
            result.append({
                'id': row['id'],
                'path': row['path'],
                'pid': row['pid'],
                'started': row['started'],
                'items_found': row['items_found'],
                'items_per_sec': row['items_per_sec'],
                'error_count': row['error_count'],
                'status': row['status'],
            })
        return jsonify(result)
    except sqlite3.OperationalError:
        # Table might not exist yet
        return jsonify([])


@app.route('/api/scans/progress/stream')
def stream_scans_progress():
    """Stream scan progress updates via Server-Sent Events."""
    def generate():
        last_data = None
        while True:
            # Create a new connection for each poll (can't share across threads)
            conn = sqlite3.connect(DB_PATH)
            conn.row_factory = sqlite3.Row
            try:
                cursor = conn.execute('SELECT * FROM scan_progress')
                rows = []
                for row in cursor:
                    rows.append({
                        'id': row['id'],
                        'path': row['path'],
                        'pid': row['pid'],
                        'started': row['started'],
                        'items_found': row['items_found'],
                        'items_per_sec': row['items_per_sec'],
                        'error_count': row['error_count'],
                        'status': row['status'],
                    })
                data = json.dumps(rows)
                # Only send if data changed
                if data != last_data:
                    yield f"data: {data}\n\n"
                    last_data = data
            except sqlite3.OperationalError:
                # Table might not exist yet
                yield f"data: []\n\n"
            finally:
                conn.close()

            time.sleep(1)  # Poll every second

    return Response(
        generate(),
        mimetype='text/event-stream',
        headers={
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'X-Accel-Buffering': 'no',  # Disable nginx buffering
        },
    )


def hexdump(data: bytes, offset: int = 0) -> str:
    """Generate hexdump -C style output."""
    lines = []
    for i in range(0, len(data), 16):
        chunk = data[i:i+16]
        hex_part = ' '.join(f'{b:02x}' for b in chunk[:8])
        if len(chunk) > 8:
            hex_part += '  ' + ' '.join(f'{b:02x}' for b in chunk[8:])
        # Pad hex part to fixed width
        hex_part = hex_part.ljust(49)
        # ASCII part: printable chars or '.'
        ascii_part = ''.join(chr(b) if 32 <= b < 127 else '.' for b in chunk)
        lines.append(f'{offset + i:08x}  {hex_part} |{ascii_part}|')
    return '\n'.join(lines)


# Preview limits
TEXT_PREVIEW_MAX = 64 * 1024      # 64KB for text
HEX_PREVIEW_MAX = 2 * 1024        # 2KB for hex (128 lines)
PREVIEW_ABSOLUTE_MAX = 1024 * 1024  # 1MB hard cap


@app.route('/api/file/preview')
def file_preview():
    """Return preview of a file's contents.

    Query params:
        path: Absolute path to the file
        max_size: Max bytes to read for text (default 64KB, max 1MB)
        hex_size: Max bytes for hex dump (default 2KB, max 16KB)
    """
    path = request.args.get('path')
    max_size = min(int(request.args.get('max_size', TEXT_PREVIEW_MAX)), PREVIEW_ABSOLUTE_MAX)
    hex_size = min(int(request.args.get('hex_size', HEX_PREVIEW_MAX)), 16 * 1024)

    if not path:
        return jsonify({'error': 'Path is required'}), 400

    if not path.startswith('/'):
        return jsonify({'error': 'Path must be absolute'}), 400

    if not isfile(path):
        return jsonify({'error': 'File not found'}), 404

    try:
        file_size = stat(path).st_size

        # Check if file is likely text
        text_extensions = {'.txt', '.log', '.json', '.xml', '.html', '.css', '.js', '.ts',
                          '.py', '.md', '.yaml', '.yml', '.toml', '.ini', '.cfg', '.conf',
                          '.sh', '.bash', '.zsh', '.csv', '.tsv', '.sql', '.env', '.gitignore'}
        ext = '.' + path.rsplit('.', 1)[-1].lower() if '.' in path else ''

        # Read enough to determine type and provide preview
        # For text detection, read up to max_size; for hex we'll truncate later
        read_size = max(max_size, hex_size)
        with open(path, 'rb') as f:
            raw = f.read(read_size)

        # Try to decode as text
        try:
            content = raw.decode('utf-8')
            is_text = True
        except UnicodeDecodeError:
            try:
                content = raw.decode('latin-1')
                is_text = True
            except:
                is_text = False
                content = None

        # Check for binary content (null bytes, etc.)
        if is_text and '\x00' in content:
            is_text = False
            content = None

        # Apply appropriate limits
        if is_text:
            text_truncated = len(content) > max_size or file_size > max_size
            content = content[:max_size] if content else None
            hex_content = None
            hex_truncated = False
        else:
            content = None
            text_truncated = False
            # Generate hex dump with smaller limit
            hex_raw = raw[:hex_size]
            hex_content = hexdump(hex_raw)
            hex_truncated = file_size > hex_size

        return jsonify({
            'path': path,
            'size': file_size,
            'truncated': text_truncated,
            'hex_truncated': hex_truncated,
            'preview_bytes': hex_size if not is_text else max_size,
            'is_text': is_text,
            'content': content,
            'hex': hex_content,
            'extension': ext,
        })

    except PermissionError:
        return jsonify({'error': 'Permission denied'}), 403
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/delete', methods=['POST'])
def delete_path():
    """Delete a file or directory.

    JSON body:
        path: The absolute path to delete
    """
    data = request.get_json() or {}
    path = data.get('path')

    if not path:
        return jsonify({'error': 'Path is required'}), 400

    # Security: only allow deleting within user's home directory or specific paths
    # For now, require absolute paths
    if not path.startswith('/'):
        return jsonify({'error': 'Path must be absolute'}), 400

    # Check if file/dir exists on disk
    path_exists = isfile(path) or isdir(path)

    # Get file size (fast stat call); skip for directories
    deleted_size = None
    if isfile(path):
        try:
            deleted_size = stat(path).st_size
        except (OSError, PermissionError):
            pass

    # Perform the deletion if path exists
    if path_exists:
        try:
            if isfile(path):
                remove(path)
            else:
                shutil.rmtree(path)
        except (OSError, PermissionError) as e:
            return jsonify({'error': f'Failed to delete: {e}'}), 500

    # Update the most recent scan that covers this path
    deleted_n_desc = None
    backend = get_backend()
    db = get_db()

    # Find all scans covering this path, then pick the most recent
    candidate_scans = []
    test_path = path
    while test_path and test_path != '/':
        cursor = db.execute(
            'SELECT id, path, blob, time FROM scan WHERE path = ? ORDER BY time DESC LIMIT 1',
            (test_path,)
        )
        row = cursor.fetchone()
        if row:
            candidate_scans.append(dict(row))
        parent = dirname(test_path)
        if parent == test_path:
            break
        test_path = parent

    # Pick the most recent scan
    covering_scan = max(candidate_scans, key=lambda s: s['time']) if candidate_scans else None

    if covering_scan:
        scan_path = covering_scan['path']
        blob_ref = covering_scan['blob']
        rel_path = path[len(scan_path):].lstrip('/') if path != scan_path else '.'

        if backend.supports_updates:
            # DuckDB/SQLite: efficient in-place update
            stats = backend.delete_path(blob_ref, rel_path)
            if stats:
                deleted_size = stats.size
                deleted_n_desc = stats.n_desc
                # Update denormalized stats in SQLite scan metadata
                root_stats = backend.get_path_stats(blob_ref, '.')
                if root_stats:
                    db.execute('''
                        UPDATE scan SET size = ?, n_children = ?, n_desc = ?
                        WHERE id = ?
                    ''', (root_stats.size, root_stats.n_children, root_stats.n_desc, covering_scan['id']))
                    db.commit()
        else:
            # Parquet: need to rewrite the file (expensive but only for most recent scan)
            try:
                df = backend.load(blob_ref)
                deleted_mask = (df['path'] == rel_path) | df['path'].str.startswith(rel_path + '/')
                if deleted_mask.any():
                    deleted_rows = df[deleted_mask]
                    target_row = deleted_rows[deleted_rows['path'] == rel_path]
                    if not target_row.empty:
                        deleted_size = int(target_row.iloc[0]['size'])
                        deleted_n_desc = int(target_row.iloc[0].get('n_desc', 1) or 1)

                    # Remove deleted rows and update ancestors
                    df = df[~deleted_mask].copy()

                    # Update ancestor stats
                    parts = rel_path.split('/') if rel_path != '.' else []
                    ancestors = ['.']
                    for i in range(1, len(parts)):
                        ancestors.append('/'.join(parts[:i]))

                    for ancestor in ancestors:
                        mask = df['path'] == ancestor
                        if mask.any():
                            df.loc[mask, 'size'] = df.loc[mask, 'size'] - (deleted_size or 0)
                            df.loc[mask, 'n_desc'] = df.loc[mask, 'n_desc'] - (deleted_n_desc or 0)
                            # n_children only for direct parent
                            if ancestor == ('/'.join(parts[:-1]) if len(parts) > 1 else '.'):
                                df.loc[mask, 'n_children'] = df.loc[mask, 'n_children'] - 1

                    # Rewrite parquet (this is the expensive part)
                    df.to_parquet(blob_ref, index=False)

                    # Update denormalized stats in SQLite scan metadata
                    root_row = df[df['path'] == '.']
                    if not root_row.empty:
                        r = root_row.iloc[0]
                        db.execute('''
                            UPDATE scan SET size = ?, n_children = ?, n_desc = ?
                            WHERE id = ?
                        ''', (
                            int(r['size']) if pd.notna(r['size']) else None,
                            int(r['n_children']) if pd.notna(r.get('n_children')) else None,
                            int(r['n_desc']) if pd.notna(r.get('n_desc')) else None,
                            covering_scan['id'],
                        ))
                        db.commit()
            except Exception as e:
                print(f"Warning: Failed to update scan after delete: {e}")

    # Invalidate caches so next request gets fresh data
    _cache.clear()
    if hasattr(backend, 'clear_cache'):
        backend.clear_cache()

    return jsonify({
        'success': True,
        'path': path,
        'deleted_size': deleted_size,
        'deleted_n_desc': deleted_n_desc,
        'already_deleted': not path_exists,
    })


# Static file serving routes (if UI is bundled)
if STATIC_DIR:
    @app.route('/')
    def serve_index():
        """Serve the SPA index.html."""
        return send_from_directory(STATIC_DIR, 'index.html')

    @app.route('/assets/<path:filename>')
    def serve_assets(filename):
        """Serve static assets (JS, CSS, fonts, images)."""
        return send_from_directory(join(STATIC_DIR, 'assets'), filename)

    @app.route('/<path:path>')
    def serve_spa_routes(path):
        """SPA catch-all: serve index.html for client-side routes.

        Non-API routes that don't match a static file get index.html,
        allowing React Router to handle them.
        """
        # Check if it's a static file that exists
        static_path = join(STATIC_DIR, path)
        if exists(static_path) and isfile(static_path):
            return send_from_directory(STATIC_DIR, path)
        # Otherwise serve index.html for SPA routing
        return send_from_directory(STATIC_DIR, 'index.html')


def main():
    if STATIC_DIR:
        print(f"Serving UI from: {STATIC_DIR}")
    else:
        print("No UI found. Run 'cd ui && pnpm build' to build the UI.")
    app.run(debug=True, port=5001)


if __name__ == '__main__':
    main()
