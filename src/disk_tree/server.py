import json
from os import listdir, remove, stat
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

app = Flask(__name__)
CORS(app)

DB_PATH = abspath(SQLITE_PATH)

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

import pyarrow.parquet as pq


def load_parquet_with_depth_filter(blob_path: str, max_depth: int | None = None) -> pd.DataFrame:
    """Load parquet file, optionally filtering by depth column.

    If max_depth is provided and the parquet has a 'depth' column, uses
    predicate pushdown to only load rows where depth <= max_depth.
    Falls back to full load for old parquets without depth column.
    """
    if max_depth is not None:
        # Check if parquet has 'depth' column
        try:
            schema = pq.read_schema(blob_path)
            if 'depth' in schema.names:
                # Use predicate pushdown - only load rows with depth <= max_depth
                return pd.read_parquet(blob_path, filters=[('depth', '<=', max_depth)])
        except Exception:
            pass  # Fall back to full load
    # Full load (no depth column or no filter requested)
    return pd.read_parquet(blob_path)


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
        depth: Max depth of children to return (default 2)
    """
    uri = request.args.get('uri', '/')
    depth = int(request.args.get('depth', 2))

    # Normalize URI
    uri = uri.rstrip('/')
    if not uri:
        uri = '/'

    # Find the best matching scan (exact match or ancestor)
    if uri.startswith('s3://'):
        search_path = uri
    else:
        search_path = uri if uri.startswith('/') else f'/{uri}'

    # Try exact match first, then ancestors
    db = get_db()
    scan = None
    test_path = search_path
    while test_path:
        cursor = db.execute(
            'SELECT * FROM scan WHERE path = ? ORDER BY time DESC LIMIT 1',
            (test_path,)
        )
        row = cursor.fetchone()
        if row:
            scan = dict(row)
            break
        # Go up one directory
        if test_path == '/' or test_path == 's3://':
            break
        parent = dirname(test_path)
        # Avoid infinite loop: if dirname didn't change, we're at root
        if parent == test_path:
            break
        test_path = parent

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

    # Compute max depth for predicate pushdown
    # Exact match: viewed_path_depth = 0, so max_depth = depth
    # Ancestor: viewed_path_depth = depth of relative path from scan root
    if scan['path'] == uri:
        max_depth = depth
    else:
        # Compute depth of viewed path relative to scan root
        # e.g., scan='/home', uri='/home/foo/bar' → relative='foo/bar' → depth=2
        scan_prefix = scan['path'].rstrip('/') + '/'
        relative_path = uri[len(scan_prefix):] if uri.startswith(scan_prefix) else ''
        viewed_path_depth = relative_path.count('/') + 1 if relative_path else 0
        max_depth = viewed_path_depth + depth

    # Load parquet with depth filter (only loads rows up to max_depth)
    df = load_parquet_with_depth_filter(scan['blob'], max_depth)

    # Filter to requested URI prefix
    prefix = uri.rstrip('/') + '/'
    if scan['path'] == uri:
        # Exact match - use '.' as root
        root_mask = df['path'] == '.'
        # Direct children have parent='.' (dirs) or parent='' (files)
        children_mask = (df['parent'] == '.') | ((df['parent'] == '') & (df['path'] != '.'))
    else:
        # Scan is an ancestor - filter by URI
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
    if scan['path'] == uri:
        # Filter by path depth
        def get_depth(path):
            if path == '.':
                return 0
            return path.count('/') + 1

        df['depth'] = df['path'].apply(get_depth)
        children_df = df[(df['depth'] > 0) & (df['depth'] <= depth)]
        # Direct children have parent='.' (dirs) or parent='' (files)
        direct_children_df = df[(df['parent'] == '.') | ((df['parent'] == '') & (df['path'] != '.'))]
    else:
        # Filter by relative path depth
        def get_rel_depth(rel_path):
            if rel_path == '.':
                return 0
            return rel_path.count('/') + 1

        df['depth'] = df['rel_path'].apply(get_rel_depth)
        children_df = df[children_mask & (df['depth'] > 0) & (df['depth'] <= depth)]
        direct_children_df = df[children_mask & (df['depth'] == 1)]

    # Convert to list of dicts, handling numpy types
    # use_rel_path: if True, use 'rel_path' column as 'path' (when scan is ancestor)
    use_rel_path = scan['path'] != uri

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
    rows = [row_to_dict(row) for _, row in children_df.iterrows()]

    # Mark all children as scanned
    for c in children:
        c['scanned'] = True
        c['scan_time'] = scan['time']

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

    return jsonify({
        'root': root,
        'children': sorted(children, key=lambda x: -x.get('size', 0)),
        'rows': rows,
        'time': scan['time'],
        'scan_path': scan['path'],
        'scan_status': 'full',
        'error_count': scan.get('error_count'),
        'error_paths': scan.get('error_paths'),
    })


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


def update_parent_scans_after_delete(deleted_path: str, deleted_size: int, deleted_n_desc: int):
    """Update all ancestor scans to subtract the deleted item's size and descendants."""
    db = get_db()

    # Find all scans that are ancestors of the deleted path
    cursor = db.execute('SELECT id, path, blob FROM scan')
    for row in cursor:
        scan_path = row['path']
        # Check if this scan is an ancestor of the deleted path
        if deleted_path.startswith(scan_path + '/') or deleted_path == scan_path:
            blob_path = row['blob']
            try:
                df = pd.read_parquet(blob_path)

                # Calculate the relative path within this scan
                if scan_path == deleted_path:
                    rel_path = '.'
                else:
                    rel_path = deleted_path[len(scan_path) + 1:]

                # Find the row for the deleted item and all its ancestors
                # First, mark the deleted item's row (and descendants) for removal
                deleted_mask = (df['path'] == rel_path) | df['path'].str.startswith(rel_path + '/')

                if not deleted_mask.any():
                    continue

                # Get the deleted rows to calculate totals
                deleted_rows = df[deleted_mask]
                total_deleted_size = deleted_rows[deleted_rows['path'] == rel_path]['size'].sum()
                total_deleted_n_desc = len(deleted_rows)

                # Remove the deleted rows
                df = df[~deleted_mask].copy()

                # Update all ancestor directories
                path_parts = rel_path.split('/')
                for i in range(len(path_parts)):
                    if i == 0:
                        ancestor_path = '.'
                    else:
                        ancestor_path = '/'.join(path_parts[:i])
                        if not ancestor_path:
                            ancestor_path = '.'

                    ancestor_mask = df['path'] == ancestor_path
                    if ancestor_mask.any():
                        df.loc[ancestor_mask, 'size'] -= total_deleted_size
                        df.loc[ancestor_mask, 'n_desc'] -= total_deleted_n_desc
                        # Decrement n_children only for direct parent
                        if i == len(path_parts) - 1 or (i == 0 and '/' not in rel_path):
                            df.loc[ancestor_mask, 'n_children'] -= 1

                # Also update the root '.' entry
                root_mask = df['path'] == '.'
                if root_mask.any() and rel_path != '.':
                    # Size and n_desc already updated if '.' was in ancestors
                    pass

                # Save updated parquet
                df.to_parquet(blob_path, index=False)

            except Exception as e:
                print(f"Error updating scan {scan_path}: {e}")
                continue


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
    # For now, require absolute paths and check they exist
    if not path.startswith('/'):
        return jsonify({'error': 'Path must be absolute'}), 400

    if not isfile(path) and not isdir(path):
        return jsonify({'error': 'Path does not exist'}), 404

    # Get size before deletion for updating scans
    try:
        if isfile(path):
            deleted_size = stat(path).st_size
            deleted_n_desc = 1
        else:
            # For directories, we need to calculate total size
            deleted_size = 0
            deleted_n_desc = 0
            for root, dirs, files in __import__('os').walk(path):
                deleted_n_desc += len(files) + len(dirs)
                for f in files:
                    try:
                        deleted_size += stat(join(root, f)).st_size
                    except (OSError, PermissionError):
                        pass
            deleted_n_desc += 1  # Include the directory itself
    except (OSError, PermissionError) as e:
        return jsonify({'error': f'Cannot access path: {e}'}), 403

    # Perform the deletion
    try:
        if isfile(path):
            remove(path)
        else:
            shutil.rmtree(path)
    except (OSError, PermissionError) as e:
        return jsonify({'error': f'Failed to delete: {e}'}), 500

    # Update parent scans
    try:
        update_parent_scans_after_delete(path, deleted_size, deleted_n_desc)
    except Exception as e:
        # Deletion succeeded but scan update failed - log but don't fail the request
        print(f"Warning: Failed to update scans after deletion: {e}")

    return jsonify({
        'success': True,
        'path': path,
        'deleted_size': deleted_size,
        'deleted_n_desc': deleted_n_desc,
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
