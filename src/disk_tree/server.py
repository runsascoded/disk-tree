from os import listdir, remove, stat
from os.path import abspath, dirname, isdir, isfile, join
import shutil
import sqlite3
import subprocess
import threading
import uuid
from datetime import datetime

import pandas as pd
from flask import Flask, jsonify, request, g
from flask_cors import CORS

from disk_tree.config import SQLITE_PATH

app = Flask(__name__)
CORS(app)

DB_PATH = abspath(SQLITE_PATH)

# Track in-progress scans: {job_id: {path, status, started, output, error}}
running_scans: dict[str, dict] = {}


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


@app.route('/api/scans')
def get_scans():
    """Return list of most recent scan per path."""
    db = get_db()
    # Get most recent scan for each path
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
        })
    return jsonify(result)


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


def get_scanned_paths(db) -> dict[str, dict]:
    """Get a mapping of scanned paths to their most recent scan info."""
    cursor = db.execute('''
        SELECT s.path, s.time, s.blob
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
        test_path = dirname(test_path)
        if not test_path or test_path == test_path.rstrip('/'):
            if not test_path.startswith('s3://'):
                test_path = dirname(test_path) if test_path != '/' else None

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
                        # Load parquet to get stats
                        df = pd.read_parquet(scan_info['blob'])
                        root_row = df[df['path'] == '.'].iloc[0]
                        scan_data_by_name[immediate] = {
                            'size': int(root_row['size']),
                            'mtime': int(root_row['mtime']),
                            'n_children': int(root_row.get('n_children', 0)),
                            'n_desc': int(root_row.get('n_desc', 0)),
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
                    df = pd.read_parquet(scan_info['blob'])
                    root_row = df[df['path'] == '.'].iloc[0]
                    child['size'] = int(root_row['size'])
                    child['mtime'] = int(root_row['mtime'])
                    child['n_children'] = int(root_row.get('n_children', 0))
                    child['n_desc'] = int(root_row.get('n_desc', 0))
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

        return jsonify({'error': 'No scan found for path', 'uri': uri}), 404

    # Load parquet
    df = pd.read_parquet(scan['blob'])

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

        # Recompute relative paths
        def make_relative(row):
            if row['uri'] == uri:
                return '.'
            elif row['uri'].startswith(prefix):
                return row['uri'][len(prefix):]
            return row['path']

        df = df.copy()
        df['rel_path'] = df.apply(make_relative, axis=1)
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
        children_df = df[(df['depth'] > 0) & (df['depth'] <= depth)]
        direct_children_df = df[df['depth'] == 1]

    # Convert to list of dicts, handling numpy types
    def row_to_dict(row):
        d = row.to_dict()
        # Convert numpy types to Python types
        for k, v in d.items():
            if hasattr(v, 'item'):
                d[k] = v.item()
        return d

    children = [row_to_dict(row) for _, row in direct_children_df.iterrows()]
    rows = [row_to_dict(row) for _, row in children_df.iterrows()]

    # Mark all children as scanned
    for c in children:
        c['scanned'] = True

    return jsonify({
        'root': root,
        'children': sorted(children, key=lambda x: -x.get('size', 0)),
        'rows': rows,
        'time': scan['time'],
        'scan_path': scan['path'],
        'scan_status': 'full',
    })


def run_scan_job(job_id: str, path: str):
    """Run disk-tree index in background and update job status."""
    try:
        running_scans[job_id]['status'] = 'running'
        result = subprocess.run(
            ['disk-tree', 'index', path],
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


def main():
    app.run(debug=True, port=5001)


if __name__ == '__main__':
    main()
