from os.path import abspath, dirname
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
        # No ancestor scan - look for child scans that could form a virtual directory
        prefix = search_path.rstrip('/') + '/'
        cursor = db.execute('''
            SELECT path, time, blob FROM scan
            WHERE path LIKE ?
            GROUP BY path
            HAVING time = MAX(time)
            ORDER BY path
        ''', (prefix + '%',))
        child_scans = cursor.fetchall()

        if child_scans:
            # Build a virtual directory from child scans
            children = []
            for child in child_scans:
                child_path = child['path']
                # Get the immediate child name (first path segment after prefix)
                rel = child_path[len(prefix):]
                immediate = rel.split('/')[0]
                # Load the parquet to get the root row's stats
                df = pd.read_parquet(child['blob'])
                root_row = df[df['path'] == '.'].iloc[0]
                children.append({
                    'path': immediate,
                    'uri': prefix + immediate,
                    'size': int(root_row['size']),
                    'mtime': int(root_row['mtime']),
                    'kind': 'dir',
                    'n_children': int(root_row.get('n_children', 0)),
                    'n_desc': int(root_row.get('n_desc', 0)),
                })

            # Dedupe by immediate child name (keep largest)
            seen = {}
            for c in children:
                if c['path'] not in seen or c['size'] > seen[c['path']]['size']:
                    seen[c['path']] = c
            children = sorted(seen.values(), key=lambda x: -x['size'])

            # Create a virtual root
            total_size = sum(c['size'] for c in children)
            max_mtime = max(c['mtime'] for c in children)
            total_desc = sum(c['n_desc'] for c in children)

            return jsonify({
                'root': {
                    'path': '.',
                    'uri': search_path,
                    'size': total_size,
                    'mtime': max_mtime,
                    'kind': 'dir',
                    'n_children': len(children),
                    'n_desc': total_desc,
                    'parent': None,
                },
                'children': children,
                'rows': children,
                'time': None,
                'scan_path': None,
                'virtual': True,
            })

        return jsonify({'error': 'No scan found for path', 'uri': uri}), 404

    # Load parquet
    df = pd.read_parquet(scan['blob'])

    # Filter to requested URI prefix
    prefix = uri.rstrip('/') + '/'
    if scan['path'] == uri:
        # Exact match - use '.' as root
        root_mask = df['path'] == '.'
        children_mask = df['parent'] == '.'
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
        direct_children_df = df[df['parent'] == '.']
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

    return jsonify({
        'root': root,
        'children': sorted(children, key=lambda x: -x.get('size', 0)),
        'rows': rows,
        'time': scan['time'],
        'scan_path': scan['path'],
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


def main():
    app.run(debug=True, port=5001)


if __name__ == '__main__':
    main()
