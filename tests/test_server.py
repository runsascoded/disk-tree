"""Tests for the Flask API server."""
import os
import shutil
import sqlite3
import tempfile
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import pytest

from disk_tree.server import app


@pytest.fixture
def test_db_dir():
    """Create a temporary directory for test database and parquet files."""
    tmpdir = tempfile.mkdtemp()
    yield tmpdir
    shutil.rmtree(tmpdir)


@pytest.fixture
def test_client(test_db_dir, monkeypatch):
    """Create a test client with isolated database."""
    db_path = os.path.join(test_db_dir, 'disk-tree.db')
    scans_dir = os.path.join(test_db_dir, 'scans')
    os.makedirs(scans_dir)

    # Create database with scan table (including denormalized stats columns)
    conn = sqlite3.connect(db_path)
    conn.execute('''
        CREATE TABLE scan (
            id INTEGER NOT NULL PRIMARY KEY,
            path VARCHAR NOT NULL,
            time DATETIME NOT NULL,
            blob VARCHAR NOT NULL,
            error_count INTEGER,
            error_paths TEXT,
            size INTEGER,
            n_children INTEGER,
            n_desc INTEGER
        )
    ''')
    conn.execute('CREATE INDEX ix_scan_path_time ON scan(path, time)')
    conn.commit()
    conn.close()

    # Patch the DB_PATH
    monkeypatch.setattr('disk_tree.server.DB_PATH', db_path)

    # Clear cache before each test
    from disk_tree.server import clear_cache
    clear_cache()

    app.config['TESTING'] = True
    with app.test_client() as client:
        yield client, db_path, scans_dir


def create_test_parquet(scans_dir: str, name: str, rows: list[dict]) -> str:
    """Create a test parquet file with given rows."""
    df = pd.DataFrame(rows)
    path = os.path.join(scans_dir, f'{name}.parquet')
    df.to_parquet(path)
    return path


class TestGetScans:
    """Tests for GET /api/scans endpoint."""

    def test_empty_db(self, test_client):
        """Returns empty list when no scans exist."""
        client, _, _ = test_client
        response = client.get('/api/scans')
        assert response.status_code == 200
        assert response.json == []

    def test_returns_scans(self, test_client):
        """Returns list of scans."""
        client, db_path, scans_dir = test_client

        # Create a parquet file
        parquet_path = create_test_parquet(scans_dir, 'test', [
            {'path': '.', 'size': 1000, 'mtime': 100, 'kind': 'dir', 'parent': '', 'uri': '/test', 'n_desc': 1, 'n_children': 0},
        ])

        # Insert scan record
        conn = sqlite3.connect(db_path)
        conn.execute(
            'INSERT INTO scan (path, time, blob) VALUES (?, ?, ?)',
            ('/test', '2025-01-01T12:00:00', parquet_path),
        )
        conn.commit()
        conn.close()

        response = client.get('/api/scans')
        assert response.status_code == 200
        scans = response.json
        assert len(scans) == 1
        assert scans[0]['path'] == '/test'
        assert scans[0]['time'] == '2025-01-01T12:00:00'

    def test_returns_most_recent_per_path(self, test_client):
        """Returns only the most recent scan for each path."""
        client, db_path, scans_dir = test_client

        parquet1 = create_test_parquet(scans_dir, 'test1', [
            {'path': '.', 'size': 1000, 'mtime': 100, 'kind': 'dir', 'parent': '', 'uri': '/test', 'n_desc': 1, 'n_children': 0},
        ])
        parquet2 = create_test_parquet(scans_dir, 'test2', [
            {'path': '.', 'size': 2000, 'mtime': 200, 'kind': 'dir', 'parent': '', 'uri': '/test', 'n_desc': 1, 'n_children': 0},
        ])

        conn = sqlite3.connect(db_path)
        conn.execute(
            'INSERT INTO scan (path, time, blob) VALUES (?, ?, ?)',
            ('/test', '2025-01-01T12:00:00', parquet1),
        )
        conn.execute(
            'INSERT INTO scan (path, time, blob) VALUES (?, ?, ?)',
            ('/test', '2025-01-02T12:00:00', parquet2),
        )
        conn.commit()
        conn.close()

        response = client.get('/api/scans')
        assert response.status_code == 200
        scans = response.json
        assert len(scans) == 1
        assert scans[0]['time'] == '2025-01-02T12:00:00'


class TestGetScan:
    """Tests for GET /api/scan endpoint."""

    def test_scan_not_found(self, test_client):
        """Returns 404 for paths without scans."""
        client, _, _ = test_client
        response = client.get('/api/scan?uri=/nonexistent')
        # Falls through to filesystem listing if no scan, which returns scan_status='none'
        assert response.status_code == 200
        data = response.json
        assert data['scan_status'] == 'none'

    def test_exact_match(self, test_client):
        """Returns scan data for exact path match."""
        client, db_path, scans_dir = test_client

        parquet_path = create_test_parquet(scans_dir, 'test', [
            {'path': '.', 'size': 1000, 'mtime': 100, 'kind': 'dir', 'parent': '', 'uri': '/test/parent', 'n_desc': 2, 'n_children': 1},
            {'path': 'child', 'size': 500, 'mtime': 50, 'kind': 'dir', 'parent': '.', 'uri': '/test/parent/child', 'n_desc': 1, 'n_children': 0},
        ])

        conn = sqlite3.connect(db_path)
        conn.execute(
            'INSERT INTO scan (path, time, blob) VALUES (?, ?, ?)',
            ('/test/parent', '2025-01-01T12:00:00', parquet_path),
        )
        conn.commit()
        conn.close()

        response = client.get('/api/scan?uri=/test/parent')
        assert response.status_code == 200
        data = response.json
        assert data['scan_status'] == 'full'
        assert data['root']['size'] == 1000
        assert len(data['children']) == 1
        assert data['children'][0]['path'] == 'child'


class TestFresherChildPatching:
    """Tests for fresher child scan patching."""

    def test_patches_fresher_child_scan(self, test_client):
        """When child has newer scan, patches its stats into parent view."""
        client, db_path, scans_dir = test_client

        parent_time = datetime(2025, 1, 1, 12, 0, 0)
        child_time = parent_time + timedelta(hours=1)

        # Parent scan shows child with old stats (size=100)
        parent_parquet = create_test_parquet(scans_dir, 'parent', [
            {'path': '.', 'size': 1000, 'mtime': 100, 'kind': 'dir', 'parent': '', 'uri': '/test/parent', 'n_desc': 3, 'n_children': 2},
            {'path': 'child1', 'size': 100, 'mtime': 10, 'kind': 'dir', 'parent': '.', 'uri': '/test/parent/child1', 'n_desc': 1, 'n_children': 0},
            {'path': 'child2', 'size': 900, 'mtime': 90, 'kind': 'dir', 'parent': '.', 'uri': '/test/parent/child2', 'n_desc': 1, 'n_children': 0},
        ])

        # Child scan shows updated stats (size=200)
        child_parquet = create_test_parquet(scans_dir, 'child', [
            {'path': '.', 'size': 200, 'mtime': 20, 'kind': 'dir', 'parent': '', 'uri': '/test/parent/child1', 'n_desc': 2, 'n_children': 1},
            {'path': 'subfile.txt', 'size': 200, 'mtime': 20, 'kind': 'file', 'parent': '.', 'uri': '/test/parent/child1/subfile.txt', 'n_desc': 1, 'n_children': 0},
        ])

        conn = sqlite3.connect(db_path)
        conn.execute(
            'INSERT INTO scan (path, time, blob, size, n_children, n_desc) VALUES (?, ?, ?, ?, ?, ?)',
            ('/test/parent', parent_time.isoformat(), parent_parquet, 1000, 2, 3),
        )
        conn.execute(
            'INSERT INTO scan (path, time, blob, size, n_children, n_desc) VALUES (?, ?, ?, ?, ?, ?)',
            ('/test/parent/child1', child_time.isoformat(), child_parquet, 200, 1, 2),
        )
        conn.commit()
        conn.close()

        response = client.get('/api/scan?uri=/test/parent')
        assert response.status_code == 200
        data = response.json

        # Find child1 in children
        child1 = next((c for c in data['children'] if c['path'] == 'child1'), None)
        assert child1 is not None
        assert child1['size'] == 200, 'child1 size should be patched to 200'
        assert child1['patched'] is True, 'child1 should be marked as patched'
        assert child1['n_desc'] == 2, 'child1 n_desc should be patched'
        assert child1['n_children'] == 1, 'child1 n_children should be patched'

        # child2 should be unchanged
        child2 = next((c for c in data['children'] if c['path'] == 'child2'), None)
        assert child2 is not None
        assert child2['size'] == 900
        assert child2.get('patched') is not True

    def test_does_not_patch_older_child_scan(self, test_client):
        """When child has older scan, does not patch."""
        client, db_path, scans_dir = test_client

        parent_time = datetime(2025, 1, 2, 12, 0, 0)  # Parent is newer
        child_time = datetime(2025, 1, 1, 12, 0, 0)   # Child is older

        parent_parquet = create_test_parquet(scans_dir, 'parent', [
            {'path': '.', 'size': 1000, 'mtime': 100, 'kind': 'dir', 'parent': '', 'uri': '/test/parent', 'n_desc': 2, 'n_children': 1},
            {'path': 'child1', 'size': 100, 'mtime': 10, 'kind': 'dir', 'parent': '.', 'uri': '/test/parent/child1', 'n_desc': 1, 'n_children': 0},
        ])

        child_parquet = create_test_parquet(scans_dir, 'child', [
            {'path': '.', 'size': 200, 'mtime': 20, 'kind': 'dir', 'parent': '', 'uri': '/test/parent/child1', 'n_desc': 2, 'n_children': 1},
        ])

        conn = sqlite3.connect(db_path)
        conn.execute(
            'INSERT INTO scan (path, time, blob) VALUES (?, ?, ?)',
            ('/test/parent', parent_time.isoformat(), parent_parquet),
        )
        conn.execute(
            'INSERT INTO scan (path, time, blob) VALUES (?, ?, ?)',
            ('/test/parent/child1', child_time.isoformat(), child_parquet),
        )
        conn.commit()
        conn.close()

        response = client.get('/api/scan?uri=/test/parent')
        assert response.status_code == 200
        data = response.json

        child1 = next((c for c in data['children'] if c['path'] == 'child1'), None)
        assert child1 is not None
        assert child1['size'] == 100, 'child1 size should NOT be patched (older scan)'
        assert child1.get('patched') is not True

    def test_does_not_patch_grandchild_scans(self, test_client):
        """Grandchild scans should not be patched into parent view (not transitive)."""
        client, db_path, scans_dir = test_client

        parent_time = datetime(2025, 1, 1, 12, 0, 0)
        grandchild_time = parent_time + timedelta(hours=1)

        parent_parquet = create_test_parquet(scans_dir, 'parent', [
            {'path': '.', 'size': 1000, 'mtime': 100, 'kind': 'dir', 'parent': '', 'uri': '/test/parent', 'n_desc': 3, 'n_children': 1},
            {'path': 'child', 'size': 500, 'mtime': 50, 'kind': 'dir', 'parent': '.', 'uri': '/test/parent/child', 'n_desc': 2, 'n_children': 1},
            {'path': 'child/grandchild', 'size': 100, 'mtime': 10, 'kind': 'dir', 'parent': 'child', 'uri': '/test/parent/child/grandchild', 'n_desc': 1, 'n_children': 0},
        ])

        grandchild_parquet = create_test_parquet(scans_dir, 'grandchild', [
            {'path': '.', 'size': 200, 'mtime': 20, 'kind': 'dir', 'parent': '', 'uri': '/test/parent/child/grandchild', 'n_desc': 1, 'n_children': 0},
        ])

        conn = sqlite3.connect(db_path)
        conn.execute(
            'INSERT INTO scan (path, time, blob) VALUES (?, ?, ?)',
            ('/test/parent', parent_time.isoformat(), parent_parquet),
        )
        conn.execute(
            'INSERT INTO scan (path, time, blob) VALUES (?, ?, ?)',
            ('/test/parent/child/grandchild', grandchild_time.isoformat(), grandchild_parquet),
        )
        conn.commit()
        conn.close()

        response = client.get('/api/scan?uri=/test/parent')
        assert response.status_code == 200
        data = response.json

        # child should NOT be patched (grandchild scans don't propagate)
        child = next((c for c in data['children'] if c['path'] == 'child'), None)
        assert child is not None
        assert child['size'] == 500, 'child size should NOT be patched from grandchild'
        assert child.get('patched') is not True


class TestAncestorScanRelativePaths:
    """Tests for viewing subdirectories of scans (ancestor scan case)."""

    def test_children_have_relative_paths(self, test_client):
        """When viewing subdir of a scan, children paths should be relative to viewed dir."""
        client, db_path, scans_dir = test_client

        # Scan at /test with nested structure
        parquet_path = create_test_parquet(scans_dir, 'test', [
            {'path': '.', 'size': 1000, 'mtime': 100, 'kind': 'dir', 'parent': '', 'uri': '/test', 'n_desc': 5, 'n_children': 2},
            {'path': 'subdir', 'size': 600, 'mtime': 80, 'kind': 'dir', 'parent': '.', 'uri': '/test/subdir', 'n_desc': 3, 'n_children': 2},
            {'path': 'subdir/child1', 'size': 300, 'mtime': 60, 'kind': 'dir', 'parent': 'subdir', 'uri': '/test/subdir/child1', 'n_desc': 1, 'n_children': 0},
            {'path': 'subdir/child2', 'size': 300, 'mtime': 70, 'kind': 'file', 'parent': 'subdir', 'uri': '/test/subdir/child2', 'n_desc': 1, 'n_children': 0},
            {'path': 'other', 'size': 400, 'mtime': 90, 'kind': 'dir', 'parent': '.', 'uri': '/test/other', 'n_desc': 1, 'n_children': 0},
        ])

        conn = sqlite3.connect(db_path)
        conn.execute(
            'INSERT INTO scan (path, time, blob) VALUES (?, ?, ?)',
            ('/test', '2025-01-01T12:00:00', parquet_path),
        )
        conn.commit()
        conn.close()

        # View /test/subdir (a subdir of the scan at /test)
        response = client.get('/api/scan?uri=/test/subdir')
        assert response.status_code == 200
        data = response.json

        # Root should be '.'
        assert data['root']['path'] == '.'
        assert data['root']['size'] == 600

        # Children should have relative paths (child1, child2), NOT (subdir/child1, subdir/child2)
        child_paths = sorted([c['path'] for c in data['children']])
        assert child_paths == ['child1', 'child2'], f'Expected relative paths, got {child_paths}'

    def test_deeply_nested_subdir_paths(self, test_client):
        """Relative paths work correctly for deeply nested directories."""
        client, db_path, scans_dir = test_client

        # Scan at /root with deeply nested structure
        parquet_path = create_test_parquet(scans_dir, 'deep', [
            {'path': '.', 'size': 1000, 'mtime': 100, 'kind': 'dir', 'parent': '', 'uri': '/root', 'n_desc': 5, 'n_children': 1},
            {'path': 'a', 'size': 800, 'mtime': 90, 'kind': 'dir', 'parent': '.', 'uri': '/root/a', 'n_desc': 4, 'n_children': 1},
            {'path': 'a/b', 'size': 600, 'mtime': 80, 'kind': 'dir', 'parent': 'a', 'uri': '/root/a/b', 'n_desc': 3, 'n_children': 1},
            {'path': 'a/b/c', 'size': 400, 'mtime': 70, 'kind': 'dir', 'parent': 'a/b', 'uri': '/root/a/b/c', 'n_desc': 2, 'n_children': 1},
            {'path': 'a/b/c/file.txt', 'size': 100, 'mtime': 60, 'kind': 'file', 'parent': 'a/b/c', 'uri': '/root/a/b/c/file.txt', 'n_desc': 1, 'n_children': 0},
        ])

        conn = sqlite3.connect(db_path)
        conn.execute(
            'INSERT INTO scan (path, time, blob) VALUES (?, ?, ?)',
            ('/root', '2025-01-01T12:00:00', parquet_path),
        )
        conn.commit()
        conn.close()

        # View /root/a/b (deeply nested)
        response = client.get('/api/scan?uri=/root/a/b')
        assert response.status_code == 200
        data = response.json

        assert data['root']['path'] == '.'
        assert data['root']['size'] == 600

        # Direct child should be 'c', not 'a/b/c'
        assert len(data['children']) == 1
        assert data['children'][0]['path'] == 'c', f'Expected "c", got {data["children"][0]["path"]}'

    def test_s3_subdir_relative_paths(self, test_client):
        """Relative paths work for S3 URIs when viewing subdir of scan."""
        client, db_path, scans_dir = test_client

        # Scan at s3://bucket with nested structure (simulating .dvc case)
        parquet_path = create_test_parquet(scans_dir, 's3bucket', [
            {'path': '.', 'size': 1000, 'mtime': 100, 'kind': 'dir', 'parent': '', 'uri': 's3://bucket', 'n_desc': 4, 'n_children': 1},
            {'path': '.dvc', 'size': 800, 'mtime': 90, 'kind': 'dir', 'parent': '.', 'uri': 's3://bucket/.dvc', 'n_desc': 3, 'n_children': 2},
            {'path': '.dvc/files', 'size': 500, 'mtime': 80, 'kind': 'dir', 'parent': '.dvc', 'uri': 's3://bucket/.dvc/files', 'n_desc': 1, 'n_children': 0},
            {'path': '.dvc/cache', 'size': 300, 'mtime': 70, 'kind': 'dir', 'parent': '.dvc', 'uri': 's3://bucket/.dvc/cache', 'n_desc': 1, 'n_children': 0},
        ])

        conn = sqlite3.connect(db_path)
        conn.execute(
            'INSERT INTO scan (path, time, blob) VALUES (?, ?, ?)',
            ('s3://bucket', '2025-01-01T12:00:00', parquet_path),
        )
        conn.commit()
        conn.close()

        # View s3://bucket/.dvc (subdir of the scan)
        response = client.get('/api/scan?uri=s3://bucket/.dvc')
        assert response.status_code == 200
        data = response.json

        assert data['root']['path'] == '.'
        assert data['root']['uri'] == 's3://bucket/.dvc'

        # Children should be 'files' and 'cache', NOT '.dvc/files' and '.dvc/cache'
        child_paths = sorted([c['path'] for c in data['children']])
        assert child_paths == ['cache', 'files'], f'Expected relative paths, got {child_paths}'


class TestScanHistoryWithAncestors:
    """Tests for GET /api/scans/history including ancestor scans."""

    def test_returns_exact_match_scans(self, test_client):
        """Returns scans that exactly match the requested path."""
        client, db_path, scans_dir = test_client

        parquet_path = create_test_parquet(scans_dir, 'test', [
            {'path': '.', 'size': 1000, 'mtime': 100, 'kind': 'dir', 'parent': '', 'uri': '/test/subdir', 'n_desc': 1, 'n_children': 0},
        ])

        conn = sqlite3.connect(db_path)
        conn.execute(
            'INSERT INTO scan (path, time, blob, size, n_children, n_desc) VALUES (?, ?, ?, ?, ?, ?)',
            ('/test/subdir', '2025-01-01T12:00:00', parquet_path, 1000, 0, 1),
        )
        conn.commit()
        conn.close()

        response = client.get('/api/scans/history?uri=/test/subdir')
        assert response.status_code == 200
        scans = response.json
        assert len(scans) == 1
        assert scans[0]['path'] == '/test/subdir'
        assert scans[0]['scan_path'] == '/test/subdir'

    def test_includes_ancestor_scans(self, test_client):
        """Returns ancestor scans that contain data for the requested path."""
        client, db_path, scans_dir = test_client

        # Parent scan at /test containing subdir
        parent_parquet = create_test_parquet(scans_dir, 'parent', [
            {'path': '.', 'size': 2000, 'mtime': 100, 'kind': 'dir', 'parent': '', 'uri': '/test', 'n_desc': 3, 'n_children': 2},
            {'path': 'subdir', 'size': 1000, 'mtime': 80, 'kind': 'dir', 'parent': '.', 'uri': '/test/subdir', 'n_desc': 1, 'n_children': 0},
            {'path': 'other', 'size': 1000, 'mtime': 90, 'kind': 'dir', 'parent': '.', 'uri': '/test/other', 'n_desc': 1, 'n_children': 0},
        ])

        conn = sqlite3.connect(db_path)
        conn.execute(
            'INSERT INTO scan (path, time, blob, size, n_children, n_desc) VALUES (?, ?, ?, ?, ?, ?)',
            ('/test', '2025-01-01T12:00:00', parent_parquet, 2000, 2, 3),
        )
        conn.commit()
        conn.close()

        # Request history for /test/subdir - should include the parent scan
        response = client.get('/api/scans/history?uri=/test/subdir')
        assert response.status_code == 200
        scans = response.json
        assert len(scans) == 1
        assert scans[0]['path'] == '/test'  # Path is the scan's path
        assert scans[0]['scan_path'] == '/test'  # scan_path indicates source
        assert scans[0]['size'] == 1000  # Size extracted from parquet for /test/subdir

    def test_combines_exact_and_ancestor_scans(self, test_client):
        """Returns both exact match scans and ancestor scans."""
        client, db_path, scans_dir = test_client

        # Exact scan of /test/subdir
        exact_parquet = create_test_parquet(scans_dir, 'exact', [
            {'path': '.', 'size': 1500, 'mtime': 100, 'kind': 'dir', 'parent': '', 'uri': '/test/subdir', 'n_desc': 2, 'n_children': 1},
        ])

        # Ancestor scan at /test containing subdir
        ancestor_parquet = create_test_parquet(scans_dir, 'ancestor', [
            {'path': '.', 'size': 3000, 'mtime': 100, 'kind': 'dir', 'parent': '', 'uri': '/test', 'n_desc': 4, 'n_children': 2},
            {'path': 'subdir', 'size': 1000, 'mtime': 80, 'kind': 'dir', 'parent': '.', 'uri': '/test/subdir', 'n_desc': 1, 'n_children': 0},
            {'path': 'other', 'size': 2000, 'mtime': 90, 'kind': 'dir', 'parent': '.', 'uri': '/test/other', 'n_desc': 2, 'n_children': 1},
        ])

        conn = sqlite3.connect(db_path)
        conn.execute(
            'INSERT INTO scan (path, time, blob, size, n_children, n_desc) VALUES (?, ?, ?, ?, ?, ?)',
            ('/test/subdir', '2025-01-02T12:00:00', exact_parquet, 1500, 1, 2),
        )
        conn.execute(
            'INSERT INTO scan (path, time, blob, size, n_children, n_desc) VALUES (?, ?, ?, ?, ?, ?)',
            ('/test', '2025-01-01T12:00:00', ancestor_parquet, 3000, 2, 4),
        )
        conn.commit()
        conn.close()

        response = client.get('/api/scans/history?uri=/test/subdir')
        assert response.status_code == 200
        scans = response.json
        assert len(scans) == 2

        # Sort by time to check both are present
        scan_paths = {s['scan_path'] for s in scans}
        assert scan_paths == {'/test/subdir', '/test'}

        # Verify sizes are correct for each
        exact_scan = next(s for s in scans if s['scan_path'] == '/test/subdir')
        ancestor_scan = next(s for s in scans if s['scan_path'] == '/test')
        assert exact_scan['size'] == 1500  # Direct from denormalized stats
        assert ancestor_scan['size'] == 1000  # Extracted from parquet


class TestCompareWithAncestorScans:
    """Tests for /api/compare endpoint with ancestor scans."""

    def test_compare_exact_match_scans(self, test_client):
        """Compare two scans that exactly match the requested path."""
        client, db_path, scans_dir = test_client

        parquet1 = create_test_parquet(scans_dir, 'scan1', [
            {'path': '.', 'size': 1000, 'mtime': 100, 'kind': 'dir', 'parent': '', 'uri': '/test', 'n_desc': 2, 'n_children': 1},
            {'path': 'file.txt', 'size': 500, 'mtime': 80, 'kind': 'file', 'parent': '.', 'uri': '/test/file.txt', 'n_desc': 1, 'n_children': 0},
        ])
        parquet2 = create_test_parquet(scans_dir, 'scan2', [
            {'path': '.', 'size': 1500, 'mtime': 110, 'kind': 'dir', 'parent': '', 'uri': '/test', 'n_desc': 2, 'n_children': 1},
            {'path': 'file.txt', 'size': 1000, 'mtime': 90, 'kind': 'file', 'parent': '.', 'uri': '/test/file.txt', 'n_desc': 1, 'n_children': 0},
        ])

        conn = sqlite3.connect(db_path)
        conn.execute(
            'INSERT INTO scan (path, time, blob, size, n_children, n_desc) VALUES (?, ?, ?, ?, ?, ?)',
            ('/test', '2025-01-01T12:00:00', parquet1, 1000, 1, 2),
        )
        conn.execute(
            'INSERT INTO scan (path, time, blob, size, n_children, n_desc) VALUES (?, ?, ?, ?, ?, ?)',
            ('/test', '2025-01-02T12:00:00', parquet2, 1500, 1, 2),
        )
        conn.commit()
        conn.close()

        response = client.get('/api/compare?uri=/test&scan1=1&scan2=2')
        assert response.status_code == 200
        data = response.json

        assert data['uri'] == '/test'
        assert len(data['rows']) == 1
        assert data['rows'][0]['path'] == 'file.txt'
        assert data['rows'][0]['size_delta'] == 500
        assert data['summary']['changed'] == 1

    def test_compare_ancestor_scans_for_subdir(self, test_client):
        """Compare two ancestor scans when viewing a subdirectory."""
        client, db_path, scans_dir = test_client

        # Two scans at /test, we want to compare /test/subdir
        parquet1 = create_test_parquet(scans_dir, 'scan1', [
            {'path': '.', 'size': 2000, 'mtime': 100, 'kind': 'dir', 'parent': '', 'uri': '/test', 'n_desc': 4, 'n_children': 2},
            {'path': 'subdir', 'size': 1000, 'mtime': 80, 'kind': 'dir', 'parent': '.', 'uri': '/test/subdir', 'n_desc': 2, 'n_children': 1},
            {'path': 'subdir/child', 'size': 500, 'mtime': 70, 'kind': 'dir', 'parent': 'subdir', 'uri': '/test/subdir/child', 'n_desc': 1, 'n_children': 0},
            {'path': 'other', 'size': 1000, 'mtime': 90, 'kind': 'dir', 'parent': '.', 'uri': '/test/other', 'n_desc': 1, 'n_children': 0},
        ])
        parquet2 = create_test_parquet(scans_dir, 'scan2', [
            {'path': '.', 'size': 2500, 'mtime': 110, 'kind': 'dir', 'parent': '', 'uri': '/test', 'n_desc': 4, 'n_children': 2},
            {'path': 'subdir', 'size': 1500, 'mtime': 90, 'kind': 'dir', 'parent': '.', 'uri': '/test/subdir', 'n_desc': 2, 'n_children': 1},
            {'path': 'subdir/child', 'size': 1000, 'mtime': 85, 'kind': 'dir', 'parent': 'subdir', 'uri': '/test/subdir/child', 'n_desc': 1, 'n_children': 0},
            {'path': 'other', 'size': 1000, 'mtime': 90, 'kind': 'dir', 'parent': '.', 'uri': '/test/other', 'n_desc': 1, 'n_children': 0},
        ])

        conn = sqlite3.connect(db_path)
        conn.execute(
            'INSERT INTO scan (path, time, blob, size, n_children, n_desc) VALUES (?, ?, ?, ?, ?, ?)',
            ('/test', '2025-01-01T12:00:00', parquet1, 2000, 2, 4),
        )
        conn.execute(
            'INSERT INTO scan (path, time, blob, size, n_children, n_desc) VALUES (?, ?, ?, ?, ?, ?)',
            ('/test', '2025-01-02T12:00:00', parquet2, 2500, 2, 4),
        )
        conn.commit()
        conn.close()

        # Compare /test/subdir using the two ancestor scans
        response = client.get('/api/compare?uri=/test/subdir&scan1=1&scan2=2')
        assert response.status_code == 200
        data = response.json

        assert data['uri'] == '/test/subdir'
        assert data['scan1']['scan_path'] == '/test'
        assert data['scan2']['scan_path'] == '/test'
        assert data['scan1']['size'] == 1000  # Size of /test/subdir in scan1
        assert data['scan2']['size'] == 1500  # Size of /test/subdir in scan2

        # Should show child as changed
        assert len(data['rows']) == 1
        assert data['rows'][0]['path'] == 'child'
        assert data['rows'][0]['size_delta'] == 500
        assert data['rows'][0]['uri'] == '/test/subdir/child'

    def test_compare_detects_added_removed(self, test_client):
        """Compare detects added and removed items in ancestor scans."""
        client, db_path, scans_dir = test_client

        # scan1: subdir has child1
        parquet1 = create_test_parquet(scans_dir, 'scan1', [
            {'path': '.', 'size': 2000, 'mtime': 100, 'kind': 'dir', 'parent': '', 'uri': '/test', 'n_desc': 3, 'n_children': 1},
            {'path': 'subdir', 'size': 1000, 'mtime': 80, 'kind': 'dir', 'parent': '.', 'uri': '/test/subdir', 'n_desc': 2, 'n_children': 1},
            {'path': 'subdir/child1', 'size': 500, 'mtime': 70, 'kind': 'dir', 'parent': 'subdir', 'uri': '/test/subdir/child1', 'n_desc': 1, 'n_children': 0},
        ])
        # scan2: subdir has child2 (child1 removed, child2 added)
        parquet2 = create_test_parquet(scans_dir, 'scan2', [
            {'path': '.', 'size': 2000, 'mtime': 110, 'kind': 'dir', 'parent': '', 'uri': '/test', 'n_desc': 3, 'n_children': 1},
            {'path': 'subdir', 'size': 800, 'mtime': 90, 'kind': 'dir', 'parent': '.', 'uri': '/test/subdir', 'n_desc': 2, 'n_children': 1},
            {'path': 'subdir/child2', 'size': 300, 'mtime': 85, 'kind': 'file', 'parent': 'subdir', 'uri': '/test/subdir/child2', 'n_desc': 1, 'n_children': 0},
        ])

        conn = sqlite3.connect(db_path)
        conn.execute(
            'INSERT INTO scan (path, time, blob, size, n_children, n_desc) VALUES (?, ?, ?, ?, ?, ?)',
            ('/test', '2025-01-01T12:00:00', parquet1, 2000, 1, 3),
        )
        conn.execute(
            'INSERT INTO scan (path, time, blob, size, n_children, n_desc) VALUES (?, ?, ?, ?, ?, ?)',
            ('/test', '2025-01-02T12:00:00', parquet2, 2000, 1, 3),
        )
        conn.commit()
        conn.close()

        response = client.get('/api/compare?uri=/test/subdir&scan1=1&scan2=2')
        assert response.status_code == 200
        data = response.json

        assert data['summary']['added'] == 1
        assert data['summary']['removed'] == 1

        rows_by_status = {r['path']: r['status'] for r in data['rows']}
        assert rows_by_status.get('child1') == 'removed'
        assert rows_by_status.get('child2') == 'added'
