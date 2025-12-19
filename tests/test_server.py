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

    # Create database with scan table
    conn = sqlite3.connect(db_path)
    conn.execute('''
        CREATE TABLE scan (
            id INTEGER NOT NULL PRIMARY KEY,
            path VARCHAR NOT NULL,
            time DATETIME NOT NULL,
            blob VARCHAR NOT NULL,
            error_count INTEGER,
            error_paths TEXT
        )
    ''')
    conn.execute('CREATE INDEX ix_scan_path_time ON scan(path, time)')
    conn.commit()
    conn.close()

    # Patch the DB_PATH
    monkeypatch.setattr('disk_tree.server.DB_PATH', db_path)

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
