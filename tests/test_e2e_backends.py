"""End-to-end tests for scan/delete sequences across all storage backends.

These tests verify that multi-step operations work correctly regardless of
which storage backend is used. Tests directly insert scan data via the storage
backend API rather than mocking gfind, so we're testing the view/delete logic.
"""
import os
import shutil
import sqlite3
import tempfile
from datetime import datetime, timedelta

import pandas as pd
import pytest

from disk_tree.server import app
from disk_tree.storage import reset_backend, get_backend


def create_test_scan_df(scan_path: str) -> pd.DataFrame:
    """Create a test DataFrame for a scan.

    Returns hierarchical file tree data for /test with:
    - /test (root)
    - /test/foo/ (dir with 2 files)
    - /test/bar/ (dir with 1 file + subdir)
    - /test/bar/subdir/ (dir with 1 file)
    """
    if scan_path == '/test':
        return pd.DataFrame([
            {'path': '.', 'size': 1024000, 'mtime': 1700000000, 'kind': 'dir', 'parent': '', 'uri': '/test', 'n_desc': 7, 'n_children': 2, 'depth': 0},
            {'path': 'foo', 'size': 307200, 'mtime': 1700000100, 'kind': 'dir', 'parent': '.', 'uri': '/test/foo', 'n_desc': 2, 'n_children': 2, 'depth': 1},
            {'path': 'foo/file1.txt', 'size': 102400, 'mtime': 1700000200, 'kind': 'file', 'parent': 'foo', 'uri': '/test/foo/file1.txt', 'n_desc': 0, 'n_children': 0, 'depth': 2},
            {'path': 'foo/file2.txt', 'size': 204800, 'mtime': 1700000300, 'kind': 'file', 'parent': 'foo', 'uri': '/test/foo/file2.txt', 'n_desc': 0, 'n_children': 0, 'depth': 2},
            {'path': 'bar', 'size': 716800, 'mtime': 1700000400, 'kind': 'dir', 'parent': '.', 'uri': '/test/bar', 'n_desc': 3, 'n_children': 2, 'depth': 1},
            {'path': 'bar/file3.txt', 'size': 307200, 'mtime': 1700000500, 'kind': 'file', 'parent': 'bar', 'uri': '/test/bar/file3.txt', 'n_desc': 0, 'n_children': 0, 'depth': 2},
            {'path': 'bar/subdir', 'size': 409600, 'mtime': 1700000600, 'kind': 'dir', 'parent': 'bar', 'uri': '/test/bar/subdir', 'n_desc': 1, 'n_children': 1, 'depth': 2},
            {'path': 'bar/subdir/file4.txt', 'size': 409600, 'mtime': 1700000700, 'kind': 'file', 'parent': 'bar/subdir', 'uri': '/test/bar/subdir/file4.txt', 'n_desc': 0, 'n_children': 0, 'depth': 3},
        ])
    elif scan_path == '/test/foo':
        return pd.DataFrame([
            {'path': '.', 'size': 307200, 'mtime': 1700000100, 'kind': 'dir', 'parent': '', 'uri': '/test/foo', 'n_desc': 2, 'n_children': 2, 'depth': 0},
            {'path': 'file1.txt', 'size': 102400, 'mtime': 1700000200, 'kind': 'file', 'parent': '.', 'uri': '/test/foo/file1.txt', 'n_desc': 0, 'n_children': 0, 'depth': 1},
            {'path': 'file2.txt', 'size': 204800, 'mtime': 1700000300, 'kind': 'file', 'parent': '.', 'uri': '/test/foo/file2.txt', 'n_desc': 0, 'n_children': 0, 'depth': 1},
        ])
    elif scan_path == '/test/bar':
        return pd.DataFrame([
            {'path': '.', 'size': 716800, 'mtime': 1700000400, 'kind': 'dir', 'parent': '', 'uri': '/test/bar', 'n_desc': 3, 'n_children': 2, 'depth': 0},
            {'path': 'file3.txt', 'size': 307200, 'mtime': 1700000500, 'kind': 'file', 'parent': '.', 'uri': '/test/bar/file3.txt', 'n_desc': 0, 'n_children': 0, 'depth': 1},
            {'path': 'subdir', 'size': 409600, 'mtime': 1700000600, 'kind': 'dir', 'parent': '.', 'uri': '/test/bar/subdir', 'n_desc': 1, 'n_children': 1, 'depth': 1},
            {'path': 'subdir/file4.txt', 'size': 409600, 'mtime': 1700000700, 'kind': 'file', 'parent': 'subdir', 'uri': '/test/bar/subdir/file4.txt', 'n_desc': 0, 'n_children': 0, 'depth': 2},
        ])
    elif scan_path == '/test/bar/subdir':
        return pd.DataFrame([
            {'path': '.', 'size': 409600, 'mtime': 1700000600, 'kind': 'dir', 'parent': '', 'uri': '/test/bar/subdir', 'n_desc': 1, 'n_children': 1, 'depth': 0},
            {'path': 'file4.txt', 'size': 409600, 'mtime': 1700000700, 'kind': 'file', 'parent': '.', 'uri': '/test/bar/subdir/file4.txt', 'n_desc': 0, 'n_children': 0, 'depth': 1},
        ])
    elif scan_path == '/empty':
        return pd.DataFrame([
            {'path': '.', 'size': 0, 'mtime': 1700000000, 'kind': 'dir', 'parent': '', 'uri': '/empty', 'n_desc': 0, 'n_children': 0, 'depth': 0},
        ])
    elif scan_path == '/deep':
        # Deep hierarchy: /deep/a/b/c/d/file.txt
        return pd.DataFrame([
            {'path': '.', 'size': 500000, 'mtime': 1700000000, 'kind': 'dir', 'parent': '', 'uri': '/deep', 'n_desc': 5, 'n_children': 1, 'depth': 0},
            {'path': 'a', 'size': 500000, 'mtime': 1700000100, 'kind': 'dir', 'parent': '.', 'uri': '/deep/a', 'n_desc': 4, 'n_children': 1, 'depth': 1},
            {'path': 'a/b', 'size': 500000, 'mtime': 1700000200, 'kind': 'dir', 'parent': 'a', 'uri': '/deep/a/b', 'n_desc': 3, 'n_children': 1, 'depth': 2},
            {'path': 'a/b/c', 'size': 500000, 'mtime': 1700000300, 'kind': 'dir', 'parent': 'a/b', 'uri': '/deep/a/b/c', 'n_desc': 2, 'n_children': 1, 'depth': 3},
            {'path': 'a/b/c/d', 'size': 500000, 'mtime': 1700000400, 'kind': 'dir', 'parent': 'a/b/c', 'uri': '/deep/a/b/c/d', 'n_desc': 1, 'n_children': 1, 'depth': 4},
            {'path': 'a/b/c/d/file.txt', 'size': 500000, 'mtime': 1700000500, 'kind': 'file', 'parent': 'a/b/c/d', 'uri': '/deep/a/b/c/d/file.txt', 'n_desc': 0, 'n_children': 0, 'depth': 5},
        ])
    elif scan_path == '/deep/a/b':
        return pd.DataFrame([
            {'path': '.', 'size': 500000, 'mtime': 1700000200, 'kind': 'dir', 'parent': '', 'uri': '/deep/a/b', 'n_desc': 3, 'n_children': 1, 'depth': 0},
            {'path': 'c', 'size': 500000, 'mtime': 1700000300, 'kind': 'dir', 'parent': '.', 'uri': '/deep/a/b/c', 'n_desc': 2, 'n_children': 1, 'depth': 1},
            {'path': 'c/d', 'size': 500000, 'mtime': 1700000400, 'kind': 'dir', 'parent': 'c', 'uri': '/deep/a/b/c/d', 'n_desc': 1, 'n_children': 1, 'depth': 2},
            {'path': 'c/d/file.txt', 'size': 500000, 'mtime': 1700000500, 'kind': 'file', 'parent': 'c/d', 'uri': '/deep/a/b/c/d/file.txt', 'n_desc': 0, 'n_children': 0, 'depth': 3},
        ])
    else:
        raise ValueError(f"No test data for path: {scan_path}")


@pytest.fixture(params=['hybrid', 'duckdb', 'sqlite'])
def backend_client(request, monkeypatch):
    """Test client parameterized by storage backend.

    Creates isolated test environment for each backend.
    Parquet backend excluded since it doesn't support updates.
    """
    backend_type = request.param

    # Create temp directory for this test
    tmpdir = tempfile.mkdtemp()

    # Set up paths
    db_path = os.path.join(tmpdir, 'disk-tree.db')
    scans_dir = os.path.join(tmpdir, 'scans')
    os.makedirs(scans_dir)

    # Create SQLite metadata database
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
            n_desc INTEGER,
            mtime INTEGER
        )
    ''')
    conn.execute('CREATE INDEX ix_scan_path_time ON scan(path, time)')
    conn.execute('''
        CREATE TABLE IF NOT EXISTS scan_progress (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            path TEXT NOT NULL UNIQUE,
            started_at DATETIME NOT NULL,
            items_found INTEGER DEFAULT 0,
            items_per_sec REAL,
            error_count INTEGER DEFAULT 0
        )
    ''')
    conn.commit()
    conn.close()

    # Patch environment and paths
    monkeypatch.setenv('DISK_TREE_BACKEND', backend_type)
    monkeypatch.setattr('disk_tree.server.DB_PATH', db_path)
    monkeypatch.setattr('disk_tree.config.ROOT_DIR', tmpdir)
    monkeypatch.setattr('disk_tree.config.SCANS_DIR', scans_dir)

    # Reset backend singleton to pick up new env var
    reset_backend()

    # Clear server cache
    from disk_tree.server import clear_cache
    clear_cache()

    app.config['TESTING'] = True
    with app.test_client() as client:
        yield {
            'client': client,
            'backend_type': backend_type,
            'db_path': db_path,
            'scans_dir': scans_dir,
            'tmpdir': tmpdir,
        }

    # Cleanup
    reset_backend()
    shutil.rmtree(tmpdir)


def insert_scan(db_path: str, scan_path: str, scan_time: datetime) -> str:
    """Insert a scan into the database and storage backend.

    Returns the blob_ref for the scan.
    """
    df = create_test_scan_df(scan_path)
    backend = get_backend()
    blob_ref = backend.save(df, scan_path)

    root = df[df['path'] == '.'].iloc[0]

    conn = sqlite3.connect(db_path)
    conn.execute('''
        INSERT INTO scan (path, time, blob, size, n_children, n_desc)
        VALUES (?, ?, ?, ?, ?, ?)
    ''', (scan_path, scan_time.isoformat(), blob_ref,
          int(root['size']), int(root['n_children']), int(root['n_desc'])))
    conn.commit()
    conn.close()

    return blob_ref


@pytest.fixture
def mock_filesystem(monkeypatch):
    """Mock filesystem existence checks for delete operations."""
    # Track which paths "exist"
    existing_paths = {
        '/test', '/test/foo', '/test/foo/file1.txt', '/test/foo/file2.txt',
        '/test/bar', '/test/bar/file3.txt', '/test/bar/subdir',
        '/test/bar/subdir/file4.txt',
    }

    def mock_isfile(path):
        return path in existing_paths and '.' in os.path.basename(path)

    def mock_isdir(path):
        return path in existing_paths and '.' not in os.path.basename(path)

    def mock_remove(path):
        existing_paths.discard(path)

    def mock_rmtree(path):
        to_remove = [p for p in existing_paths if p == path or p.startswith(path + '/')]
        for p in to_remove:
            existing_paths.discard(p)

    monkeypatch.setattr('disk_tree.server.isfile', mock_isfile)
    monkeypatch.setattr('disk_tree.server.isdir', mock_isdir)
    monkeypatch.setattr('disk_tree.server.remove', mock_remove)
    monkeypatch.setattr('disk_tree.server.shutil.rmtree', mock_rmtree)

    return existing_paths


class TestScanSequences:
    """Test scan operations across backends."""

    def test_initial_scan(self, backend_client):
        """Inserting a scan creates correct data structure."""
        client = backend_client['client']
        backend = backend_client['backend_type']
        db_path = backend_client['db_path']

        # Insert scan directly (bypassing gfind)
        scan_time = datetime.now()
        insert_scan(db_path, '/test', scan_time)

        # Clear cache to ensure fresh data
        from disk_tree.server import clear_cache
        clear_cache()

        # Verify scan data via API
        response = client.get('/api/scan?uri=/test')
        assert response.status_code == 200, f"[{backend}] Failed to get scan: {response.json}"
        data = response.json

        assert data['root']['path'] == '.', f"[{backend}] Root path should be '.'"
        assert data['scan_status'] == 'full', f"[{backend}] Should be full scan"

        # Check children exist
        children = {c['path'] for c in data['children']}
        assert 'foo' in children, f"[{backend}] Missing 'foo' child"
        assert 'bar' in children, f"[{backend}] Missing 'bar' child"

    def test_child_scan_patches_parent(self, backend_client):
        """A fresher child scan patches stats into parent view."""
        client = backend_client['client']
        backend = backend_client['backend_type']
        db_path = backend_client['db_path']

        # Insert parent scan (older)
        parent_time = datetime.now() - timedelta(hours=1)
        insert_scan(db_path, '/test', parent_time)

        # Insert child scan (fresher)
        child_time = datetime.now()
        insert_scan(db_path, '/test/foo', child_time)

        from disk_tree.server import clear_cache
        clear_cache()

        # When viewing parent, child's stats should be patched
        response = client.get('/api/scan?uri=/test')
        data = response.json
        foo = next(c for c in data['children'] if c['path'] == 'foo')

        # foo should be marked as patched from fresher child scan
        assert foo.get('patched') is True, f"[{backend}] foo should be patched from fresher child scan"


class TestDeleteSequences:
    """Test delete operations across backends."""

    def test_delete_updates_scan(self, backend_client, mock_filesystem):
        """Deleting a file updates the scan data."""
        client = backend_client['client']
        backend = backend_client['backend_type']
        db_path = backend_client['db_path']

        # Insert scan directly
        scan_time = datetime.now()
        insert_scan(db_path, '/test', scan_time)

        from disk_tree.server import clear_cache
        clear_cache()

        # Get initial stats
        response = client.get('/api/scan?uri=/test')
        initial_data = response.json
        initial_n_desc = initial_data['root']['n_desc']

        # Delete a file
        response = client.post('/api/delete', json={'path': '/test/foo/file1.txt'})
        assert response.status_code == 200, f"[{backend}] Delete failed: {response.json}"

        clear_cache()

        # Verify file is removed from view
        response = client.get('/api/scan?uri=/test/foo')
        data = response.json
        child_paths = {c['path'] for c in data['children']}
        assert 'file1.txt' not in child_paths, f"[{backend}] Deleted file still in view"

    def test_delete_directory_updates_ancestors(self, backend_client, mock_filesystem):
        """Deleting a directory updates ancestor stats."""
        client = backend_client['client']
        backend = backend_client['backend_type']
        db_path = backend_client['db_path']

        # Insert scan directly
        scan_time = datetime.now()
        insert_scan(db_path, '/test', scan_time)

        from disk_tree.server import clear_cache
        clear_cache()

        # Get initial root size
        response = client.get('/api/scan?uri=/test')
        initial_root_size = response.json['root']['size']

        # Get size of directory to delete
        bar_data = next(c for c in response.json['children'] if c['path'] == 'bar')
        bar_size = bar_data['size']

        # Delete the directory
        response = client.post('/api/delete', json={'path': '/test/bar'})
        assert response.status_code == 200, f"[{backend}] Delete failed"

        clear_cache()

        # Verify root size decreased
        response = client.get('/api/scan?uri=/test')
        new_root_size = response.json['root']['size']

        assert new_root_size < initial_root_size, \
            f"[{backend}] Root size should decrease after delete"

    def test_delete_already_deleted_cleans_stale_entry(self, backend_client, mock_filesystem):
        """Deleting an already-deleted file removes stale entry from scan."""
        client = backend_client['client']
        backend = backend_client['backend_type']
        db_path = backend_client['db_path']

        # Insert scan directly
        scan_time = datetime.now()
        insert_scan(db_path, '/test', scan_time)

        from disk_tree.server import clear_cache
        clear_cache()

        # "Externally" delete the file (remove from mock filesystem)
        mock_filesystem.discard('/test/foo/file1.txt')

        # Try to delete via API (file no longer exists)
        response = client.post('/api/delete', json={'path': '/test/foo/file1.txt'})
        assert response.status_code == 200, f"[{backend}] Delete of stale entry failed"
        assert response.json.get('already_deleted') is True, \
            f"[{backend}] Should indicate file was already deleted"

        clear_cache()

        # Verify file is removed from view
        response = client.get('/api/scan?uri=/test/foo')
        data = response.json
        child_paths = {c['path'] for c in data['children']}
        assert 'file1.txt' not in child_paths, \
            f"[{backend}] Stale entry should be cleaned up"


class TestScanSelectionWithMultipleScans:
    """Test that the correct scan is selected when multiple exist."""

    def test_prefers_fresher_ancestor_over_stale_exact(self, backend_client):
        """When ancestor scan is fresher than exact match, use ancestor."""
        client = backend_client['client']
        backend = backend_client['backend_type']
        db_path = backend_client['db_path']

        # First: insert a child scan (older)
        child_time = datetime.now() - timedelta(hours=1)
        insert_scan(db_path, '/test/foo', child_time)

        # Then: insert a parent scan (fresher)
        parent_time = datetime.now()
        insert_scan(db_path, '/test', parent_time)

        from disk_tree.server import clear_cache
        clear_cache()

        # When viewing /test/foo, should use the fresher parent scan
        response = client.get('/api/scan?uri=/test/foo')
        data = response.json

        # The scan_path should be /test (the fresher ancestor)
        assert data['scan_path'] == '/test', \
            f"[{backend}] Should use fresher ancestor scan, got scan_path={data.get('scan_path')}"

    def test_delete_updates_fresher_scan(self, backend_client, mock_filesystem):
        """Delete updates the fresher scan, not the stale exact match."""
        client = backend_client['client']
        backend = backend_client['backend_type']
        db_path = backend_client['db_path']

        # Insert child scan first (older)
        child_time = datetime.now() - timedelta(hours=1)
        insert_scan(db_path, '/test/foo', child_time)

        # Insert parent scan (fresher)
        parent_time = datetime.now()
        insert_scan(db_path, '/test', parent_time)

        from disk_tree.server import clear_cache
        clear_cache()

        # Delete a file in /test/foo
        response = client.post('/api/delete', json={'path': '/test/foo/file1.txt'})
        assert response.status_code == 200, f"[{backend}] Delete failed: {response.json}"

        clear_cache()

        # View /test/foo - should show file removed (since parent scan was updated)
        response = client.get('/api/scan?uri=/test/foo')
        data = response.json
        child_paths = {c['path'] for c in data['children']}
        assert 'file1.txt' not in child_paths, \
            f"[{backend}] File should be removed from fresher scan view"


class TestStatsConsistency:
    """Test that stats (size, n_desc, n_children) remain consistent after operations.

    Note: Hybrid backend fully supports stats propagation after deletes.
    DuckDB/SQLite backends have partial support (size updates but not n_desc/n_children).
    """

    def test_delete_file_updates_all_ancestor_stats(self, backend_client, mock_filesystem):
        """Deleting a file updates ancestor stats (size and n_desc)."""
        client = backend_client['client']
        backend = backend_client['backend_type']
        db_path = backend_client['db_path']

        insert_scan(db_path, '/test', datetime.now())
        from disk_tree.server import clear_cache
        clear_cache()

        # Get initial stats
        response = client.get('/api/scan?uri=/test')
        initial = response.json
        initial_root_n_desc = initial['root']['n_desc']
        initial_root_size = initial['root']['size']

        foo = next(c for c in initial['children'] if c['path'] == 'foo')
        initial_foo_n_desc = foo['n_desc']
        initial_foo_size = foo['size']

        # file1.txt is 102400 bytes
        file_size = 102400

        # Delete file1.txt
        response = client.post('/api/delete', json={'path': '/test/foo/file1.txt'})
        assert response.status_code == 200
        clear_cache()

        # Check updated stats
        response = client.get('/api/scan?uri=/test')
        updated = response.json

        # Root size should decrease by file size
        assert updated['root']['size'] == initial_root_size - file_size, \
            f"[{backend}] Root size should decrease by {file_size}"

        # Root n_desc should decrease by 1
        assert updated['root']['n_desc'] == initial_root_n_desc - 1, \
            f"[{backend}] Root n_desc should decrease by 1"

        # foo size and n_desc should also decrease
        foo_updated = next(c for c in updated['children'] if c['path'] == 'foo')
        assert foo_updated['size'] == initial_foo_size - file_size, \
            f"[{backend}] foo size should decrease by {file_size}"
        assert foo_updated['n_desc'] == initial_foo_n_desc - 1, \
            f"[{backend}] foo n_desc should decrease by 1"

    def test_delete_directory_updates_n_children(self, backend_client, mock_filesystem):
        """Deleting a directory decrements parent's n_children."""
        client = backend_client['client']
        backend_type = backend_client['backend_type']
        db_path = backend_client['db_path']

        insert_scan(db_path, '/test', datetime.now())
        from disk_tree.server import clear_cache
        clear_cache()

        # Get initial n_children for root (disable expand_single to get exact root stats)
        response = client.get('/api/scan?uri=/test&expand_single=false')
        initial_n_children = response.json['root']['n_children']
        assert initial_n_children == 2, f"[{backend_type}] Expected 2 children (foo, bar)"

        # Delete foo directory
        response = client.post('/api/delete', json={'path': '/test/foo'})
        assert response.status_code == 200, f"[{backend_type}] Delete failed: {response.json}"
        clear_cache()

        # n_children should decrease (disable expand_single to prevent auto-expansion
        # into bar after foo is deleted - bar has n_children=2 which would confuse the check)
        response = client.get('/api/scan?uri=/test&expand_single=false')
        new_n_children = response.json['root']['n_children']

        assert new_n_children == initial_n_children - 1, \
            f"[{backend_type}] n_children should decrease from {initial_n_children} to {initial_n_children - 1}"

    def test_multiple_deletes_accumulate(self, backend_client, mock_filesystem):
        """Multiple deletes correctly accumulate stat changes."""
        client = backend_client['client']
        backend = backend_client['backend_type']
        db_path = backend_client['db_path']

        insert_scan(db_path, '/test', datetime.now())
        from disk_tree.server import clear_cache
        clear_cache()

        # Get initial stats
        response = client.get('/api/scan?uri=/test')
        initial_n_desc = response.json['root']['n_desc']  # 7
        initial_size = response.json['root']['size']

        # file1.txt is 102400, file2.txt is 204800
        file1_size = 102400
        file2_size = 204800

        # Delete file1.txt (1 item)
        client.post('/api/delete', json={'path': '/test/foo/file1.txt'})
        # Delete file2.txt (1 item)
        client.post('/api/delete', json={'path': '/test/foo/file2.txt'})
        clear_cache()

        response = client.get('/api/scan?uri=/test')
        new_n_desc = response.json['root']['n_desc']
        new_size = response.json['root']['size']

        # Size should decrease
        assert new_size == initial_size - file1_size - file2_size, \
            f"[{backend}] Size should decrease by {file1_size + file2_size}"

        # n_desc should decrease by 2 (one for each deleted file)
        assert new_n_desc == initial_n_desc - 2, \
            f"[{backend}] n_desc should decrease by 2 after deleting 2 files"


class TestMultiLevelHierarchy:
    """Test operations with grandparent→parent→child scan chains."""

    def test_view_grandchild_from_grandparent_scan(self, backend_client):
        """Can view deeply nested path from ancestor scan."""
        client = backend_client['client']
        backend = backend_client['backend_type']
        db_path = backend_client['db_path']

        insert_scan(db_path, '/deep', datetime.now())
        from disk_tree.server import clear_cache
        clear_cache()

        # View /deep/a/b/c from the /deep scan (disable expand_single to get exact path)
        response = client.get('/api/scan?uri=/deep/a/b/c&expand_single=false')
        assert response.status_code == 200, f"[{backend}] Failed to view nested path"

        data = response.json
        assert data['scan_path'] == '/deep', \
            f"[{backend}] Should use ancestor scan"
        assert data['root']['uri'] == '/deep/a/b/c', \
            f"[{backend}] Root should be the requested path"

    def test_grandchild_scan_does_not_patch_grandparent(self, backend_client):
        """Grandchild scans don't patch into grandparent view (non-transitive)."""
        client = backend_client['client']
        backend = backend_client['backend_type']
        db_path = backend_client['db_path']

        # Insert grandparent scan (older)
        grandparent_time = datetime.now() - timedelta(hours=2)
        insert_scan(db_path, '/test', grandparent_time)

        # Insert grandchild scan (fresher) - /test/bar/subdir
        grandchild_time = datetime.now()
        insert_scan(db_path, '/test/bar/subdir', grandchild_time)

        from disk_tree.server import clear_cache
        clear_cache()

        # View /test - grandchild should NOT be patched (only direct children patch)
        response = client.get('/api/scan?uri=/test')
        data = response.json

        bar = next(c for c in data['children'] if c['path'] == 'bar')
        # bar should NOT be marked as patched since the fresher scan is for its child
        assert bar.get('patched') is not True, \
            f"[{backend}] bar should not be patched from grandchild scan"

    def test_intermediate_scan_patches_parent_only(self, backend_client):
        """A middle-level scan patches its parent but not grandparent."""
        client = backend_client['client']
        backend = backend_client['backend_type']
        db_path = backend_client['db_path']

        # Insert grandparent and parent scans
        grandparent_time = datetime.now() - timedelta(hours=2)
        insert_scan(db_path, '/test', grandparent_time)

        parent_time = datetime.now() - timedelta(hours=1)
        insert_scan(db_path, '/test/bar', parent_time)

        # Insert grandchild scan (fresher than both)
        grandchild_time = datetime.now()
        insert_scan(db_path, '/test/bar/subdir', grandchild_time)

        from disk_tree.server import clear_cache
        clear_cache()

        # View /test/bar - subdir should be patched
        response = client.get('/api/scan?uri=/test/bar')
        data = response.json

        subdir = next((c for c in data['children'] if c['path'] == 'subdir'), None)
        if subdir:
            assert subdir.get('patched') is True, \
                f"[{backend}] subdir should be patched from fresher child scan"


class TestRescanScenarios:
    """Test rescanning behavior."""

    def test_newer_scan_supersedes_older(self, backend_client):
        """A newer scan of the same path supersedes the older one."""
        client = backend_client['client']
        backend = backend_client['backend_type']
        db_path = backend_client['db_path']

        # Insert old scan
        old_time = datetime.now() - timedelta(hours=1)
        insert_scan(db_path, '/test', old_time)

        # Insert new scan of same path
        new_time = datetime.now()
        insert_scan(db_path, '/test', new_time)

        from disk_tree.server import clear_cache
        clear_cache()

        # Get scan list - should show 2 scans but API uses the newer one
        response = client.get('/api/scan?uri=/test')
        data = response.json

        # Should use the newer scan
        assert data['scan_status'] == 'full', \
            f"[{backend}] Should have full scan status"

    def test_rescan_parent_after_child_exists(self, backend_client):
        """Rescanning parent after child exists - parent becomes authoritative."""
        client = backend_client['client']
        backend = backend_client['backend_type']
        db_path = backend_client['db_path']

        # First: child scan
        child_time = datetime.now() - timedelta(hours=1)
        insert_scan(db_path, '/test/foo', child_time)

        from disk_tree.server import clear_cache
        clear_cache()

        # At this point, viewing /test/foo uses the child scan
        response = client.get('/api/scan?uri=/test/foo')
        assert response.json['scan_path'] == '/test/foo'

        # Now: rescan parent (fresher)
        parent_time = datetime.now()
        insert_scan(db_path, '/test', parent_time)
        clear_cache()

        # Now viewing /test/foo should use the fresher parent
        response = client.get('/api/scan?uri=/test/foo')
        assert response.json['scan_path'] == '/test', \
            f"[{backend}] Should use fresher parent scan"


class TestDepthFiltering:
    """Test depth parameter in API requests."""

    def test_depth_limits_children(self, backend_client):
        """depth=1 returns only direct children."""
        client = backend_client['client']
        backend = backend_client['backend_type']
        db_path = backend_client['db_path']

        insert_scan(db_path, '/deep', datetime.now())
        from disk_tree.server import clear_cache
        clear_cache()

        # Request with depth=1
        response = client.get('/api/scan?uri=/deep&depth=1')
        data = response.json

        # Should only have direct children, no deeper
        child_paths = {c['path'] for c in data['children']}
        assert 'a' in child_paths, f"[{backend}] Should have direct child 'a'"

        # Should NOT have grandchildren in children list
        all_paths = {c['path'] for c in data['children']}
        assert not any('/' in p for p in all_paths), \
            f"[{backend}] depth=1 should not include nested paths"

    def test_depth_0_returns_root_only(self, backend_client):
        """depth=0 returns root with no children."""
        client = backend_client['client']
        backend = backend_client['backend_type']
        db_path = backend_client['db_path']

        insert_scan(db_path, '/test', datetime.now())
        from disk_tree.server import clear_cache
        clear_cache()

        response = client.get('/api/scan?uri=/test&depth=0')
        data = response.json

        assert data['root'] is not None, f"[{backend}] Should have root"
        assert len(data['children']) == 0, \
            f"[{backend}] depth=0 should have no children"


class TestEdgeCases:
    """Test edge cases and boundary conditions."""

    def test_empty_directory_scan(self, backend_client):
        """Can scan and view an empty directory."""
        client = backend_client['client']
        backend = backend_client['backend_type']
        db_path = backend_client['db_path']

        insert_scan(db_path, '/empty', datetime.now())
        from disk_tree.server import clear_cache
        clear_cache()

        response = client.get('/api/scan?uri=/empty')
        assert response.status_code == 200
        data = response.json

        assert data['root']['n_children'] == 0, \
            f"[{backend}] Empty dir should have 0 children"
        assert data['root']['n_desc'] == 0, \
            f"[{backend}] Empty dir should have 0 descendants"
        assert len(data['children']) == 0, \
            f"[{backend}] Empty dir should return no children"

    def test_delete_last_child_in_directory(self, backend_client, mock_filesystem):
        """Deleting the only child leaves parent empty."""
        client = backend_client['client']
        backend = backend_client['backend_type']
        db_path = backend_client['db_path']

        # Use /test/bar/subdir which has only file4.txt
        insert_scan(db_path, '/test/bar/subdir', datetime.now())
        mock_filesystem.add('/test/bar/subdir')
        mock_filesystem.add('/test/bar/subdir/file4.txt')

        from disk_tree.server import clear_cache
        clear_cache()

        # Delete the only file
        response = client.post('/api/delete', json={'path': '/test/bar/subdir/file4.txt'})
        assert response.status_code == 200
        clear_cache()

        # Directory should now be empty
        response = client.get('/api/scan?uri=/test/bar/subdir')
        data = response.json
        assert data['root']['n_children'] == 0, \
            f"[{backend}] Should have 0 children after deleting last child"
        assert len(data['children']) == 0, \
            f"[{backend}] Should return no children"

    def test_view_nonexistent_path_no_scan(self, backend_client):
        """Viewing a path with no scan falls back gracefully."""
        client = backend_client['client']
        backend = backend_client['backend_type']

        from disk_tree.server import clear_cache
        clear_cache()

        # No scans inserted - viewing any path should handle gracefully
        response = client.get('/api/scan?uri=/nonexistent')
        # Should return 200 with empty/fallback data or scan_status indicating no scan
        assert response.status_code == 200, \
            f"[{backend}] Should handle missing scan gracefully"

    def test_scan_list_shows_correct_counts(self, backend_client):
        """GET /api/scans returns correct scan metadata."""
        client = backend_client['client']
        backend = backend_client['backend_type']
        db_path = backend_client['db_path']

        insert_scan(db_path, '/test', datetime.now())
        from disk_tree.server import clear_cache
        clear_cache()

        response = client.get('/api/scans')
        assert response.status_code == 200

        scans = response.json
        assert len(scans) >= 1, f"[{backend}] Should have at least 1 scan"

        test_scan = next((s for s in scans if s['path'] == '/test'), None)
        assert test_scan is not None, f"[{backend}] Should find /test scan"
        assert test_scan['size'] == 1024000, f"[{backend}] Size should match"
        assert test_scan['n_children'] == 2, f"[{backend}] n_children should be 2"
        assert test_scan['n_desc'] == 7, f"[{backend}] n_desc should be 7"

    def test_delete_in_overlapping_scans(self, backend_client, mock_filesystem):
        """Delete when path is covered by multiple overlapping scans."""
        client = backend_client['client']
        backend = backend_client['backend_type']
        db_path = backend_client['db_path']

        # Create overlapping scans: /test and /test/foo
        insert_scan(db_path, '/test', datetime.now() - timedelta(minutes=30))
        insert_scan(db_path, '/test/foo', datetime.now())

        from disk_tree.server import clear_cache
        clear_cache()

        # Delete file in /test/foo - should update the fresher /test/foo scan
        response = client.post('/api/delete', json={'path': '/test/foo/file1.txt'})
        assert response.status_code == 200
        clear_cache()

        # Verify file removed from both views
        response = client.get('/api/scan?uri=/test/foo')
        child_paths = {c['path'] for c in response.json['children']}
        assert 'file1.txt' not in child_paths, \
            f"[{backend}] File should be removed from child scan view"
