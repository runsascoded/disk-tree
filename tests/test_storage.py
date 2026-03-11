"""Tests for storage backends."""
import os
import tempfile

import pandas as pd
import pytest

from disk_tree.storage.base import PathStats
from disk_tree.storage.parquet import ParquetBackend
from disk_tree.storage.duckdb import DuckDBBackend
from disk_tree.storage.sqlite import SQLiteBackend
from disk_tree.storage.hybrid import HybridBackend


@pytest.fixture
def sample_df():
    """Create a sample scan DataFrame.

    n_desc = number of descendants (not including self)
    Files have n_desc=0, directories count their descendants.
    """
    return pd.DataFrame([
        {'path': '.', 'size': 1000, 'mtime': 1000.0, 'kind': 'dir', 'parent': '', 'uri': '/test', 'n_desc': 5, 'n_children': 2, 'depth': 0},
        {'path': 'foo', 'size': 400, 'mtime': 1001.0, 'kind': 'dir', 'parent': '.', 'uri': '/test/foo', 'n_desc': 2, 'n_children': 2, 'depth': 1},
        {'path': 'bar', 'size': 600, 'mtime': 1002.0, 'kind': 'dir', 'parent': '.', 'uri': '/test/bar', 'n_desc': 1, 'n_children': 1, 'depth': 1},
        {'path': 'foo/a.txt', 'size': 100, 'mtime': 1003.0, 'kind': 'file', 'parent': 'foo', 'uri': '/test/foo/a.txt', 'n_desc': 0, 'n_children': 0, 'depth': 2},
        {'path': 'foo/b.txt', 'size': 300, 'mtime': 1004.0, 'kind': 'file', 'parent': 'foo', 'uri': '/test/foo/b.txt', 'n_desc': 0, 'n_children': 0, 'depth': 2},
        {'path': 'bar/c.txt', 'size': 600, 'mtime': 1005.0, 'kind': 'file', 'parent': 'bar', 'uri': '/test/bar/c.txt', 'n_desc': 0, 'n_children': 0, 'depth': 2},
    ])


class TestParquetBackend:
    def test_save_and_load(self, sample_df):
        with tempfile.TemporaryDirectory() as tmpdir:
            backend = ParquetBackend(scans_dir=tmpdir)
            blob_ref = backend.save(sample_df, '/test')

            loaded = backend.load(blob_ref)
            assert len(loaded) == 6
            assert set(loaded['path'].tolist()) == {'.', 'foo', 'bar', 'foo/a.txt', 'foo/b.txt', 'bar/c.txt'}

    def test_depth_filtering(self, sample_df):
        with tempfile.TemporaryDirectory() as tmpdir:
            backend = ParquetBackend(scans_dir=tmpdir)
            blob_ref = backend.save(sample_df, '/test')

            # Load only depth 1
            loaded = backend.load(blob_ref, max_depth=1, min_depth=1)
            assert len(loaded) == 2
            assert set(loaded['path'].tolist()) == {'foo', 'bar'}

    def test_get_path_stats(self, sample_df):
        with tempfile.TemporaryDirectory() as tmpdir:
            backend = ParquetBackend(scans_dir=tmpdir)
            blob_ref = backend.save(sample_df, '/test')

            stats = backend.get_path_stats(blob_ref, 'foo')
            assert stats is not None
            assert stats.size == 400
            assert stats.n_desc == 2

    def test_does_not_support_updates(self, sample_df):
        with tempfile.TemporaryDirectory() as tmpdir:
            backend = ParquetBackend(scans_dir=tmpdir)
            assert backend.supports_updates is False
            blob_ref = backend.save(sample_df, '/test')
            result = backend.delete_path(blob_ref, 'foo/a.txt')
            assert result is None  # Not supported


class TestDuckDBBackend:
    def test_save_and_load(self, sample_df):
        with tempfile.TemporaryDirectory() as tmpdir:
            backend = DuckDBBackend(db_path=os.path.join(tmpdir, 'test.duckdb'))
            blob_ref = backend.save(sample_df, '/test')

            loaded = backend.load(blob_ref)
            assert len(loaded) == 6
            assert set(loaded['path'].tolist()) == {'.', 'foo', 'bar', 'foo/a.txt', 'foo/b.txt', 'bar/c.txt'}

    def test_depth_filtering(self, sample_df):
        with tempfile.TemporaryDirectory() as tmpdir:
            backend = DuckDBBackend(db_path=os.path.join(tmpdir, 'test.duckdb'))
            blob_ref = backend.save(sample_df, '/test')

            loaded = backend.load(blob_ref, max_depth=1, min_depth=1)
            assert len(loaded) == 2
            assert set(loaded['path'].tolist()) == {'foo', 'bar'}

    def test_get_path_stats(self, sample_df):
        with tempfile.TemporaryDirectory() as tmpdir:
            backend = DuckDBBackend(db_path=os.path.join(tmpdir, 'test.duckdb'))
            blob_ref = backend.save(sample_df, '/test')

            stats = backend.get_path_stats(blob_ref, 'foo')
            assert stats is not None
            assert stats.size == 400
            assert stats.n_desc == 2

    def test_delete_path_updates_ancestors(self, sample_df):
        with tempfile.TemporaryDirectory() as tmpdir:
            backend = DuckDBBackend(db_path=os.path.join(tmpdir, 'test.duckdb'))
            assert backend.supports_updates is True
            blob_ref = backend.save(sample_df, '/test')

            # Delete foo/a.txt (size=100, n_desc=1)
            stats = backend.delete_path(blob_ref, 'foo/a.txt')
            assert stats is not None
            assert stats.size == 100

            # Verify it's deleted
            assert backend.get_path_stats(blob_ref, 'foo/a.txt') is None

            # Verify parent 'foo' was updated
            foo_stats = backend.get_path_stats(blob_ref, 'foo')
            assert foo_stats.size == 300  # 400 - 100
            assert foo_stats.n_desc == 1  # 2 - 1
            assert foo_stats.n_children == 1  # 2 - 1

            # Verify root '.' was updated
            root_stats = backend.get_path_stats(blob_ref, '.')
            assert root_stats.size == 900  # 1000 - 100
            assert root_stats.n_desc == 4  # 5 - 1


class TestSQLiteBackend:
    def test_save_and_load(self, sample_df):
        with tempfile.TemporaryDirectory() as tmpdir:
            backend = SQLiteBackend(db_path=os.path.join(tmpdir, 'test.sqlite'))
            blob_ref = backend.save(sample_df, '/test')

            loaded = backend.load(blob_ref)
            assert len(loaded) == 6
            assert set(loaded['path'].tolist()) == {'.', 'foo', 'bar', 'foo/a.txt', 'foo/b.txt', 'bar/c.txt'}

    def test_depth_filtering(self, sample_df):
        with tempfile.TemporaryDirectory() as tmpdir:
            backend = SQLiteBackend(db_path=os.path.join(tmpdir, 'test.sqlite'))
            blob_ref = backend.save(sample_df, '/test')

            loaded = backend.load(blob_ref, max_depth=1, min_depth=1)
            assert len(loaded) == 2
            assert set(loaded['path'].tolist()) == {'foo', 'bar'}

    def test_get_path_stats(self, sample_df):
        with tempfile.TemporaryDirectory() as tmpdir:
            backend = SQLiteBackend(db_path=os.path.join(tmpdir, 'test.sqlite'))
            blob_ref = backend.save(sample_df, '/test')

            stats = backend.get_path_stats(blob_ref, 'foo')
            assert stats is not None
            assert stats.size == 400
            assert stats.n_desc == 2

    def test_delete_path_updates_ancestors(self, sample_df):
        with tempfile.TemporaryDirectory() as tmpdir:
            backend = SQLiteBackend(db_path=os.path.join(tmpdir, 'test.sqlite'))
            assert backend.supports_updates is True
            blob_ref = backend.save(sample_df, '/test')

            # Delete foo/a.txt (size=100, n_desc=1)
            stats = backend.delete_path(blob_ref, 'foo/a.txt')
            assert stats is not None
            assert stats.size == 100

            # Verify it's deleted
            assert backend.get_path_stats(blob_ref, 'foo/a.txt') is None

            # Verify parent 'foo' was updated
            foo_stats = backend.get_path_stats(blob_ref, 'foo')
            assert foo_stats.size == 300  # 400 - 100
            assert foo_stats.n_desc == 1  # 2 - 1
            assert foo_stats.n_children == 1  # 2 - 1

            # Verify root '.' was updated
            root_stats = backend.get_path_stats(blob_ref, '.')
            assert root_stats.size == 900  # 1000 - 100
            assert root_stats.n_desc == 4  # 5 - 1


class TestHybridBackend:
    def test_save_and_load_small(self, sample_df):
        """Small scans should not be chunked."""
        with tempfile.TemporaryDirectory() as tmpdir:
            backend = HybridBackend(scans_dir=tmpdir, chunk_threshold=1000)
            blob_ref = backend.save(sample_df, '/test')

            loaded = backend.load(blob_ref)
            assert len(loaded) == 6
            assert set(loaded['path'].tolist()) == {'.', 'foo', 'bar', 'foo/a.txt', 'foo/b.txt', 'bar/c.txt'}

    def test_depth_filtering(self, sample_df):
        with tempfile.TemporaryDirectory() as tmpdir:
            backend = HybridBackend(scans_dir=tmpdir)
            blob_ref = backend.save(sample_df, '/test')

            loaded = backend.load(blob_ref, max_depth=1, min_depth=1)
            assert len(loaded) == 2
            assert set(loaded['path'].tolist()) == {'foo', 'bar'}

    def test_get_path_stats(self, sample_df):
        with tempfile.TemporaryDirectory() as tmpdir:
            backend = HybridBackend(scans_dir=tmpdir)
            blob_ref = backend.save(sample_df, '/test')

            stats = backend.get_path_stats(blob_ref, 'foo')
            assert stats is not None
            assert stats.size == 400
            assert stats.n_desc == 2

    def test_chunking_large_subtrees(self):
        """Large subtrees should be split into separate parquets."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create a DataFrame with a large subtree
            rows = [
                {'path': '.', 'size': 10000, 'mtime': 1000.0, 'kind': 'dir', 'parent': '', 'uri': '/test', 'n_desc': 105, 'n_children': 2, 'depth': 0},
                {'path': 'small', 'size': 100, 'mtime': 1001.0, 'kind': 'dir', 'parent': '.', 'uri': '/test/small', 'n_desc': 2, 'n_children': 1, 'depth': 1},
                {'path': 'small/a.txt', 'size': 100, 'mtime': 1002.0, 'kind': 'file', 'parent': 'small', 'uri': '/test/small/a.txt', 'n_desc': 1, 'n_children': 0, 'depth': 2},
                {'path': 'large', 'size': 9900, 'mtime': 1003.0, 'kind': 'dir', 'parent': '.', 'uri': '/test/large', 'n_desc': 102, 'n_children': 100, 'depth': 1},
            ]
            # Add 100 files under 'large'
            for i in range(100):
                rows.append({
                    'path': f'large/f{i}.txt',
                    'size': 99,
                    'mtime': 1004.0 + i,
                    'kind': 'file',
                    'parent': 'large',
                    'uri': f'/test/large/f{i}.txt',
                    'n_desc': 1,
                    'n_children': 0,
                    'depth': 2,
                })
            df = pd.DataFrame(rows)

            # Use low threshold to trigger chunking
            backend = HybridBackend(scans_dir=tmpdir, chunk_threshold=50)
            blob_ref = backend.save(df, '/test')

            # Check that chunking occurred
            chunk_stats = backend.get_chunk_stats(blob_ref)
            assert chunk_stats['total_chunks'] == 1
            assert chunk_stats['chunks'][0]['path'] == 'large'

            # Verify we can still access all data
            stats = backend.get_path_stats(blob_ref, 'large/f50.txt')
            assert stats is not None
            assert stats.size == 99

            # Loading without follow_refs should only get summary
            summary = backend.load(blob_ref, follow_refs=False)
            assert 'large/f0.txt' not in summary['path'].tolist()
            assert 'large' in summary['path'].tolist()

    def test_delete_in_chunked_subtree(self):
        """Deleting a path inside a chunked subtree should work."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # n_desc = number of descendants (not including self)
            # files have n_desc=0, big has 50 descendants, root has 51 (big + 50 files)
            rows = [
                {'path': '.', 'size': 1000, 'mtime': 1000.0, 'kind': 'dir', 'parent': '', 'uri': '/test', 'n_desc': 51, 'n_children': 1, 'depth': 0},
                {'path': 'big', 'size': 1000, 'mtime': 1001.0, 'kind': 'dir', 'parent': '.', 'uri': '/test/big', 'n_desc': 50, 'n_children': 50, 'depth': 1},
            ]
            for i in range(50):
                rows.append({
                    'path': f'big/f{i}.txt',
                    'size': 20,
                    'mtime': 1002.0 + i,
                    'kind': 'file',
                    'parent': 'big',
                    'uri': f'/test/big/f{i}.txt',
                    'n_desc': 0,  # files have no descendants
                    'n_children': 0,
                    'depth': 2,
                })
            df = pd.DataFrame(rows)

            backend = HybridBackend(scans_dir=tmpdir, chunk_threshold=10)
            assert backend.supports_updates is True
            blob_ref = backend.save(df, '/test')

            # Delete a file inside the chunked subtree
            stats = backend.delete_path(blob_ref, 'big/f0.txt')
            assert stats is not None
            assert stats.size == 20

            # Verify it's deleted
            assert backend.get_path_stats(blob_ref, 'big/f0.txt') is None

            # Verify parent stats were updated
            big_stats = backend.get_path_stats(blob_ref, 'big')
            assert big_stats.size == 980  # 1000 - 20
            assert big_stats.n_desc == 49  # 50 - 1

            # Verify root was updated
            root_stats = backend.get_path_stats(blob_ref, '.')
            assert root_stats.size == 980
            assert root_stats.n_desc == 50  # 51 - 1
