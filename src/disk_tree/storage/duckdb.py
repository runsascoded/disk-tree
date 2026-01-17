import os
from os import makedirs
from os.path import exists, join
from uuid import uuid4

import pandas as pd

from .base import StorageBackend, PathStats
from ..config import ROOT_DIR


class DuckDBBackend(StorageBackend):
    """DuckDB-based storage backend.

    Stores all scan data in a single DuckDB database file. Supports efficient
    in-place updates (delete, modify) without rewriting entire dataset.
    Good compression (columnar), fast analytical queries.
    """

    def __init__(self, db_path: str | None = None):
        import duckdb
        self.db_path = db_path or join(ROOT_DIR, 'scans.duckdb')
        makedirs(os.path.dirname(self.db_path), exist_ok=True)
        self.conn = duckdb.connect(self.db_path)
        self._init_schema()

    def _init_schema(self):
        """Create tables if they don't exist."""
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS scan_data (
                blob_ref VARCHAR NOT NULL,
                path VARCHAR NOT NULL,
                size BIGINT,
                mtime DOUBLE,
                kind VARCHAR,
                parent VARCHAR,
                uri VARCHAR,
                n_desc BIGINT,
                n_children BIGINT,
                depth INTEGER,
                PRIMARY KEY (blob_ref, path)
            )
        ''')
        # Index for efficient depth filtering
        self.conn.execute('''
            CREATE INDEX IF NOT EXISTS idx_scan_data_depth
            ON scan_data (blob_ref, depth)
        ''')

    @property
    def name(self) -> str:
        return "duckdb"

    @property
    def supports_updates(self) -> bool:
        return True

    def save(self, df: pd.DataFrame, scan_path: str) -> str:
        """Save DataFrame to DuckDB with a unique blob_ref."""
        blob_ref = f"ddb:{uuid4()}"

        # Add blob_ref column
        df = df.copy()
        df['blob_ref'] = blob_ref

        # Insert into table
        self.conn.execute('''
            INSERT INTO scan_data
            SELECT blob_ref, path, size, mtime, kind, parent, uri, n_desc, n_children, depth
            FROM df
        ''')

        return blob_ref

    def load(
        self,
        blob_ref: str,
        max_depth: int | None = None,
        min_depth: int | None = None,
    ) -> pd.DataFrame:
        """Load scan data with optional depth filtering."""
        query = "SELECT path, size, mtime, kind, parent, uri, n_desc, n_children, depth FROM scan_data WHERE blob_ref = ?"
        params = [blob_ref]

        if max_depth is not None:
            query += " AND depth <= ?"
            params.append(max_depth)
        if min_depth is not None:
            query += " AND depth >= ?"
            params.append(min_depth)

        return self.conn.execute(query, params).fetchdf()

    def get_path_stats(self, blob_ref: str, rel_path: str) -> PathStats | None:
        """Get stats for a specific path."""
        result = self.conn.execute('''
            SELECT size, n_desc, n_children, mtime
            FROM scan_data
            WHERE blob_ref = ? AND path = ?
        ''', [blob_ref, rel_path]).fetchone()

        if not result:
            return None

        return PathStats(
            size=int(result[0]) if result[0] is not None else 0,
            n_desc=int(result[1]) if result[1] is not None else 1,
            n_children=int(result[2]) if result[2] is not None else 0,
            mtime=float(result[3]) if result[3] is not None else None,
        )

    def delete(self, blob_ref: str) -> None:
        """Delete all data for a scan."""
        self.conn.execute('DELETE FROM scan_data WHERE blob_ref = ?', [blob_ref])

    def _delete_path_impl(self, blob_ref: str, rel_path: str) -> PathStats | None:
        """Delete a path and its descendants, updating ancestor stats."""
        # Get stats before deletion
        stats = self.get_path_stats(blob_ref, rel_path)
        if not stats:
            return None

        deleted_size = stats.size
        deleted_n_desc = stats.n_desc

        # Delete the path and all descendants
        self.conn.execute('''
            DELETE FROM scan_data
            WHERE blob_ref = ? AND (path = ? OR path LIKE ?)
        ''', [blob_ref, rel_path, f"{rel_path}/%"])

        # Update all ancestor directories
        # Build list of ancestors (e.g., 'a/b/c' -> ['a/b', 'a', '.'])
        ancestors = []
        parts = rel_path.split('/')
        for i in range(len(parts) - 1, 0, -1):
            ancestors.append('/'.join(parts[:i]))
        ancestors.append('.')

        for ancestor in ancestors:
            # Update size and n_desc
            self.conn.execute('''
                UPDATE scan_data
                SET size = size - ?,
                    n_desc = n_desc - ?
                WHERE blob_ref = ? AND path = ?
            ''', [deleted_size, deleted_n_desc, blob_ref, ancestor])

        # Update n_children for direct parent only
        if '/' in rel_path:
            parent = '/'.join(parts[:-1])
        else:
            parent = '.'

        self.conn.execute('''
            UPDATE scan_data
            SET n_children = n_children - 1
            WHERE blob_ref = ? AND path = ?
        ''', [blob_ref, parent])

        return stats

    def vacuum(self):
        """Reclaim space after deletions."""
        self.conn.execute('VACUUM')

    def get_stats(self) -> dict:
        """Get storage statistics."""
        result = self.conn.execute('''
            SELECT
                COUNT(DISTINCT blob_ref) as num_scans,
                COUNT(*) as total_rows,
                SUM(size) as total_size_tracked
            FROM scan_data
        ''').fetchone()

        db_size = os.path.getsize(self.db_path) if exists(self.db_path) else 0

        return {
            'num_scans': result[0],
            'total_rows': result[1],
            'total_size_tracked': result[2],
            'db_file_size': db_size,
        }
