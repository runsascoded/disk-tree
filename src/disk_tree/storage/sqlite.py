import os
import sqlite3
from os import makedirs
from os.path import exists, join
from uuid import uuid4

import pandas as pd

from .base import StorageBackend, PathStats
from ..config import ROOT_DIR


class SQLiteBackend(StorageBackend):
    """SQLite-based storage backend.

    Stores all scan data in a single SQLite database file. Supports efficient
    in-place updates. Row-based storage means larger file size than columnar
    formats, but reliable and widely compatible.
    """

    def __init__(self, db_path: str | None = None):
        self.db_path = db_path or join(ROOT_DIR, 'scans-data.sqlite')
        makedirs(os.path.dirname(self.db_path), exist_ok=True)
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._init_schema()

    def _init_schema(self):
        """Create tables if they don't exist."""
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS scan_data (
                blob_ref TEXT NOT NULL,
                path TEXT NOT NULL,
                size INTEGER,
                mtime REAL,
                kind TEXT,
                parent TEXT,
                uri TEXT,
                n_desc INTEGER,
                n_children INTEGER,
                depth INTEGER,
                PRIMARY KEY (blob_ref, path)
            )
        ''')
        # Index for efficient depth filtering
        self.conn.execute('''
            CREATE INDEX IF NOT EXISTS idx_scan_data_depth
            ON scan_data (blob_ref, depth)
        ''')
        # Index for parent lookups (for ancestor updates)
        self.conn.execute('''
            CREATE INDEX IF NOT EXISTS idx_scan_data_parent
            ON scan_data (blob_ref, parent)
        ''')
        self.conn.commit()

    @property
    def name(self) -> str:
        return "sqlite"

    @property
    def supports_updates(self) -> bool:
        return True

    def save(self, df: pd.DataFrame, scan_path: str) -> str:
        """Save DataFrame to SQLite with a unique blob_ref."""
        blob_ref = f"sqlite:{uuid4()}"

        # Insert rows in batches
        rows = []
        for _, row in df.iterrows():
            rows.append((
                blob_ref,
                row['path'],
                int(row['size']) if pd.notna(row['size']) else None,
                float(row['mtime']) if pd.notna(row.get('mtime')) else None,
                row.get('kind'),
                row.get('parent'),
                row.get('uri'),
                int(row['n_desc']) if pd.notna(row.get('n_desc')) else None,
                int(row['n_children']) if pd.notna(row.get('n_children')) else None,
                int(row['depth']) if pd.notna(row.get('depth')) else None,
            ))

        self.conn.executemany('''
            INSERT INTO scan_data (blob_ref, path, size, mtime, kind, parent, uri, n_desc, n_children, depth)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', rows)
        self.conn.commit()

        return blob_ref

    def load(
        self,
        blob_ref: str,
        max_depth: int | None = None,
        min_depth: int | None = None,
        follow_refs: bool = False,
    ) -> pd.DataFrame:
        """Load scan data with optional depth filtering.

        Args:
            follow_refs: Ignored (SQLite doesn't use chunked refs)
        """
        query = "SELECT path, size, mtime, kind, parent, uri, n_desc, n_children, depth FROM scan_data WHERE blob_ref = ?"
        params = [blob_ref]

        if max_depth is not None:
            query += " AND depth <= ?"
            params.append(max_depth)
        if min_depth is not None:
            query += " AND depth >= ?"
            params.append(min_depth)

        cursor = self.conn.execute(query, params)
        rows = cursor.fetchall()

        if not rows:
            return pd.DataFrame(columns=['path', 'size', 'mtime', 'kind', 'parent', 'uri', 'n_desc', 'n_children', 'depth'])

        return pd.DataFrame([dict(row) for row in rows])

    def get_path_stats(self, blob_ref: str, rel_path: str) -> PathStats | None:
        """Get stats for a specific path."""
        cursor = self.conn.execute('''
            SELECT size, n_desc, n_children, mtime
            FROM scan_data
            WHERE blob_ref = ? AND path = ?
        ''', [blob_ref, rel_path])
        result = cursor.fetchone()

        if not result:
            return None

        return PathStats(
            size=int(result['size']) if result['size'] is not None else 0,
            n_desc=int(result['n_desc']) if result['n_desc'] is not None else 1,
            n_children=int(result['n_children']) if result['n_children'] is not None else 0,
            mtime=float(result['mtime']) if result['mtime'] is not None else None,
        )

    def delete(self, blob_ref: str) -> None:
        """Delete all data for a scan."""
        self.conn.execute('DELETE FROM scan_data WHERE blob_ref = ?', [blob_ref])
        self.conn.commit()

    def _delete_path_impl(self, blob_ref: str, rel_path: str) -> PathStats | None:
        """Delete a path and its descendants, updating ancestor stats."""
        # Get stats before deletion
        stats = self.get_path_stats(blob_ref, rel_path)
        if not stats:
            return None

        deleted_size = stats.size
        # n_desc to subtract = item's n_desc + 1 (the item itself counts as a descendant of ancestors)
        deleted_n_desc = stats.n_desc + 1

        # Delete the path and all descendants
        self.conn.execute('''
            DELETE FROM scan_data
            WHERE blob_ref = ? AND (path = ? OR path LIKE ?)
        ''', [blob_ref, rel_path, f"{rel_path}/%"])

        # Update all ancestor directories
        ancestors = []
        parts = rel_path.split('/')
        for i in range(len(parts) - 1, 0, -1):
            ancestors.append('/'.join(parts[:i]))
        ancestors.append('.')

        for ancestor in ancestors:
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

        self.conn.commit()
        return stats

    def vacuum(self):
        """Reclaim space after deletions."""
        self.conn.execute('VACUUM')

    def get_stats(self) -> dict:
        """Get storage statistics."""
        cursor = self.conn.execute('''
            SELECT
                COUNT(DISTINCT blob_ref) as num_scans,
                COUNT(*) as total_rows,
                SUM(size) as total_size_tracked
            FROM scan_data
        ''')
        result = cursor.fetchone()

        db_size = os.path.getsize(self.db_path) if exists(self.db_path) else 0

        return {
            'num_scans': result['num_scans'],
            'total_rows': result['total_rows'],
            'total_size_tracked': result['total_size_tracked'],
            'db_file_size': db_size,
        }
