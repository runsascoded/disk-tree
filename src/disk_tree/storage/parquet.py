import os
import time
from os import makedirs, remove
from os.path import exists, join
from uuid import uuid4

import pandas as pd
import pyarrow.parquet as pq

from .base import StorageBackend, PathStats
from ..config import SCANS_DIR


# LRU cache for parquet DataFrames
_cache: dict[str, tuple[float, pd.DataFrame]] = {}
CACHE_TTL = 300  # 5 minutes
CACHE_MAX_SIZE = 10


class ParquetBackend(StorageBackend):
    """Parquet-based storage backend.

    Stores each scan as a separate .parquet file. Immutable - does not support
    in-place updates. Best compression, but requires full rewrite for any changes.
    """

    def __init__(self, scans_dir: str | None = None):
        self.scans_dir = scans_dir or SCANS_DIR
        makedirs(self.scans_dir, exist_ok=True)

    @property
    def name(self) -> str:
        return "parquet"

    @property
    def supports_updates(self) -> bool:
        return False

    def save(self, df: pd.DataFrame, scan_path: str) -> str:
        """Save DataFrame to a new parquet file."""
        blob_id = str(uuid4())
        blob_path = join(self.scans_dir, f'{blob_id}.parquet')
        if exists(blob_path):
            raise RuntimeError(f"Blob path already exists: {blob_path}")
        df.to_parquet(blob_path, index=False)
        return blob_path

    def load(
        self,
        blob_ref: str,
        max_depth: int | None = None,
        min_depth: int | None = None,
    ) -> pd.DataFrame:
        """Load parquet with optional depth filtering via predicate pushdown."""
        cache_key = f"{blob_ref}:{min_depth}:{max_depth}"
        now = time.time()

        # Check cache
        if cache_key in _cache:
            cached_time, cached_df = _cache[cache_key]
            if now - cached_time < CACHE_TTL:
                return cached_df

        df = None
        if max_depth is not None or min_depth is not None:
            # Check if parquet has 'depth' column for predicate pushdown
            try:
                schema = pq.read_schema(blob_ref)
                if 'depth' in schema.names:
                    filters = []
                    if max_depth is not None:
                        filters.append(('depth', '<=', max_depth))
                    if min_depth is not None:
                        filters.append(('depth', '>=', min_depth))
                    df = pd.read_parquet(blob_ref, filters=filters)
            except Exception:
                pass  # Fall back to full load

        if df is None:
            df = pd.read_parquet(blob_ref)

        # Update cache (simple LRU)
        if len(_cache) >= CACHE_MAX_SIZE:
            oldest_key = min(_cache.keys(), key=lambda k: _cache[k][0])
            del _cache[oldest_key]
        _cache[cache_key] = (now, df)

        return df

    def get_path_stats(self, blob_ref: str, rel_path: str) -> PathStats | None:
        """Get stats for a path by loading just that depth level."""
        target_depth = rel_path.count('/') + 1 if rel_path else 0
        df = self.load(blob_ref, max_depth=target_depth, min_depth=target_depth)
        match = df[df['path'] == rel_path]
        if match.empty:
            return None
        row = match.iloc[0]
        return PathStats(
            size=int(row['size']),
            n_desc=int(row.get('n_desc', 1) or 1),
            n_children=int(row.get('n_children', 0) or 0),
            mtime=float(row['mtime']) if pd.notna(row.get('mtime')) else None,
        )

    def delete(self, blob_ref: str) -> None:
        """Delete the parquet file."""
        if exists(blob_ref):
            remove(blob_ref)
        # Clear from cache
        keys_to_remove = [k for k in _cache if k.startswith(blob_ref)]
        for k in keys_to_remove:
            del _cache[k]

    def clear_cache(self):
        """Clear the in-memory cache."""
        _cache.clear()
