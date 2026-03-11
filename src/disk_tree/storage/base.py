from abc import ABC, abstractmethod
from dataclasses import dataclass

import pandas as pd


@dataclass
class PathStats:
    """Stats for a single path in a scan."""
    size: int
    n_desc: int
    n_children: int
    mtime: float | None = None


class StorageBackend(ABC):
    """Abstract interface for scan data storage.

    Implementations store the file tree data from scans. The SQLite metadata
    (Scan table with id, path, time, blob reference) remains separate.
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Human-readable name for this backend."""
        pass

    @property
    @abstractmethod
    def supports_updates(self) -> bool:
        """Whether this backend supports in-place updates (delete_path)."""
        pass

    @abstractmethod
    def save(self, df: pd.DataFrame, scan_path: str) -> str:
        """Save scan data and return a blob reference.

        Args:
            df: DataFrame with columns: path, size, mtime, kind, parent, uri, n_desc, n_children, depth
            scan_path: The root path that was scanned (for organizing storage)

        Returns:
            A blob reference string (file path, table name, etc.) to store in Scan.blob
        """
        pass

    @abstractmethod
    def load(
        self,
        blob_ref: str,
        max_depth: int | None = None,
        min_depth: int | None = None,
        follow_refs: bool = False,
    ) -> pd.DataFrame:
        """Load scan data with optional depth filtering.

        Args:
            blob_ref: The blob reference from Scan.blob
            max_depth: Only return rows with depth <= max_depth
            min_depth: Only return rows with depth >= min_depth
            follow_refs: If True, recursively load child chunks (hybrid backend only)

        Returns:
            DataFrame with scan data
        """
        pass

    @abstractmethod
    def get_path_stats(self, blob_ref: str, rel_path: str) -> PathStats | None:
        """Get stats for a specific path within a scan.

        Args:
            blob_ref: The blob reference from Scan.blob
            rel_path: Path relative to scan root (e.g., 'foo/bar' for /scan/root/foo/bar)

        Returns:
            PathStats if found, None otherwise
        """
        pass

    @abstractmethod
    def delete(self, blob_ref: str) -> None:
        """Delete the stored scan data.

        Args:
            blob_ref: The blob reference to delete
        """
        pass

    def delete_path(self, blob_ref: str, rel_path: str) -> PathStats | None:
        """Delete a path from scan data and update ancestor stats.

        Only supported by backends where supports_updates=True.
        For immutable backends, returns None (caller should rescan).

        Args:
            blob_ref: The blob reference from Scan.blob
            rel_path: Path relative to scan root to delete

        Returns:
            PathStats of deleted item if successful, None if not supported/found
        """
        if not self.supports_updates:
            return None
        return self._delete_path_impl(blob_ref, rel_path)

    def _delete_path_impl(self, blob_ref: str, rel_path: str) -> PathStats | None:
        """Implementation of delete_path for backends that support updates."""
        raise NotImplementedError("Backend claims to support updates but didn't implement _delete_path_impl")
