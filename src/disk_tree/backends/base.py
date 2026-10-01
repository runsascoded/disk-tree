from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Callable, Iterator


# Progress callback: (items_found, items_per_sec, error_count) -> None
ProgressCallback = Callable[[int, float | None, int], None]


@dataclass
class ErrorCollector:
    """Mutable container to collect errors during iteration."""
    count: int = 0
    paths: list[str] = field(default_factory=list)
    max_paths: int = 100  # Only store first N paths to avoid memory issues

    def add(self, path: str):
        self.count += 1
        if len(self.paths) < self.max_paths:
            self.paths.append(path)


class Backend(ABC):
    """Scan-source backend: where disk-tree reads from."""

    @property
    @abstractmethod
    def scheme(self) -> str:
        ...

    @abstractmethod
    def list(
        self,
        url: str,
        *,
        errors: ErrorCollector | None = None,
        progress: bool = True,
    ) -> Iterator[dict]:
        """Recursive bulk listing.

        Yields dicts with keys: path, size, mtime, kind, parent, uri.
        `path` is relative to scan root (empty string for the root entry);
        `uri` is fully qualified for this backend's scheme.
        """
        ...

    def exists(self, url: str) -> bool:
        """Cheap existence check (used for navigation fallbacks)."""
        raise NotImplementedError(f"{type(self).__name__}.exists not implemented")
