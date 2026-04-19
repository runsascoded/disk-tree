from .base import Backend, ErrorCollector, ProgressCallback
from .local import LocalBackend
from .s3 import S3Backend


def backend_for(url: str) -> Backend:
    """Return a Backend instance for the given URL's scheme."""
    if url.startswith('s3://'):
        return S3Backend()
    return LocalBackend()


__all__ = [
    'Backend',
    'ErrorCollector',
    'LocalBackend',
    'ProgressCallback',
    'S3Backend',
    'backend_for',
]
