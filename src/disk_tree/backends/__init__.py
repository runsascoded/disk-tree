from .base import Backend, ErrorCollector, ProgressCallback
from .local import LocalBackend
from .s3 import S3Backend
from .url import ParsedUrl, canonical, parse_url, url_parent


def backend_for(url: str) -> Backend:
    """Return a Backend instance for the given URL's scheme."""
    parsed = parse_url(url)
    if parsed.scheme == 's3':
        return S3Backend()
    if parsed.scheme == 'ssh':
        from .ssh import SshBackend
        return SshBackend()
    # TODO: r2, gcs, az via configured endpoints
    return LocalBackend()


__all__ = [
    'Backend',
    'ErrorCollector',
    'LocalBackend',
    'ParsedUrl',
    'ProgressCallback',
    'S3Backend',
    'backend_for',
    'canonical',
    'parse_url',
    'url_parent',
]
