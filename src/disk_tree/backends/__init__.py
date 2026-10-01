from .base import Backend, ErrorCollector, ProgressCallback
from .s3 import S3Backend
from .url import ParsedUrl, canonical, parse_url, url_parent


class UnsupportedBackend(Backend):
    """A source disk-tree cannot list live: `gcs://` (whose scans are imported
    via `bulk-list` + `import`; it used to fall through to a filesystem walk and
    "succeed" with an empty scan), and local paths — this engine scans object
    stores only."""

    def __init__(self, scheme: str):
        self._scheme = scheme

    @property
    def scheme(self) -> str:
        return self._scheme

    def refusal(self, verb: str = 'live scanning') -> str:
        if self._scheme == 'file':
            return (
                f"{verb} of local paths isn't supported; index an `s3://` or `r2://` URL, "
                "or import a listing (`disk-tree bulk-list` + `disk-tree import -l <listing>`)"
            )
        return (
            f"{verb} of {self._scheme}:// isn't implemented; import a listing instead "
            "(`disk-tree bulk-list` + `disk-tree import -l <listing>`)"
        )

    def _refuse(self, verb: str) -> NotImplementedError:
        return NotImplementedError(self.refusal(verb))

    def list(self, url: str, **kwargs):
        raise self._refuse('live scanning')

    def exists(self, url: str) -> bool:
        raise self._refuse('existence check')


def backend_for(url: str) -> Backend:
    """Return a Backend instance for the given URL's scheme.

    `r2://` is S3-compatible: it lists through the bucket's Cloudflare endpoint
    (`DISK_TREE_R2_ENDPOINT_URL`, else the bucket's `endpoint_url` in
    buckets.yml — resolved here, missing reported at list time). Local paths,
    `gcs://` and any other scheme have no live lister (see `UnsupportedBackend`).
    """
    parsed = parse_url(url)
    if parsed.scheme == 's3':
        from urllib.parse import urlparse
        from disk_tree.blobfs import bucket_profile
        return S3Backend(profile=bucket_profile(urlparse(url).netloc))
    if parsed.scheme == 'r2':
        from urllib.parse import urlparse
        from disk_tree.blobfs import bucket_profile, r2_endpoint
        netloc = urlparse(url).netloc
        return S3Backend(endpoint_url=r2_endpoint(netloc), profile=bucket_profile(netloc), scheme='r2')
    return UnsupportedBackend(parsed.scheme)


__all__ = [
    'Backend',
    'ErrorCollector',
    'ParsedUrl',
    'ProgressCallback',
    'S3Backend',
    'UnsupportedBackend',
    'backend_for',
    'canonical',
    'parse_url',
    'url_parent',
]
