import os
import shutil
from os.path import abspath, isdir, isfile
from typing import Iterator

from .base import Backend, ErrorCollector, ProgressCallback
from .gfind import run_gfind


# macOS CloudStorage paths (File Provider virtual filesystem)
# These directories proxy to cloud services and block on network I/O
CLOUDSTORAGE_PATHS = [
    '/Library/CloudStorage',
    os.path.expanduser('~/Library/CloudStorage'),
]


class LocalBackend(Backend):
    """Local filesystem, scanned via `gfind`."""

    scheme = 'file'

    @property
    def is_local(self) -> bool:
        return True

    @property
    def supports_sudo(self) -> bool:
        return True

    def list(
        self,
        url: str,
        *,
        errors: ErrorCollector | None = None,
        excludes: list[str] | None = None,
        sudo: bool = False,
        progress_callback: ProgressCallback | None = None,
        progress_interval: float = 1.0,
    ) -> Iterator[dict]:
        path0 = abspath(url)
        cmd = ['gfind', path0]

        if excludes is None:
            excludes = CLOUDSTORAGE_PATHS
        for pattern in excludes:
            abs_pattern = abspath(os.path.expanduser(pattern))
            if (abs_pattern.startswith(path0 + '/')
                    or path0.startswith(abs_pattern.rstrip('/') + '/')
                    or abs_pattern == path0):
                cmd.extend(['-path', abs_pattern, '-prune', '-o'])

        # %b = 512-byte blocks actually allocated (handles sparse files correctly)
        cmd.extend(['-printf', r'%y %b %T@ %p\0'])
        if sudo:
            cmd = ['sudo', *cmd]

        yield from run_gfind(
            cmd,
            path0,
            uri_for=lambda p: p,
            errors=errors,
            progress_callback=progress_callback,
            progress_interval=progress_interval,
        )

    def delete(self, url: str) -> None:
        if isfile(url):
            os.remove(url)
        elif isdir(url):
            shutil.rmtree(url)

    def exists(self, url: str) -> bool:
        return isfile(url) or isdir(url)
