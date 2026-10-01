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

#: GNU find: `gfind` where it's Homebrew's findutils (macOS), else the system
#: `find` (Linux, where it's already GNU and has `-printf`).
FIND = shutil.which('gfind') and 'gfind' or 'find'


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
        progress: bool = True,
    ) -> Iterator[dict]:
        path0 = abspath(url)

        if excludes is None:
            excludes = CLOUDSTORAGE_PATHS
        # `rstrip` so a `/` root's prefix is `/`, not `//` (which matched nothing,
        # so a `/` scan walked into CloudStorage).
        prefix = path0.rstrip('/') + '/'
        applicable_excludes = [
            abs_pattern
            for pattern in excludes
            for abs_pattern in [abspath(os.path.expanduser(pattern))]
            if (abs_pattern.startswith(prefix)
                or path0.startswith(abs_pattern.rstrip('/') + '/')
                or abs_pattern == path0)
        ]

        # %b = 512-byte blocks actually allocated (handles sparse files correctly)
        fmt = r'%y %b %T@ %p\0'
        cmd = [FIND, path0]
        for abs_pattern in applicable_excludes:
            # Print the pruned dir itself (an empty leaf): the tree shows an
            # excluded dir is there.
            cmd.extend(['-path', abs_pattern, '-printf', fmt, '-prune', '-o'])
        cmd.extend(['-printf', fmt])
        if sudo:
            cmd = ['sudo', *cmd]

        yield from run_gfind(
            cmd,
            path0,
            uri_for=lambda p: p,
            errors=errors,
            progress_callback=progress_callback,
            progress_interval=progress_interval,
            progress=progress,
        )

    def exists(self, url: str) -> bool:
        return isfile(url) or isdir(url)
