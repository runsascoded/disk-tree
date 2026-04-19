import os
import re
import shutil
import subprocess
import time as time_module
from os.path import abspath, dirname, isdir, isfile
from subprocess import PIPE
from typing import Iterator

from tqdm import tqdm
from utz import err, o

from disk_tree import time

from .base import Backend, ErrorCollector, ProgressCallback


# gfind uses Unicode curly quotes (' ') in error messages on some systems
PERMISSION_DENIED_RE = re.compile(r"^gfind: ['\u2018]([^'\u2019]+)['\u2019]: Permission denied$")

# macOS CloudStorage paths (File Provider virtual filesystem)
# These directories proxy to cloud services and block on network I/O
CLOUDSTORAGE_PATHS = [
    '/Library/CloudStorage',        # System-wide (rare)
    os.path.expanduser('~/Library/CloudStorage'),  # Per-user
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

        # Add exclusion patterns for virtual filesystems
        if excludes is None:
            excludes = CLOUDSTORAGE_PATHS
        for pattern in excludes:
            abs_pattern = abspath(os.path.expanduser(pattern))
            if (abs_pattern.startswith(path0 + '/')
                    or path0.startswith(abs_pattern.rstrip('/') + '/')
                    or abs_pattern == path0):
                cmd.extend(['-path', abs_pattern, '-prune', '-o'])

        # Null-terminated output to handle filenames with newlines
        # %b = 512-byte blocks actually allocated (handles sparse files correctly)
        cmd.extend(['-printf', r'%y %b %T@ %p\0'])
        if sudo:
            cmd = ['sudo', *cmd]
        proc = subprocess.Popen(cmd, stdout=PIPE, stderr=PIPE)

        def read_records():
            """Read null-terminated records from gfind output."""
            buffer = b''
            for chunk in iter(lambda: proc.stdout.read(65536), b''):
                buffer += chunk
                while b'\0' in buffer:
                    record, buffer = buffer.split(b'\0', 1)
                    yield record

        # Progress tracking (optional)
        last_progress_time = time_module.time()
        start_time = last_progress_time
        items_count = 0

        with time("files_iter lines"):
            for record in tqdm(read_records()):
                try:
                    line = record.decode('utf-8', errors='replace')
                except UnicodeDecodeError:
                    continue
                strs = line.split(' ', 3)
                if len(strs) < 4:
                    continue
                kind = 'file' if strs[0] == 'f' else 'dir' if strs[0] == 'd' else strs[0]
                try:
                    size = int(strs[1]) * 512
                    mtime = int(float(strs[2]))
                except ValueError:
                    continue
                filepath = strs[3]
                prefix = '/' if path0 == '/' else f'{path0}/'
                if not filepath.startswith(prefix) and filepath != path0:
                    continue
                uri = filepath
                if filepath == path0:
                    filepath = ''
                elif path0 == '/':
                    filepath = filepath[1:]
                else:
                    filepath = filepath[len(path0) + 1:]
                yield o(
                    path=filepath,
                    size=size,
                    mtime=mtime,
                    kind=kind,
                    parent=None if filepath == path0 else dirname(filepath),
                    uri=uri,
                )

                items_count += 1
                now = time_module.time()
                if progress_callback and (now - last_progress_time) >= progress_interval:
                    elapsed = now - start_time
                    items_per_sec = items_count / elapsed if elapsed > 0 else None
                    err_count = errors.count if errors else 0
                    progress_callback(items_count, items_per_sec, err_count)
                    last_progress_time = now

        # Check for any errors from gfind after the loop
        stderr_bytes = proc.stderr.read()
        if stderr_bytes:
            stderr_output = stderr_bytes.decode('utf-8', errors='replace')
            for line in stderr_output.strip().split('\n'):
                if not line:
                    continue
                match = PERMISSION_DENIED_RE.match(line)
                if match and errors is not None:
                    errors.add(match.group(1))
                else:
                    err(line)

        code = proc.wait()
        if code != 0 and (errors is None or errors.count == 0):
            err(f"gfind process exited with return code {code}")

    def delete(self, url: str) -> None:
        if isfile(url):
            os.remove(url)
        elif isdir(url):
            shutil.rmtree(url)

    def exists(self, url: str) -> bool:
        return isfile(url) or isdir(url)
