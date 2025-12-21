from dataclasses import dataclass, field
from dateutil.parser import parse
import os
import re
from os.path import dirname
import subprocess
from subprocess import PIPE
import time as time_module
from typing import Callable, Iterator
from urllib.parse import urlparse

import pandas as pd
from tqdm import tqdm
from utz import o, err

from disk_tree import time


# Type for progress callback: (items_found, items_per_sec, error_count) -> None
ProgressCallback = Callable[[int, float | None, int], None]


WS = re.compile(r'\s+')
# gfind uses Unicode curly quotes (' ') in error messages on some systems
PERMISSION_DENIED_RE = re.compile(r"^gfind: ['\u2018]([^'\u2019]+)['\u2019]: Permission denied$")


@dataclass
class IndexResult:
    df: pd.DataFrame
    error_count: int = 0
    error_paths: list[str] = field(default_factory=list)


def s3_files_iter(path: str) -> Iterator[dict]:
    cmd = [ 'aws', 's3', 'ls', '--recursive', path ]
    proc = subprocess.Popen(cmd, stdout=PIPE, stderr=PIPE, text=True)
    root = path
    url = urlparse(root)
    bkt = url.netloc
    key0 = url.path.lstrip('/')
    dirs = set()
    with time("s3_files_iter lines"):
        for line in tqdm(proc.stdout):
            strs = WS.split(line.rstrip('\n'), 3)
            mtime_str = f'{strs[0]} {strs[1]}'
            mtime = int(parse(mtime_str).timestamp())
            size = int(strs[2])
            key = strs[3]
            # When key0 is empty (bucket root), all keys are valid
            # Otherwise, keys must start with the prefix
            if key0:
                if not key.startswith(f'{key0}/') and key != key0:
                    raise ValueError(f"{path}: unexpected {key=}")
                relpath = key[len(key0)+1:] if key != key0 else ''
            else:
                relpath = key
            cur = relpath
            if not cur and not relpath:
                continue
            new_dirs = []
            while True:
                try:
                    idx = cur.rindex('/')
                except ValueError:
                    idx = 0
                cur = cur[:idx]
                seen = cur in dirs
                if not seen:
                    dirs.add(cur)
                    new_dirs.append(cur)
                if not cur or seen:
                    break

            for d in reversed(new_dirs):
                yield o(
                    path=d,
                    size=0,
                    mtime=0,
                    kind='dir',
                    parent=dirname(d) if d else None,
                    uri=f's3://{bkt}/{key0}/{d}' if key0 else f's3://{bkt}/{d}',
                )
            yield o(
                path=relpath,
                size=size,
                mtime=mtime,
                kind='file',
                parent=dirname(relpath),
                uri=f's3://{bkt}/{key}',
            )


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


# macOS CloudStorage paths (File Provider virtual filesystem)
# These directories proxy to cloud services and block on network I/O
CLOUDSTORAGE_PATHS = [
    '/Library/CloudStorage',        # System-wide (rare)
    os.path.expanduser('~/Library/CloudStorage'),  # Per-user
]


def files_iter(
    path: str,
    sudo: bool = False,
    errors: ErrorCollector | None = None,
    excludes: list[str] | None = None,
) -> Iterator[dict]:
    if path.startswith('s3://'):
        yield from s3_files_iter(path)
        return
    path0 = os.path.abspath(path)
    cmd = ['gfind', path0]

    # Add exclusion patterns for virtual filesystems
    if excludes is None:
        excludes = CLOUDSTORAGE_PATHS
    for pattern in excludes:
        # Only add exclusion if the pattern could be under the scan path
        # (or the scan path is under the pattern)
        abs_pattern = os.path.abspath(os.path.expanduser(pattern))
        if abs_pattern.startswith(path0 + '/') or path0.startswith(abs_pattern.rstrip('/') + '/') or abs_pattern == path0:
            cmd.extend(['-path', abs_pattern, '-prune', '-o'])

    # Use null-terminated output to handle filenames with newlines
    # %b = 512-byte blocks actually allocated (handles sparse files correctly)
    # %s would give logical size which is wrong for sparse files like Docker VMs
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
                # %b gives 512-byte blocks, convert to bytes
                size = int(strs[1]) * 512
                mtime = int(float(strs[2]))
            except ValueError:
                continue
            filepath = strs[3]
            if not filepath.startswith(f'{path0}/') and filepath != path0:
                continue
            uri = filepath
            filepath = filepath[len(path0)+1:] if filepath != path0 else ''
            yield o(
                path=filepath,
                size=size,
                mtime=mtime,
                kind=kind,
                parent=None if filepath == path0 else dirname(filepath),
                uri=uri,
            )

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
                # Print non-permission errors to stderr
                err(line)

    # Wait for the proc to finish and check the return code
    code = proc.wait()
    if code != 0 and (errors is None or errors.count == 0):
        # Only warn about non-zero exit if we didn't capture permission errors
        err(f"gfind process exited with return code {code}")


def index(
    path: str,
    sudo: bool = False,
    progress_callback: ProgressCallback | None = None,
    progress_interval: float = 1.0,  # How often to call progress_callback (seconds)
    excludes: list[str] | None = None,  # Paths to exclude (default: CloudStorage)
) -> IndexResult:
    path0 = path.rstrip('/')
    errors = ErrorCollector()

    # Track progress timing
    last_progress_time = time_module.time()
    items_count = 0
    start_time = last_progress_time

    def collect_with_progress():
        nonlocal last_progress_time, items_count
        for e in files_iter(path0, sudo=sudo, errors=errors, excludes=excludes):
            items_count += 1
            now = time_module.time()
            if progress_callback and (now - last_progress_time) >= progress_interval:
                elapsed = now - start_time
                items_per_sec = items_count / elapsed if elapsed > 0 else None
                progress_callback(items_count, items_per_sec, errors.count)
                last_progress_time = now
            yield dict(**e, n_desc=1, n_children=0)

    with time("files_iter"):
        paths = list(collect_with_progress())

    # Final progress update
    if progress_callback:
        elapsed = time_module.time() - start_time
        items_per_sec = items_count / elapsed if elapsed > 0 else None
        progress_callback(items_count, items_per_sec, errors.count)

    df = pd.DataFrame(paths)

    # Handle empty bucket/directory: return early with just a root row
    if df.empty:
        df = pd.DataFrame([{
            'path': '.',
            'size': 0,
            'mtime': 0,
            'n_desc': 0,
            'n_children': 0,
            'kind': 'dir',
            'parent': '',
            'uri': path0,
            'depth': 0,
        }])
        return IndexResult(
            df=df,
            error_count=errors.count,
            error_paths=errors.paths,
        )

    files = df[df.kind == 'file']
    dirs0 = df[df.kind == 'dir']
    cur = df
    dir_dfs = [dirs0]
    level = 0
    while True:
        with time(f"index-agg-{level}"):
            cur = cur[cur.path != ''].copy()
            if cur.empty:
                break
            cur['path'] = cur['parent']
            grouped = cur.groupby('path')
            sizes = grouped['size'].sum()
            n_children = grouped.size() if level == 0 else 0
            mtimes = grouped['mtime'].max()
            n_desc = grouped['n_desc'].sum()
            dirs = pd.DataFrame({
                'path': sizes.index,
                'size': sizes,
                'mtime': mtimes,
                'n_desc': n_desc,
                'n_children': n_children,
            }).reset_index(drop=True)
            dirs['parent'] = dirs.path.apply(dirname)
            dir_dfs.append(dirs)
            cur = dirs
            level += 1

    with time("index-agg-dirs"):
        dirs = pd.concat(dir_dfs)
        if dirs.empty:
            # Empty directory/bucket: create a root row with zeros
            dirs = pd.DataFrame([{
                'path': '.',
                'size': 0,
                'mtime': 0,
                'n_desc': 0,
                'n_children': 0,
                'kind': 'dir',
                'parent': '',
                'uri': path0,
            }])
        else:
            grouped = dirs.groupby('path')
            sizes = grouped['size'].sum()
            dirs = pd.DataFrame({
                'path': sizes.index,
                'size': sizes,
                'mtime': grouped['mtime'].max(),
                'n_desc': grouped['n_desc'].sum(),
                'n_children': grouped['n_children'].sum(),
                'kind': 'dir',
            }).reset_index(drop=True)
            dirs['parent'] = dirs.path.apply(dirname)
            dirs.loc[dirs.parent == '', 'parent'] = '.'
            dirs.loc[dirs.path == '', ['path', 'parent']] = [ '.', '' ]
            dirs['uri'] = dirs.path.apply(lambda p: path0 if p == '.' else f'{path0}/{p}')
        df = (
            pd.concat(
                [dirs, files],
                ignore_index=True,
            )
            .sort_values('path')
            .reset_index(drop=True)
        )
        # Add depth column for efficient parquet filtering
        # '.' = 0, 'foo' = 1, 'foo/bar' = 2, etc.
        df['depth'] = df['path'].apply(lambda p: 0 if p == '.' else p.count('/') + 1)
        return IndexResult(
            df=df,
            error_count=errors.count,
            error_paths=errors.paths,
        )
