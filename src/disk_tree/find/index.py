from array import array
from dataclasses import dataclass, field
from os.path import dirname
import time as time_module
from typing import Callable

import pandas as pd

from disk_tree import time
from ..backends import backend_for, ErrorCollector


# Type for progress callback: (items_found, items_per_sec, error_count) -> None
ProgressCallback = Callable[[int, float | None, int], None]


@dataclass
class IndexResult:
    df: pd.DataFrame
    error_count: int = 0
    error_paths: list[str] = field(default_factory=list)


def index(
    path: str,
    sudo: bool = False,
    progress_callback: ProgressCallback | None = None,
    progress_interval: float = 1.0,
    excludes: list[str] | None = None,
) -> IndexResult:
    path0 = path.rstrip('/') or '/'
    errors = ErrorCollector()
    backend = backend_for(path0)

    last_progress_time = time_module.time()
    items_count = 0
    start_time = last_progress_time

    # Accumulate columns in parallel lists (much cheaper than 7M dict objects).
    path_l: list[str] = []
    size_l = array('q')  # signed int64
    mtime_l = array('q')
    kind_l: list[str] = []
    parent_l: list[str | None] = []
    uri_l: list[str] = []

    def collect():
        nonlocal last_progress_time, items_count
        kwargs = dict(errors=errors, excludes=excludes, sudo=sudo)
        for e in backend.list(path0, **kwargs):
            items_count += 1
            now = time_module.time()
            if progress_callback and (now - last_progress_time) >= progress_interval:
                elapsed = now - start_time
                items_per_sec = items_count / elapsed if elapsed > 0 else None
                progress_callback(items_count, items_per_sec, errors.count)
                last_progress_time = now
            path_l.append(e['path'])
            size_l.append(e['size'])
            mtime_l.append(e['mtime'])
            kind_l.append(e['kind'])
            parent_l.append(e['parent'])
            uri_l.append(e['uri'])

    with time("files_iter"):
        collect()

    if progress_callback:
        elapsed = time_module.time() - start_time
        items_per_sec = items_count / elapsed if elapsed > 0 else None
        progress_callback(items_count, items_per_sec, errors.count)

    # Handle empty bucket/directory: return early with just a root row
    if not path_l:
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

    df = pd.DataFrame({
        'path': path_l,
        'size': size_l,
        'mtime': mtime_l,
        'kind': kind_l,
        'parent': parent_l,
        'uri': uri_l,
    })
    # Free the per-column lists now that the DataFrame owns the data — peak memory
    # otherwise has both representations resident through the aggregation passes.
    del path_l, size_l, mtime_l, kind_l, parent_l, uri_l
    df['n_desc'] = 1
    df['n_children'] = 0

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
            dirs.loc[dirs.path == '', ['path', 'parent']] = ['.', '']
            dirs['uri'] = dirs.path.apply(lambda p: path0 if p == '.' else f'{path0}/{p}')
        df = pd.concat([dirs, files], ignore_index=True)
        # Add depth column for efficient parquet filtering
        # '.' = 0, 'foo' = 1, 'foo/bar' = 2, etc.
        df['depth'] = df['path'].apply(lambda p: 0 if p == '.' else p.count('/') + 1)
        # Sort by depth first (breadth-first order) for efficient parquet row group filtering
        df = df.sort_values(['depth', 'path']).reset_index(drop=True)
        return IndexResult(
            df=df,
            error_count=errors.count,
            error_paths=errors.paths,
        )
