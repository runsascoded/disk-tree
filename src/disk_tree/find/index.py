from dateutil.parser import parse
import os
import re
from os.path import dirname
import subprocess
from subprocess import PIPE
from typing import Iterator
from urllib.parse import urlparse

import pandas as pd
from tqdm import tqdm
from utz import o, err

from disk_tree import time


WS = re.compile(r'\s+')


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
            if not key.startswith(f'{key0}/') and key != key0:
                raise ValueError(f"{path}: unexpected {key=}")
            key = key[len(key0)+1:] if key != key0 else ''
            cur = key
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
                    parent=None if d == key0 else dirname(d),
                    uri=f's3://{bkt}/{d}',
                )
            yield o(
                path=key,
                size=size,
                mtime=mtime,
                kind='file',
                parent=None if key == key0 else dirname(key),
                uri=f's3://{bkt}/{key}',
            )


def files_iter(
    path: str,
    sudo: bool = False,
) -> Iterator[dict]:
    if path.startswith('s3://'):
        yield from s3_files_iter(path)
        return
    abspath = os.path.abspath(path)
    cmd = ['gfind', abspath, '-printf', r'%y %s %T@ %p\n']
    if sudo:
        cmd = [ 'sudo', *cmd ]
    proc = subprocess.Popen(cmd, stdout=PIPE, stderr=PIPE, text=True)
    with time("files_iter lines"):
        for line in tqdm(proc.stdout):
            strs = line.rstrip('\n').split(' ', 3)
            kind = 'file' if strs[0] == 'f' else 'dir' if strs[0] == 'd' else strs[0]
            size = int(strs[1])
            mtime = int(float(strs[2]))
            path = strs[3]
            yield o(
                path=path,
                size=size,
                mtime=mtime,
                kind=kind,
                parent=None if path == abspath else dirname(path),
            )

    # Check for any errors from gfind after the loop
    stderr_output = proc.stderr.read()
    if stderr_output:
        err(stderr_output)

    # Wait for the proc to finish and check the return code
    code = proc.wait()
    if code != 0:
        err(f"gfind process exited with return code {code}")


def index(
    path: str,
    sudo: bool = False,
) -> pd.DataFrame:
    path0 = path.rstrip('/')
    with time("files_iter"):
        paths = [
            dict(**e, n_desc=1, n_children=0)
            for e in files_iter(path0, sudo=sudo)
        ]

    df = pd.DataFrame(paths)
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
        dirs['uri'] = dirs.path.apply(lambda p: f'{path0}/{p}' if p else path0)
        return (
            pd.concat(
                [dirs, files],
                ignore_index=True,
            )
            .sort_values('path')
            .reset_index(drop=True)
        )
