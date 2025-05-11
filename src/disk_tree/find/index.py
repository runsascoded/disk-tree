import os
from os.path import dirname
from typing import Iterator

import pandas as pd
from tqdm import tqdm
from utz import proc


def index(path: str) -> pd.DataFrame:
    path0 = path.rstrip('/')
    files = [
        dict(path=path, size=size, mtime=mtime, parent=dirname(path), n_desc=1, n_children=0, type='file')
        for path, size, mtime in files_iter(path0)
    ]
    df = pd.DataFrame(files)
    # n_dir_descs = {}
    # for f in files:
    #     parent = f['parent']
    #     size = f['size']
    #     mtime = f['mtime']
    #     while True:
    #
    #         if parent == path:
    #             break
    #         parent = dirname(parent)
    dir_dfs = []
    dirs = df.copy()
    dirs['path'] = dirs.parent
    dirs['parent'] = dirs['path'].apply(dirname)
    dirs['n_dir_descs'] = 0
    level = 0
    while True:
        grouped = dirs.groupby('path')
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
            'n_dir_descs': grouped['n_dir_descs'].sum() + (1 if level == 0 else 0),
            'type': 'dir',
        }).reset_index(drop=True)
        dir_dfs.append(dirs)
        dirs = dirs[dirs.path != path0]
        if dirs.empty:
            break
        dirs['path'] = dirs['path'].apply(dirname)
        level += 1

    dirs = pd.concat(dir_dfs)
    grouped = dirs.groupby('path')
    sizes = grouped['size'].sum()
    mtimes = grouped['mtime'].max()
    n_children = grouped['n_children'].sum()
    dirs = pd.DataFrame({
        'path': sizes.index,
        'size': sizes,
        'mtime': mtimes,
        'n_desc': grouped['n_desc'].sum() + grouped['n_dir_descs'].sum(),
        'n_children': n_children,
        'type': 'dir',
    }).reset_index(drop=True)
    dirs['parent'] = dirs.path.apply(dirname)
    return pd.concat([dirs, df], ignore_index=True).sort_values('path')


def files_iter(path: str) -> Iterator[tuple[str, int, int]]:
    abspath = os.path.abspath(path)
    lines = proc.lines('gfind', abspath, '-type', 'f', '-printf', r'%s %T@ %p\n')
    for line in tqdm(lines):
        strs = line.split(' ', 2)
        size = int(strs[0])
        mtime = int(float(strs[1]))
        path = strs[2]
        yield path, size, mtime
