#!/usr/bin/env python

from functools import partial
from os import remove
from os.path import basename, splitext
from sys import stderr

import pandas as pd
from click import command, argument, option
from disk_tree import find
from utz import iec


def rm_root(path, root):
    if path == root:
        return '.'
    elif path.startswith(f'{root}/'):
        return path[len(root)+1:]
    else:
        raise ValueError(f"{path} doesn't start with {root}")


err = partial(print, file=stderr)


@command(no_args_is_help=True)
@option('-r', '--remove', 'do_remove', is_flag=True)
@argument('root')
def main(
    do_remove: bool,
    root: str,
):
    d = find.index(root)
    d['path'] = d['path'].apply(rm_root, root=root)
    d['basename'] = d.path.apply(basename)
    d['name'] = d.basename.apply(lambda n: splitext(n)[0])

    mp4s = d[d.path.str.endswith('.mp4')]
    insv = d[d.path.str.endswith('.insv')]
    fs = pd.concat([ mp4s, insv ])
    fs = pd.concat([
        fs,
        fs.basename.str.extract(r'VID_(?P<dt>\d{8}_\d{6})_(?P<id>\d\d)_(?P<idx>\d\d\d)\.(?P<xtn>.*)'),
    ], axis=1)

    def extra_mp4s(df):
        mp4s = df[df.basename.str.endswith('.mp4')].path.tolist()
        if not mp4s:
            return pd.DataFrame()
        ins = df[df.basename.str.endswith('.insv')].path.tolist()
        if len(ins) == 2:
            return pd.DataFrame([
                dict(mp4=mp4, ins0=ins[0], ins1=ins[1])
                for mp4 in mp4s
            ])
        else:
            for mp4 in mp4s:
                err(f"{mp4}: {len(ins)} `.insv`s")
            return pd.DataFrame()

    ms = fs.groupby(['dt', 'idx']).apply(extra_mp4s, include_groups=False).reset_index(drop=True)

    total_size = (
        ms
        .set_index('mp4')
        .merge(
            d.set_index('path')[['size']],
            how='left',
            left_index=True,
            right_index=True,
        )['size']
        .sum()
    )
    err(f'{len(ms)} MP4s, {iec(total_size)}:')

    for m in ms['mp4']:
        path = f'{root}/{m}'
        print(m)
        if do_remove:
            remove(path)


if __name__ == '__main__':
    main()
