import re
from os.path import join

from utz import concat, DF, dirname, o, sxs, to_dt

LINE_RGX = '(?P<mtime>\d{4}-\d{2}-\d{2} \d\d:\d\d:\d\d) +(?P<size>\d+) (?P<key>.*)'


def parse_line(line):
    m = re.fullmatch(LINE_RGX, line)
    if not m:
        raise ValueError(f'Unrecognized line: {line}')

    mtime = to_dt(m['mtime'])
    size = int(m['size'])
    key = m['key']
    return o(mtime=mtime, size=size, key=key)


def dirs(file):
    #rv = [file]
    rv = []
    cur = file
    while True:
        dir = dirname(cur)
        if dir == cur:
            break
        else:
            rv.append(dir)
            cur = dir
    return list(reversed(rv))


def flatmap(self, func):
    dfs = []
    for idx, row in self.iterrows():
        df = func(row)
        dfs.append(df)
    return concat(dfs)


DF.flatmap = flatmap


def expand_file_row(r):
    file_df = r.to_frame().transpose()
    if r.key.endswith('/'):
        return DF()
    ancestors = dirs(r.relpath)
    dirs_df = DF([
        {
            **r.to_dict(),
            # mtime=r.mtime,
            # size=r['size'],
            'key': join(r.root_key, dir) if dir else r.root_key,
            'kind': 'dir',
        }
        for dir in ancestors
    ])
    rows = concat([file_df, dirs_df]).reset_index(drop=True)
    return rows


def agg_dirs(files, k='key'):
    files['kind'] = 'file'
    expanded = files.flatmap(expand_file_row)
    groups = expanded.groupby([k, 'kind'])
    if len(groups) == len(files):
        return files
    sizes = groups['size'].sum()
    mtimes = groups['mtime'].max()
    num_descendants = groups.size().rename('num_descendants')
    aggd = sxs(mtimes, sizes, num_descendants).reset_index()
    return aggd


def strip_prefix(key, prefix):
    if key.startswith(prefix):
        return key[len(prefix):]
    else:
        raise ValueError(f"Key {key} doesn't start with expected prefix {prefix}")
