from click import argument, command, option
from humanize import naturalsize
import os
from os import stat
from os.path import abspath, exists, isfile, join
import pandas as pd
from pandas import to_datetime as to_dt
from pathlib import Path
import re
from re import fullmatch

from utz import basename, concat, DF, dirname, dt, env, exists, o, process, splitext, sxs, urlparse


LINE_RGX = '(?P<mtime>\d{4}-\d{2}-\d{2} \d\d:\d\d:\d\d) +(?P<size>\d+) (?P<key>.*)'

DISK_TREE_ROOT_VAR = 'DISK_TREE_ROOT'
HOME = env['HOME']
CONFIG_DIR = join(HOME, '.config')
if exists(CONFIG_DIR):
    DEFAULT_ROOT = join(CONFIG_DIR, 'disk-tree')
else:
    DEFAULT_ROOT = join(HOME, '.disk-tree')


def parse_s3_line(line):
    m = re.fullmatch(LINE_RGX, line)
    if not m:
        raise ValueError(f'Unrecognized line: {line}')

    mtime = to_dt(m['mtime'])
    size = int(m['size'])
    key = m['key']
    return o(mtime=mtime, size=size, key=key)


def path_metadata(path):
    stat = os.stat(path)
    mtime = to_dt(stat.st_mtime, unit='s')
    size = stat.st_size
    return o(mtime=mtime, size=size, path=path, abspath=abspath(path))


def dirs(file, root=None):
    rv = [file]
    cur = file
    while True:
        dir = dirname(cur)
        if dir == cur:
            break
        else:
            rv.append(dir)
            cur = dir
            if root and cur == root:
                break
    return list(reversed(rv))


def flatmap(self, func, **kwargs):
    dfs = []
    for idx, row in self.iterrows():
        df = func(row, **kwargs)
        dfs.append(df)
    return concat(dfs)

DF.flatmap = flatmap


def expand_file_row(r, root=None):
    path = r['path']
    file_df = r.to_frame().transpose()
    ancestors = dirs(path, root=root)[:-1]
    dirs_df = DF([ dict(mtime=r['mtime'], size=r['size'], path=dir, type='dir') for dir in ancestors ])
    rows = concat([file_df, dirs_df]).reset_index(drop=True)
    return rows


def agg_dirs(files, k='path', root=None,):
    files['type'] = 'file'
    expanded = files.flatmap(expand_file_row, root=root)
    groups = expanded.groupby([k, 'type',])
    if len(groups) == len(files):
        return files
    sizes = groups['size'].sum()
    mtimes = groups['mtime'].max()
    num_descendents = groups.size().rename('num_descendents')
    aggd = sxs(mtimes, sizes, num_descendents).reset_index()
    return aggd


def strip_prefix(key, prefix):
    if key.startswith(prefix):
        return key[len(prefix):]
    else:
        raise ValueError(f"Key {key} doesn't start with expected prefix {prefix}")


def main(path, profile=None):
    if profile:
        env['AWS_PROFILE'] = profile

    url = urlparse(path)
    if url.scheme == 's3' or profile:
        # S3
        m = fullmatch('s3://(?P<bucket>[^/]+)(?:/(?P<root>.*))?', path)
        if not m:
            raise ValueError(f'Unrecognized S3 URL: {path}')
        bucket = m['bucket']
        root = m['root'] or ''

        now = to_dt(dt.now())
        lines = process.lines('aws', 's3', 'ls', '--recursive', path)
        files = DF([parse_s3_line(line) for line in lines])
        files['path'] = files['key'].apply(strip_prefix, prefix=f'{root}/')
        aggd = agg_dirs(files, root=root).sort_values('path')
        aggd['bucket'] = bucket
        aggd['root'] = root
        aggd['key'] = aggd['root'] + '/' + aggd['path']
        aggd['checked_dt'] = now
        aggd = aggd.drop(columns=['path', 'root'])
        return root, bucket, aggd
    elif not url.scheme:
        # Local filesystem
        now = to_dt(dt.now())
        root = path
        paths = [ str(p) for p in Path(path).glob("**/*") ]
        files = DF([ path_metadata(p) for p in paths if isfile(p) ])
        aggd = agg_dirs(files, root=root).sort_values('path')
        aggd['root'] = root
        aggd['checked_dt'] = now
        aggd = aggd.drop(columns=['root'])
        return root, None, aggd
    else:
        raise ValueError(f'Unsupported URL scheme: {url.scheme}')


@command('disk-tree')
@option('--format', default='sql', help=f'Format to write output/metadata in: `sql` or `pqt`')
@option('-o', '--output-dir', help=f'Location to store disk-tree metadata. Default: ${DISK_TREE_ROOT_VAR}, $HOME/.config/disk-tree, or $HOME/.disk-tree')
@option('-O', '--no-output', is_flag=True, help=f'Disable writing results to {DEFAULT_ROOT}')
@option('-p', '--s3-profile', help='AWS profile to use')
@argument('path')
def cli(path, format, output_dir, no_output, s3_profile):
    root, bucket, aggd = main(path, profile=s3_profile)

    if not output_dir:
        output_dir = DEFAULT_ROOT

    if format == 'sql':
        output_path = join(output_dir, 'disk-tree.db')
    elif format == 'pqt':
        output_path = join(output_dir, 'disk-tree.pqt')
    else:
        raise ValueError(f'Unrecognized output metadata format: {format}')

    if not no_output:

        def merge(prev, cur):
            passthrough = prev[(prev.bucket != bucket)]
            if root:
                passthrough = concat([ passthrough, prev[(prev.bucket == bucket) & ~(prev.key.str.startswith(root))] ])
            merged = concat([ cur, passthrough ]).sort_values(['bucket', 'key'])
            num_passthrough = len(passthrough)
            num_overwritten = len(prev) - num_passthrough
            num_new = len(cur) - num_overwritten
            print(f"Overwriting {output_dir}: {len(prev)} previous records, {num_passthrough} passing through, {num_overwritten} overwritten {num_new} new")
            return merged

        base, ext = splitext(output_path)
        if format == 'sql':
            sqlite_url = f'sqlite:///{output_path}'
            name = basename(base)
            if exists(output_path):
                prev = pd.read_sql_table(name, sqlite_url)
                aggd = merge(prev, aggd)
            aggd.to_sql(name, sqlite_url, if_exists='replace')
        elif format == 'pqt':
            if exists(output_path):
                prev = pd.read_parquet(output_path)
                aggd = merge(prev, aggd)
            aggd.to_parquet(output_path)
        else:
            raise ValueError(f'Unrecognized output metadata format: {format}')

    files, dirs = aggd[aggd['type'] == 'file'], aggd[aggd['type'] == 'dir']
    total_size = files.size.sum()
    print(f'{len(files)} files in {len(dirs)} dirs, total size {naturalsize(total_size)}')

    print(aggd)


if __name__ == '__main__':
    cli()
