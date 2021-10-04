from click import argument, command, option
from humanize import naturalsize
import os
from os import makedirs, stat, walk
from os.path import abspath, exists, isfile, islink, join
import pandas as pd
from pandas import to_datetime as to_dt
from pathlib import Path
import plotly.express as px
import re
from re import fullmatch
from subprocess import check_call

from utz import basename, concat, DF, dirname, dt, env, exists, o, process, singleton, splitext, sxs, urlparse


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
    parent = dirname(key)
    if key.endswith('/'):
        parent = dirname(parent)
    return o(mtime=mtime, size=size, key=key, parent=parent)


def path_metadata(path):
    stat = os.stat(path)
    mtime = to_dt(stat.st_mtime, unit='s')
    size = stat.st_size
    path = abspath(path)
    parent = dirname(path)
    return o(mtime=mtime, size=size, path=path, parent=parent)


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


def expand_file_row(r, k, root=None):
    path = r[k]
    file_df = r.to_frame().transpose()
    ancestors = dirs(path, root=root)[:-1]
    def to_dict(dir):
        rv = dict(mtime=r['mtime'], size=r['size'], type='dir', parent=dirname(dir))
        rv[k] = dir
        return rv
    dirs_df = DF([ to_dict(dir) for dir in ancestors ])
    rows = concat([file_df, dirs_df]).reset_index(drop=True)
    return rows


def agg_dirs(files, k='path', root=None,):
    files['type'] = 'file'
    expanded = files.flatmap(expand_file_row, k=k, root=root)
    groups = expanded.groupby([k, 'type',])
    if len(groups) == len(files):
        return files
    sizes = groups['size'].sum()
    mtimes = groups['mtime'].max()
    parents = groups.apply(lambda df: singleton(df['parent'])).rename('parent')
    num_descendents = groups.size().rename('num_descendents')
    aggd = sxs(mtimes, sizes, num_descendents, parents).reset_index()
    return aggd


def strip_prefix(key, prefix):
    if key.startswith(prefix):
        return key[len(prefix):]
    else:
        raise ValueError(f"Key {key} doesn't start with expected prefix {prefix}")


def load(path, profile=None, cache=cache, cache_ttl=None):
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
        files = DF([ parse_s3_line(line) for line in lines ])
        aggd = agg_dirs(files, k='key', root=root).sort_values('key')
        aggd['bucket'] = bucket
        # aggd['root'] = root
        aggd['checked_dt'] = now
        return root, bucket, aggd
    elif not url.scheme:
        # Local filesystem
        now = to_dt(dt.now())
        root = abspath(path)
        _, dirs, files = next(walk(root))
        dirs = [ join(root, dir) for dir in dirs ]
        files = [ join(root, file) for file in files ]
        files = DF([ path_metadata(p) for p in paths if not islink(p) ])

        # paths = [ str(p) for p in Path(path).glob("**/*") ]
        # files = DF([ path_metadata(p) for p in paths if isfile(p) and not islink(p) ])
        aggd = agg_dirs(files, root=root).sort_values('path')
        aggd['checked_dt'] = now
        return root, None, aggd
    else:
        raise ValueError(f'Unsupported URL scheme: {url.scheme}')


@command('disk-tree')
@option('--cache-dir', help=f'Location to store disk-tree metadata. Default: ${DISK_TREE_ROOT_VAR}, $HOME/.config/disk-tree, or $HOME/.disk-tree')
@option('--cache-format', default='sql', help=f'Format to write output/metadata in: `sql` or `pqt`')
@option('-t', '--cache-ttl', help='TTL for cache entries')
@option('-C', '--no-cache', is_flag=True, help=f'Disable writing results to {DEFAULT_ROOT}')
@option('-m', '--max-entries', type=int, help='Only store/render the -m/--max-entries largest directories/files found')
@option('-o', '--out-path', multiple=True, help='Paths to write output to. Supported extensions: {jpg, png, svg, html}')
@option('-p', '--s3-profile', help='AWS profile to use')
@argument('path')
def cli(path, cache_dir, cache_format, cache_ttl, no_cache, max_entries, out_path, s3_profile):

    if not cache_dir:
        cache_dir = DEFAULT_ROOT

    if cache_format == 'sql':
        cache_path = join(cache_dir, 'disk-tree.db')
    elif cache_format == 'pqt':
        cache_path = join(cache_dir, 'disk-tree.pqt')
    else:
        raise ValueError(f'Unrecognized output metadata format: {cache_format}')

    def merge(prev, cur):
        passthrough = prev[(prev.bucket != bucket)]
        if root:
            passthrough = concat([ passthrough, prev[(prev.bucket == bucket) & ~(prev.key.str.startswith(root))] ])
        merged = concat([ cur, passthrough ]).sort_values(['bucket', 'key'])
        num_passthrough = len(passthrough)
        num_overwritten = len(prev) - num_passthrough
        num_new = len(cur) - num_overwritten
        print(f"Overwriting {cache_dir}: {len(prev)} previous records, {num_passthrough} passing through, {num_overwritten} overwritten {num_new} new")
        return merged

    cache = None
    if not no_cache:
        base, ext = splitext(cache_path)
        if cache_format == 'sql':
            sqlite_url = f'sqlite:///{cache_path}'
            name = basename(base)
            if exists(cache_path):
                cache = pd.read_sql_table(name, sqlite_url)
                # aggd = merge(cache, aggd)
            # aggd.to_sql(name, sqlite_url, if_exists='replace')
        elif cache_format == 'pqt':
            if exists(cache_path):
                cache = pd.read_parquet(cache_path)
                # aggd = merge(cache, aggd)
            # aggd.to_parquet(cache_path)
        else:
            raise ValueError(f'Unrecognized output metadata format: {cache_format}')

    root, bucket, aggd = load(path, profile=s3_profile, cache=cache, cache_ttl=cache_ttl)

    aggd = aggd.sort_values('size', ascending=False)
    if max_entries:
        df = aggd.iloc[:max_entries]
    else:
        df = aggd

    files, dirs = df[df['type'] == 'file'], df[df['type'] == 'dir']
    total_size = files.size.sum()
    print(f'{len(files)} files in {len(dirs)} dirs, total size {naturalsize(total_size)}')

    out_paths = out_path
    if out_paths:
        k = 'key' if 'key' in df else 'path'
        plot = px.treemap(df, names=k, parents='parent', values='size')
        for out_path in out_paths:
            print(f'Writing: {out_path}')
            makedirs(dirname(abspath(out_path)), exist_ok=True)
            if out_path.endswith('.html'):
                plot.write_html(out_path)
            else:
                plot.write_image(out_path)
            check_call(['open', out_path])
    print(df)


if __name__ == '__main__':
    cli()
