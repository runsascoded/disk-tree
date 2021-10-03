from click import argument, command, option
from humanize import naturalsize
import os
from os import makedirs, stat
from os.path import abspath, exists, isfile, join
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


def expand_file_row(r, root=None):
    path = r['path']
    file_df = r.to_frame().transpose()
    ancestors = dirs(path, root=root)[:-1]
    dirs_df = DF([ dict(mtime=r['mtime'], size=r['size'], path=dir, type='dir', parent=dirname(dir)) for dir in ancestors ])
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
    parents = groups.apply(lambda df: singleton(df['parent'])).rename('parent')
    num_descendents = groups.size().rename('num_descendents')
    aggd = sxs(mtimes, sizes, num_descendents, parents).reset_index()
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
        root = abspath(path)
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
@option('--db-dir', help=f'Location to store disk-tree metadata. Default: ${DISK_TREE_ROOT_VAR}, $HOME/.config/disk-tree, or $HOME/.disk-tree')
@option('--db-format', default='sql', help=f'Format to write output/metadata in: `sql` or `pqt`')
@option('-o', '--out-path', multiple=True, help='Format(s) to write output to: "jpg", "png", "svg"')
@option('-O', '--no-db', is_flag=True, help=f'Disable writing results to {DEFAULT_ROOT}')
@option('-p', '--s3-profile', help='AWS profile to use')
@argument('path')
def cli(path, db_format, out_path, db_dir, no_db, s3_profile):
    root, bucket, aggd = main(path, profile=s3_profile)

    if not db_dir:
        db_dir = DEFAULT_ROOT

    if db_format == 'sql':
        output_path = join(db_dir, 'disk-tree.db')
    elif db_format == 'pqt':
        output_path = join(db_dir, 'disk-tree.pqt')
    else:
        raise ValueError(f'Unrecognized output metadata format: {db_format}')

    if not no_db:

        def merge(prev, cur):
            passthrough = prev[(prev.bucket != bucket)]
            if root:
                passthrough = concat([ passthrough, prev[(prev.bucket == bucket) & ~(prev.key.str.startswith(root))] ])
            merged = concat([ cur, passthrough ]).sort_values(['bucket', 'key'])
            num_passthrough = len(passthrough)
            num_overwritten = len(prev) - num_passthrough
            num_new = len(cur) - num_overwritten
            print(f"Overwriting {db_dir}: {len(prev)} previous records, {num_passthrough} passing through, {num_overwritten} overwritten {num_new} new")
            return merged

        base, ext = splitext(output_path)
        if db_format == 'sql':
            sqlite_url = f'sqlite:///{output_path}'
            name = basename(base)
            if exists(output_path):
                prev = pd.read_sql_table(name, sqlite_url)
                aggd = merge(prev, aggd)
            aggd.to_sql(name, sqlite_url, if_exists='replace')
        elif db_format == 'pqt':
            if exists(output_path):
                prev = pd.read_parquet(output_path)
                aggd = merge(prev, aggd)
            aggd.to_parquet(output_path)
        else:
            raise ValueError(f'Unrecognized output metadata format: {db_format}')

    files, dirs = aggd[aggd['type'] == 'file'], aggd[aggd['type'] == 'dir']
    total_size = files.size.sum()
    print(f'{len(files)} files in {len(dirs)} dirs, total size {naturalsize(total_size)}')

    out_paths = out_path
    if out_paths:
        plot = px.treemap(aggd, names='path', parents='parent', values='size')
        for out_path in out_paths:
            print(f'Writing: {out_path}')
            makedirs(dirname(abspath(out_path)), exist_ok=True)
            if out_path.endswith('.html'):
                plot.write_html(out_path)
            else:
                plot.write_image(out_path)
            check_call(['open', out_path])
    print(aggd)


if __name__ == '__main__':
    cli()
