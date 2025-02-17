from functools import partial
from os import getcwd
from os.path import abspath
from typing import Optional, Sequence
from urllib.parse import ParseResult

from humanize import naturalsize
from pandas import Series, DataFrame
from utz import basename, concat, DF, dirname, env, singleton, sxs, urlparse, err

from disk_tree.df import flatmap
from disk_tree.sql.cache import Cache

LINE_RGX = r'(?P<mtime>\d{4}-\d{2}-\d{2} \d\d:\d\d:\d\d) +(?P<size>\d+) (?P<key>.*)'


def dirs(file: str, root: str | None = None):
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


def expand_file_row(
    r: Series,
    k: str,
    root: str | None = None,
) -> DataFrame:
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


def agg_dirs(
    files: DataFrame,
    k: str = 'path',
    root: str | None = None,
):
    files['type'] = 'file'
    expanded = flatmap(files, expand_file_row, k=k, root=root)
    groups = expanded.groupby([ k, 'type' ])
    if len(groups) == len(files):
        return files
    sizes = groups['size'].sum()
    mtimes = groups['mtime'].max()
    parents = groups.apply(lambda df: singleton(df['parent'])).rename('parent')
    num_descendants = groups.size().rename('num_descendants')
    aggd = sxs(mtimes, sizes, num_descendants, parents).reset_index()
    return aggd


def load_file(
    url: str,
    cache: Cache,
    fsck: bool = False,
    excludes: Sequence[str] | None = None,
):
    # Local filesystem
    root = url
    if excludes:
        excludes = [ abspath(exclude) for exclude in excludes ]
        print(f'excludes: {excludes}')
    root = cache.compute_file(root, fsck=fsck, excludes=excludes)
    entries = root.descendants(excludes=excludes)
    keys = [ 'path', 'kind', 'size', 'mtime', 'num_descendants', 'parent', 'checked_at', ]
    df = DF([
        { k: getattr(e, k) for k in keys }
        for e in entries
    ])
    return df.set_index('path')


def make_s3_url(r: Series):
    return f's3://{r.bucket}/{r.key}' if r.key else f's3://{r.bucket}'


def load_s3(
    url: str,
    parsed: ParseResult,
    cache: Cache,
    profile: str = None,
    excludes: Optional[list[str]] = None,
):
    if profile:
        env['AWS_PROFILE'] = profile
    bucket = parsed.netloc
    root_key = parsed.path
    if root_key and root_key[0] == '/':
        root_key = root_key[1:]
    root = cache.compute_s3(url=url, bucket=bucket, root_key=root_key)
    entries = root.descendants(excludes=excludes)
    keys = [ 'bucket', 'key', 'kind', 'size', 'mtime', 'num_descendants', 'parent', 'checked_at', ]
    df = DF([
        { k: getattr(e, k) for k in keys }
        for e in entries
    ])
    df['url'] = df.apply(make_s3_url, axis=1)
    return df.set_index(['bucket', 'key'])


def load_df(
    url: str | None,
    cache: Cache,
    fsck: bool = False,
    max_entries: int | None = None,
    sort_by_name: bool = False,
    profile: str | None = None,
    size_mode: int = 0,
    excludes: Sequence[str] | None = None,
) -> tuple[DataFrame, str, str]:
    if url is None:
        url = getcwd()
    if url.endswith('/'):
        url = url[:-1]

    parsed = urlparse(url)
    if not parsed.scheme or parsed.scheme == 'file':
        url = abspath(url)
        df = load_file(url, cache=cache, fsck=fsck, excludes=excludes).reset_index()
    elif parsed.scheme == 's3':
        df = load_s3(url, parsed=parsed, cache=cache, profile=profile, excludes=excludes).reset_index()
    else:
        raise ValueError(f'Unsupported URL scheme: {parsed.scheme}')

    k = 'path' if 'path' in df else 'key'
    df['name'] = df[k].apply(basename)

    if size_mode == 0:
        size_col = 'hsize'
        size2str = partial(naturalsize, gnu=True)
        df['hsize'] = df['size'].apply(size2str)
    elif size_mode == 1:
        size_col = 'hsize'
        size2str = partial(naturalsize)
        df['hsize'] = df['size'].apply(size2str)
    elif size_mode == 2:
        size2str = lambda x: x
        size_col = 'size'
    else:
        raise ValueError(f'Pass -h/--human-readable 0, 1, or 2 times (got {size_mode})')

    files, dirs = df[df.kind == 'file'], df[df.kind == 'dir']
    total_size = files['size'].sum()
    total_size_str = size2str(total_size)
    err(f'{len(files)} files in {len(dirs)} dirs, total size {total_size_str}')

    if max_entries and len(df) > max_entries:
        err(f'Reducing to top entries ({len(df)} → {max_entries})')
        df = df.sort_values('size')
        df = df.iloc[-max_entries:]
        if sort_by_name:
            df = df.sort_values('name')
    else:
        if sort_by_name:
            df = df.sort_values('name')
        else:
            df = df.sort_values('size')

    if 'key' in df:
        df.loc[df['key'] == '', 'key'] = url
        df.loc[df['parent'] == '', 'parent'] = url
        is_root = df['url'] == url
        df.loc[is_root, ('parent', 'name')] = ('', url)
    else:
        df.loc[df['path'] == url, 'parent'] = ''

    df['label'] = df.apply(lambda r: f'{r["name"]}: {r[size_col]}', axis=1)
    # df.loc[df.parent == url, 'parent'] = f'{url}: {total_size_str}'
    return df, k, size_col
