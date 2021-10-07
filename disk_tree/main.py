from click import argument, command, option
from functools import partial
from humanize import naturalsize
from os import makedirs
from os.path import abspath
import pandas as pd
from pandas import to_datetime as to_dt
import plotly.express as px
from re import fullmatch
from subprocess import check_call

from utz import basename, concat, DF, dirname, dt, env, process, singleton, sxs, urlparse

from disk_tree.cache import Cache


LINE_RGX = '(?P<mtime>\d{4}-\d{2}-\d{2} \d\d:\d\d:\d\d) +(?P<size>\d+) (?P<key>.*)'


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


def load(path, cache, profile=None):
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
        aggd['checked_dt'] = now
        return root, bucket, aggd
    elif not url.scheme:
        # Local filesystem
        now = to_dt(dt.now())
        root = abspath(path)
        root = cache[root]
        entries = root.descendants
        df = DF([
            dict(
                path=e.path,
                kind=e.kind,
                size=e.size,
                mtime=e.mtime,
                num_descendants=e.num_descendants,
                parent=e.parent,
                checked_at=e.checked_at,
            )
            for e in entries
        ])
        return df.set_index('path')
    else:
        raise ValueError(f'Unsupported URL scheme: {url.scheme}')


@command('disk-tree')
@option('-h', '--human-readable', count=True, help='Pass once for SI units, twice for IEC')
@option('-t', '--cache-ttl', default='1d', help='TTL for cache entries')
@option('-m', '--max-entries', type=int, default=1000, help='Only store/render the -m/--max-entries largest directories/files found')
@option('-M', '--no-max-entries', is_flag=True, help='Only store/render the -m/--max-entries largest directories/files found')
@option('-o', '--out-path', multiple=True, help='Paths to write output to. Supported extensions: {jpg, png, svg, html}')
@option('-p', '--s3-profile', help='AWS profile to use')
@argument('path')
def cli(path, human_readable, cache_ttl, max_entries, no_max_entries, out_path, s3_profile):
    path = abspath(path)
    cache = Cache(ttl=pd.to_timedelta(cache_ttl))

    df = load(path, cache=cache, profile=s3_profile).reset_index()
    df['name'] = df.path.apply(basename)
    df = df.sort_values('size')

    if human_readable == 0:
        size_col = 'size'
    elif human_readable == 1:
        df['hsize'] = df['size'].apply(partial(naturalsize, gnu=True))
        size_col = 'hsize'
    elif human_readable == 2:
        df['hsize'] = df['size'].apply(partial(naturalsize, binary=True, gnu=True))
        size_col = 'hsize'
    else:
        raise ValueError(f'Pass -h/--human-readable 0, 1, or 2 times (got {human_readable})')

    if no_max_entries:
        max_entries = None

    if max_entries:
        df = df.iloc[-max_entries:]

    files, dirs = df[df.kind == 'file'], df[df.kind == 'dir']
    total_size = files['size'].sum()
    print(f'{len(files)} files in {len(dirs)} dirs, total size {naturalsize(total_size)}')

    out_paths = out_path
    if out_paths:
        k = 'key' if 'key' in df else 'path'
        plot = px.treemap(
            df,
            ids=k,
            names='name',
            # textinfo='name',
            parents='parent',
            values='size',
            #color_continuous_scale='RdBu',
            branchvalues='total',
            #color_continuous_midpoint=np.average(df['lifeExp'], weights=df['pop'])
        )
        for out_path in out_paths:
            print(f'Writing: {out_path}')
            makedirs(dirname(abspath(out_path)), exist_ok=True)
            if out_path.endswith('.html'):
                plot.write_html(out_path)
            else:
                plot.write_image(out_path)
            check_call(['open', out_path])

    children = df[df.parent == path]
    lines = children.apply(lambda r: '% 8s\t%s' % (r[size_col], r.path), axis=1, result_type='reduce').tolist()
    print('\n'.join(lines))


if __name__ == '__main__':
    cli()
