from os import getcwd, makedirs, remove
from os.path import abspath, exists

import pandas as pd
import plotly.express as px
import sys
from click import argument, command, option
from functools import partial
from humanize import naturalsize
from re import fullmatch
from subprocess import check_call
from sys import stderr
from tempfile import NamedTemporaryFile
from typing import Optional
from urllib.parse import ParseResult
from utz import basename, concat, DF, dirname, env, process, singleton, sxs, urlparse, err

from disk_tree.config import SQLITE_PATH
from disk_tree.db import init

LINE_RGX = r'(?P<mtime>\d{4}-\d{2}-\d{2} \d\d:\d\d:\d\d) +(?P<size>\d+) (?P<key>.*)'


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
    groups = expanded.groupby([ k, 'type' ])
    if len(groups) == len(files):
        return files
    sizes = groups['size'].sum()
    mtimes = groups['mtime'].max()
    parents = groups.apply(lambda df: singleton(df['parent'])).rename('parent')
    num_descendants = groups.size().rename('num_descendants')
    aggd = sxs(mtimes, sizes, num_descendants, parents).reset_index()
    return aggd


def load_file(url: str, cache: 'Cache', fsck: bool = False, excludes: Optional[list[str]] = None):
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


def make_s3_url(r):
    return f's3://{r.bucket}/{r.key}' if r.key else f's3://{r.bucket}'


def load_s3(url: str, parsed: ParseResult, cache: 'Cache', profile: str = None, excludes: Optional[list[str]] = None):
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


@command('disk-tree')
@option('-c', '--color', help='Plotly treemap color configs: "name", "size", "size=<color-scale>" (cf. https://plotly.com/python/builtin-colorscales/#builtin-sequential-color-scales)')
@option('-C', '--cache-path', help=f'Path to SQLite DB (or directory containing disk-tree.db) to use as cache; default: {SQLITE_PATH}')
@option('-f', '--fsck', count=True, help='`file` scheme only: validate all cache entries that begin with the provided path(s); when passed twice, exit after performing fsck')
@option('-m', '--max-entries', default='10k', help='Only store/render the -m/--max-entries largest directories/files found; default: "10k"')
@option('-M', '--no-max-entries', is_flag=True, help='Show all directories/files, ignore -m/--max-entries')
@option('-n', '--sort-by-name', is_flag=True, help='Sort output entries by name (default is by size)')
@option('-o', '--out-path', multiple=True, help='Paths to write output to. Supported extensions: {jpg, png, svg, html}')
@option('-O', '--no-open', is_flag=True, help='Skip attempting to `open` any output files')
@option('-p', '--profile', help='AWS_PROFILE to use')
@option('-s', '--size-mode', count=True, help='Pass once for SI units, twice for raw sizes')
@option('-t', '--cache-ttl', default='1d', help='TTL for cache entries; default: "1d"')
@option('-T', '--tmp-html', count=True, help='Write an HTML representation to a temporary file and open in browser; pass twice to keep the temp file around after exit')
@option('-x', '--exclude', 'excludes', multiple=True, help='Exclude paths')
@argument('url', required=False)
def cli(url, color, cache_path, fsck, max_entries, no_max_entries, sort_by_name, out_path, no_open, profile, size_mode, cache_ttl, tmp_html, excludes):
    from disk_tree.config import ROOT_DIR
    db = init(cache_path)
    db.create_all()

    from disk_tree.cache import Cache

    cache = Cache(ttl=pd.to_timedelta(cache_ttl))

    if fsck:
        cache.fsck()
        missing_parents = cache.missing_parents()
        if not missing_parents.empty:
            stderr.write(f'{len(missing_parents)} missing parents:\n{missing_parents}\n')
            sys.exit(2)
    if fsck == 2:
        sys.exit(0)

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

    rgx = r'(?P<base>[\d\.]+)(?P<suffix>[kmb])'
    m = fullmatch(rgx, max_entries.lower())
    if m:
        n = float(m['base'])
        suffix = m['suffix']
        order = {'k': 1e3, 'm': 1e6, 'b': 1e9 }[suffix]
        max_entries = int(n * order)
    else:
        max_entries = int(float(max_entries))

    if no_max_entries or max_entries == 0:
        max_entries = None

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

    out_paths = out_path
    if not out_paths and tmp_html:
        name = basename(url)
        tmp_html_path = NamedTemporaryFile(dir=ROOT_DIR, prefix=f'.disk-tree_{name}', suffix='.html').name
        out_paths = [ tmp_html_path ]
    if out_paths:
        color_kwargs = dict()
        if color == 'name':
            color_kwargs = dict(color='name')
        elif color:
            pcs = color.split('=', 1)
            if len(pcs) == 2:
                color, color_continuous_scale = pcs
                color_kwargs = dict(
                    color=color,
                    color_continuous_scale=color_continuous_scale,
                )
            else:
                color_continuous_scale = 'RdBu'
                color_kwargs = dict(color=color, color_continuous_scale=color_continuous_scale)

        fig = px.treemap(
            df,
            ids=k,
            names='label',
            # textinfo='name',
            parents='parent',
            values='size',
            # color_continuous_scale='RdBu',
            branchvalues='total',
            hover_data=[ 'mtime', 'size', 'parent', ],
            # color='name',
            **color_kwargs,
            # color_continuous_midpoint=np.average(df['lifeExp'], weights=df['pop'])
        )
        fig.update_traces(
            hovertemplate='<br>'.join([
                '%{label}',
                'mtime: %{customdata[0]}',
                'size: %{customdata[1]}',
                'parent: %{customdata[2]}',
            ])
        )
        fig.update_layout(coloraxis_colorbar=dict(
            title="Size",
            exponentformat='SI',
            # thicknessmode="pixels", thickness=50,
            # lenmode="pixels", len=200,
            # yanchor="top", y=1,
            # ticks="outside", ticksuffix=" bills",
            #dtick=5,
        ))
        if not no_open:
            if not process.check('which', 'open', log=None):
                stderr.write("No `open` executable found; skipping opening output files")
                no_open = True
        for out_path in out_paths:
            print(f'Writing: {out_path}')
            makedirs(dirname(abspath(out_path)), exist_ok=True)

            if out_path.endswith('.html'):
                fig.write_html(out_path, full_html=False, include_plotlyjs='cdn')
            else:
                fig.write_image(out_path)

            if not no_open:
                check_call(['open', out_path])

    if tmp_html == 1 and exists(tmp_html_path):
        print(f'Removing temp HTML file {tmp_html_path}')
        remove(tmp_html_path)

    children = df[df.parent == url]
    lines = children.apply(lambda r: '% 8s\t%s' % (r[size_col], r[k]), axis=1, result_type='reduce').tolist()
    print('\n'.join(lines))


if __name__ == '__main__':
    cli()
