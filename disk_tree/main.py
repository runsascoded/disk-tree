from typing import Optional

from click import argument, command, option
from functools import partial
from humanize import naturalsize
from os import getcwd, makedirs, remove
from os.path import abspath, exists
import pandas as pd
import plotly.express as px
from re import fullmatch
from subprocess import check_call
import sys
from sys import stderr
from tempfile import NamedTemporaryFile

from utz import basename, concat, DF, dirname, dt, env, process, singleton, sxs, urlparse, err

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


def load(url: str, cache: 'Cache', profile: str = None, fsck: bool = False, excludes: Optional[list[str]] = None):
    parsed = urlparse(url)
    if not parsed.scheme or parsed.scheme == 'file':
        # Local filesystem
        # now = to_dt(dt.now())
        root = abspath(url)
        if excludes:
            excludes = [ abspath(exclude) for exclude in excludes ]
            print(f'excludes: {excludes}')
        root = cache.compute(root, fsck=True, excludes=excludes)
        entries = root.descendants
        keys = [ 'path', 'kind', 'size', 'mtime', 'num_descendants', 'parent', 'checked_at', ]
        df = DF([
            { k: getattr(e, k) for k in keys }
            for e in entries
        ])
        return df.set_index('path')
    elif parsed.scheme == 's3':
        if profile:
            env['AWS_PROFILE'] = profile
        bucket = parsed.netloc
        root_key = parsed.path
        if root_key[0] == '/':
            root_key = root_key[1:]
        root = cache.compute_s3(url=url, bucket=bucket, root_key=root_key)
        entries = root.descendants
        keys = [ 'bucket', 'key', 'kind', 'size', 'mtime', 'num_descendants', 'parent', 'checked_at', ]
        df = DF([
            { k: getattr(e, k) for k in keys }
            for e in entries
        ])
        return df.set_index(['bucket', 'key'])
    else:
        raise ValueError(f'Unsupported URL scheme: {parsed.scheme}')


@command('disk-tree')
@option('-C', '--cache-path', help=f'Path to SQLite DB (or directory containing disk-tree.db) to use as cache; default: {SQLITE_PATH}')
@option('-f', '--fsck', count=True, help='Validate all cache entries that begin with the provided path(s); when passed twice, exit after performing fsck')
@option('-h', '--human-readable', count=True, help='Pass once for SI units, twice for IEC')
@option('-H', '--tmp-html', count=True, help='Write an HTML representation to a temporary file and open in browser; pass twice to keep the temp file around after exit')
@option('-m', '--max-entries', default='10k', help='Only store/render the -m/--max-entries largest directories/files found; default: "10k"')
@option('-M', '--no-max-entries', is_flag=True, help='Show all directories/files, ignore -m/--max-entries')
@option('-n', '--sort-by-name', is_flag=True, help='Sort output entries by name (default is by size)')
@option('-o', '--out-path', multiple=True, help='Paths to write output to. Supported extensions: {jpg, png, svg, html}')
@option('-O', '--no-open', is_flag=True, help='Skip attempting to `open` any output files')
@option('-p', '--profile', help='AWS_PROFILE to use')
@option('-t', '--cache-ttl', default='1d', help='TTL for cache entries; default: "1d"')
@option('-x', '--exclude', 'excludes', multiple=True, help='Exclude paths')
@argument('url', required=False)
def cli(url, cache_path, fsck, human_readable, tmp_html, max_entries, no_max_entries, sort_by_name, out_path, no_open, profile, cache_ttl, excludes):
    from disk_tree.config import ROOT_DIR
    db = init(cache_path)
    from .model import File, S3
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

    df = load(url, cache=cache, fsck=fsck, profile=profile, excludes=excludes).reset_index()
    k = 'path' if 'path' in df else 'key'
    df['name'] = df[k].apply(basename)

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
    err(f'{len(files)} files in {len(dirs)} dirs, total size {naturalsize(total_size)}')

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

    out_paths = out_path
    if not out_paths and tmp_html:
        name = basename(url)
        tmp_html_path = NamedTemporaryFile(dir=ROOT_DIR, prefix=f'.disk-tree_{name}', suffix='.html').name
        out_paths = [ tmp_html_path ]
    if out_paths:
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
        if not no_open:
            if not process.check('which', 'open'):
                stderr.write("No `open` executable found; skipping opening output files")
                no_open = True
        for out_path in out_paths:
            print(f'Writing: {out_path}')
            makedirs(dirname(abspath(out_path)), exist_ok=True)

            if out_path.endswith('.html'):
                plot.write_html(out_path, full_html=False, include_plotlyjs='cdn')
            else:
                plot.write_image(out_path)

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
