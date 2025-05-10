import sys
from os import makedirs, remove
from os.path import abspath, exists
from subprocess import check_call
from sys import stderr
from tempfile import NamedTemporaryFile

import pandas as pd
import plotly.express as px
from click import argument, option
from utz import basename, dirname, process
from utz.cli import number

from disk_tree.cli.base import cli
from disk_tree.config import SQLITE_PATH
from disk_tree.sqla.db import init


@cli.command('load')
@option('-c', '--color', help='Plotly treemap color configs: "name", "size", "size=<color-scale>" (cf. https://plotly.com/python/builtin-colorscales/#builtin-sequential-color-scales)')
@option('-C', '--cache-path', help=f'Path to SQLite DB (or directory containing disk-tree.db) to use as cache; default: {SQLITE_PATH}')
@option('-f', '--fsck', count=True, help='`file` scheme only: validate all cache entries that begin with the provided path(s); when passed twice, exit after performing fsck')
@number('-m', '--max-entries', default='10k', help='Only store/render the -m/--max-entries largest directories/files found; default: "10k"')
@option('-n', '--sort-by-name', is_flag=True, help='Sort output entries by name (default is by size)')
@option('-o', '--out-path', multiple=True, help='Paths to write output to. Supported extensions: {jpg, png, svg, html}')
@option('-O', '--no-open', is_flag=True, help='Skip attempting to `open` any output files')
@option('-p', '--profile', help='AWS_PROFILE to use')
@option('-s', '--size-mode', count=True, help='Pass once for SI units, twice for raw sizes')
@option('-t', '--cache-ttl', default='1d', help='TTL for cache entries; default: "1d"')
@option('-T', '--tmp-html', count=True, help='Write an HTML representation to a temporary file and open in browser; pass twice to keep the temp file around after exit')
@option('-x', '--exclude', 'excludes', multiple=True, help='Exclude paths')
@argument('url', required=False)
def load(
    color: str | None,
    cache_path: str | None,
    fsck: int,
    max_entries: int,
    sort_by_name: bool,
    out_path: tuple[str, ...],
    no_open: bool,
    profile: str | None,
    size_mode: int,
    cache_ttl: str,
    tmp_html: int,
    excludes: tuple[str, ...],
    url: str | None,
):
    """Index a directory, persisting data to a SQLite DB."""
    from disk_tree.config import ROOT_DIR
    db = init(cache_path)
    from disk_tree.sqla.cache import Cache
    db.create_all()

    cache = Cache(ttl=pd.to_timedelta(cache_ttl))

    if fsck:
        cache.fsck()
        missing_parents = cache.missing_parents()
        if not missing_parents.empty:
            stderr.write(f'{len(missing_parents)} missing parents:\n{missing_parents}\n')
            sys.exit(2)
    if fsck == 2:
        sys.exit(0)

    from .df import load_df
    df, k, size_col = load_df(
        url,
        cache,
        fsck=bool(fsck),
        max_entries=max_entries,
        sort_by_name=sort_by_name,
        profile=profile,
        size_mode=size_mode,
        excludes=excludes,
    )

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

