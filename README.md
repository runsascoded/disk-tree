# disk-tree
Disk-space tree-maps and statistics

https://github.com/runsascoded/disk-tree/assets/465045/3d03e817-b72d-4ac3-993e-191c41927d0c

## Index
<!-- toc -->
- [Install](#install)
- [Examples](#examples)
    - [S3 bucket](#s3)
    - [Local directory](#local)
- [Notes](#notes)
    - [Caching](#caching)
    - [Performance](#performance)
    - [Max. entries](#max-entries)
    - [TUI-only mode](#tui-only)
<!-- /toc -->

## Install <a id="install"></a>
```bash
pip install disk-tree
disk-tree --help
# Usage: disk-tree [OPTIONS] [URL]
#
# Options:
#   -c, --color TEXT        Plotly treemap color configs: "name", "size",
#                           "size=<color-scale>" (cf.
#                           https://plotly.com/python/builtin-
#                           colorscales/#builtin-sequential-color-scales)
#   -C, --cache-path TEXT   Path to SQLite DB (or directory containing disk-
#                           tree.db) to use as cache; default:
#                           $HOME/.config/disk-tree/disk-tree.db
#   -f, --fsck              `file` scheme only: validate all cache entries that
#                           begin with the provided path(s); when passed twice,
#                           exit after performing fsck  [x>=0]
#   -m, --max-entries TEXT  Only store/render the -m/--max-entries largest
#                           directories/files found; default: "10k"
#   -M, --no-max-entries    Show all directories/files, ignore -m/--max-entries
#   -n, --sort-by-name      Sort output entries by name (default is by size)
#   -o, --out-path TEXT     Paths to write output to. Supported extensions:
#                           {jpg, png, svg, html}
#   -O, --no-open           Skip attempting to `open` any output files
#   -p, --profile TEXT      AWS_PROFILE to use
#   -s, --size-mode         Pass once for SI units, twice for raw sizes  [x>=0]
#   -t, --cache-ttl TEXT    TTL for cache entries; default: "1d"
#   -T, --tmp-html          Write an HTML representation to a temporary file and
#                           open in browser; pass twice to keep the temp file
#                           around after exit  [x>=0]
#   -x, --exclude TEXT      Exclude paths
#   --help                  Show this message and exit.
```

## Examples <a id="examples"></a>

### S3 bucket <a id="s3"></a>
Visualizing [`s3://ctbk`](https://ctbk.s3.amazonaws.com/index.html):
```bash
disk-tree -os3/ctbk.html s3://ctbk
# 2333 files in 138 dirs, total size 45.1G
# Writing: _s3/ctbk.html
#       4B	test.txt
#    66.1K	favicon.ico
#     3.9M	index.html
#     5.5M	static
#    11.2M	.dvc
#    95.0M	tmp
#   579.1M	stations
#     1.2G	aggregated
#     6.0G	normalized
#    37.2G	csvs
```

![](screenshots/disk-tree%20ctbk%20screenshot.png)

### Local directory <a id="local"></a>
Visualize a clone of this repo, color by size:
```bash
disk-tree -odisk-tree.html -csize
# 97 files in 47 dirs, total size 1.5M
# Writing: disk-tree.html
#       0B	disk-tree/__init__.py
#      77B	disk-tree/requirements.txt
#     867B	disk-tree/setup.py
#     2.3K	disk-tree/README.md
#    23.8K	disk-tree/disk_tree
#   291.8K	disk-tree/screenshots
#   580.4K	disk-tree/.git
#   628.6K	disk-tree/www
```

![](screenshots/disk-tree%20repo%20screenshot.png)
(default color scale is "RdBu"; see Plotly options [here][plotly color scales], `-csize=<scale>` to configure)

## Notes <a id="notes"></a>

### Caching <a id="caching"></a>
`disk-tree` caches file stats in a SQLite database, defaulting to `~/.config/disk-tree/disk-tree.db` and a `1d` TTL (see `-C`/`--cache-path` and `-t`/`--ttl`, resp.).

### Performance <a id="performance"></a>
`disk-tree` is reasonably performant on S3 buckets (it caches the result of `aws s3 ls --recursive s3://…`, and hydrates its cache from there), but ["local mode"](#local) is slow, as it stats every file and directory in a given tree, in a single-threaded tree-traversal.

### Max. entries <a id="max-entries"></a>
Plotly treemaps fall over with too many elements; `-m`/`--max-entries` (default `10k`) determines the maximum number of nodes (files and directories) to attempt to render.

### TUI-only mode <a id="tui-only"></a>
If you omit the `-o<path>.html` in [the examples above](#examples), `disk-tree` will simply print the sizes of all children of the specified URL, and exit.

[plotly color scales]: https://plotly.com/python/builtin-colorscales/#builtin-sequential-color-scales
