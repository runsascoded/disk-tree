# disk-tree
Disk-space tree-maps and statistics

```bash
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
#                           /Users/ryan/.config/disk-tree/disk-tree.db
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

## Examples

### S3 bucket
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

### Local directory
Visualize a clone of this repo:
```bash
disk-tree -odisk-tree.html
```
