# disk-tree

Cloud storage analyzer: a scanning/indexing CLI for S3 / R2 / GCS buckets, and a Cloudflare-hosted treemap site (`site/`) over its indexes.

[![disk-tree treemap of an R2 bucket](screenshots/treemap.png)](https://r2.rbw.sh/r2/ctbk)

<p align="center"><b><a href="https://r2.rbw.sh/r2/ctbk">▶ Live demo</a></b> — interactive treemap of a 916&nbsp;GB R2 bucket (drill in, filter, compare), from <a href="https://ctbk.dev">ctbk.dev</a></p>

<!-- toc -->
- [Install](#install)
- [Site](#site)
- [CLI](#cli)
  - [Examples](#examples)
- [Notes](#notes)
  - [Caching](#caching)
  - [Performance](#performance)
<!-- /toc -->

## Install

From a clone (one uv workspace: the engine plus the `dt-cloud` overlay in `cloud/`):

```bash
uv sync                                     # engine (`disk-tree` CLI)
uv sync --all-packages --all-extras         # + dt-cloud, every extra
```

## Site

`site/` is a Vite + React app with Cloudflare Pages Functions over a path index of each scan (in R2 / GCS, with D1 for the index footers): treemaps you can drill into, filter, diff between scans, and an age lens. Zero-dependency DIY-SVG/canvas ([`@rdub/treemap`](packages/treemap)), no chart lib. Try it on the [live `r2://ctbk` demo](https://r2.rbw.sh/r2/ctbk).

## CLI

```bash
disk-tree index --help
# Usage: disk-tree index [OPTIONS] URL
#
#   Index an `s3://` or `r2://` URL, persisting data to a SQLite DB.
#
# Options:
#   -C, --no-cache-read   Force fresh scan (ignore cache)
#   -g, --gc              Garbage collect old scans
#   -m, --mean-mtime      Emit `mtime_mean` (size-weighted mean mtime) per path
#   -M, --measure-memory  Track peak memory usage
#   -q, --no-progress     Suppress the scan progress bar
#   -t, --to TEXT         Write the scan's blob to a local dir or an fsspec URL
#   --help                Show this message and exit.
```

`gcs://` buckets and local paths have no live lister here: list a bucket with `disk-tree bulk-list`, then `disk-tree import -l <listing>`.

### Examples

Scan an S3 bucket:
```bash
disk-tree index s3://ctbk
# 2333 files in 138 dirs, total size 45.1G
#       4B  test.txt
#    66.1K  favicon.ico
#     3.9M  index.html
#     5.5M  static
#    11.2M  .dvc
#    95.0M  tmp
#   579.1M  stations
#     1.2G  aggregated
#     6.0G  normalized
#    37.2G  csvs
```

## Notes

### Caching

`disk-tree` caches scan results as Parquet files in `~/.config/disk-tree/scans/`, with metadata in `~/.config/disk-tree/disk-tree.db`. Override with `DISK_TREE_ROOT`.

### Performance

- **S3 / R2**: Caches `aws s3 ls --recursive` output; `bulk-list` shards a large bucket's listing across workers
- **Depth-based predicate pushdown**: Parquet queries filter by depth for fast loading

## Development

```bash
# Python
uv sync
pytest tests/

# Site
pnpm install
cd site
pnpm dev
```
