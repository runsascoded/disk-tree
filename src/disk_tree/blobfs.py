"""URL-aware blob IO — the one seam between a local scans dir and a remote one.

A scans dir on the search path (`config.scan_read_dirs()`) may be a plain
directory *or* an fsspec URL (`r2://bucket/prefix`, `s3://…`, `gs://…`,
`file://…`; `memory://…` in tests). `Scan.blob` stays a basename either way;
these helpers take the resolved path and dispatch on `is_url`, so the storage
backends and the diff/index readers never branch on it themselves.

`fsspec` (and the per-scheme driver) is an optional extra — every import here
is lazy, so a local-only install never pays for it, and a remote entry on the
search path fails with a pointed message instead of an ImportError deep in
pandas. `r2://` is not a registered fsspec protocol: it rides `s3fs` with the
bucket's Cloudflare endpoint, resolved by `r2_endpoint` (env, then the same
per-bucket `endpoint_url` that `buckets.yml` already carries for `sync`).

Remote `exists` hits are remembered (`_known`): blobs are immutable UUID-named
files, so a positive answer stays true until this module removes the blob.
Writes seed it; nothing here caches a miss.
"""
from __future__ import annotations

import os
from datetime import datetime
from functools import lru_cache
from os.path import exists as _local_exists, getmtime, getsize, join as _local_join
from typing import TYPE_CHECKING
from urllib.parse import urlparse

if TYPE_CHECKING:
    import pandas as pd
    import pyarrow as pa

R2_ENDPOINT_VAR = 'DISK_TREE_R2_ENDPOINT_URL'

#: Remote paths known to exist (see module docstring).
_known: set[str] = set()


def is_url(path: str) -> bool:
    return '://' in path


def join(d: str, name: str) -> str:
    """`os.path.join` for a local dir; a textual join for a URL (no normalization)."""
    return f"{d.rstrip('/')}/{name}" if is_url(d) else _local_join(d, name)


def _fsspec():
    try:
        import fsspec
    except ImportError:
        raise RuntimeError(
            "remote scans dirs need the `fsspec` extra — install `disk-tree[r2]` (or `[s3]` / `[gcs]`)"
        ) from None
    return fsspec


def r2_endpoint(bucket: str) -> str | None:
    """Cloudflare R2's S3 endpoint for `bucket`: `DISK_TREE_R2_ENDPOINT_URL`, else
    the bucket's (or `defaults`) `endpoint_url` in `~/.config/disk-tree/buckets.yml`."""
    ep = os.environ.get(R2_ENDPOINT_VAR)
    if ep:
        return ep
    from . import config as _config
    cfg_path = _local_join(_config.ROOT_DIR, 'buckets.yml')
    if not _local_exists(cfg_path):
        return None
    import yaml
    with open(cfg_path) as f:
        raw = yaml.safe_load(f) or {}
    for e in raw.get('buckets') or []:
        if isinstance(e, dict) and e.get('endpoint_url') and urlparse(e.get('uri', '')).netloc == bucket:
            return e['endpoint_url']
    return (raw.get('defaults') or {}).get('endpoint_url')


@lru_cache(maxsize=None)
def _s3fs(endpoint_url: str):
    import s3fs
    return s3fs.S3FileSystem(client_kwargs={'endpoint_url': endpoint_url})


def fs_for(url: str):
    """`(fs, path)` for a URL — the filesystem plus the path *within* it."""
    p = urlparse(url)
    if p.scheme == 'r2':
        ep = r2_endpoint(p.netloc)
        if not ep:
            raise RuntimeError(
                f"r2://{p.netloc}: no endpoint — set {R2_ENDPOINT_VAR}, or give the bucket an "
                "`endpoint_url` in buckets.yml"
            )
        return _s3fs(ep), f"{p.netloc}{p.path}"
    return _fsspec().core.url_to_fs(url)


def _ensure_parent(fs, p: str) -> None:
    """Object stores have no directories, but the local driver does — and pyarrow
    swaps fsspec's `LocalFileSystem` for its native one on write, so the
    driver's own `auto_mkdir` never gets a say. Make the parent explicitly."""
    from fsspec.implementations.local import LocalFileSystem
    if isinstance(fs, LocalFileSystem):
        fs.makedirs(p.rsplit('/', 1)[0], exist_ok=True)


def exists(path: str) -> bool:
    if not is_url(path):
        return _local_exists(path)
    if path in _known:
        return True
    fs, p = fs_for(path)
    if fs.exists(p):
        _known.add(path)
        return True
    return False


def _info(path: str) -> dict:
    fs, p = fs_for(path)
    return fs.info(p)


def size(path: str) -> int:
    return getsize(path) if not is_url(path) else int(_info(path)['size'])


def mtime(path: str) -> float:
    """Modification time as an epoch float — `getmtime` locally; whatever the
    remote driver reports (`mtime` / `LastModified` / `created`, float or datetime)."""
    if not is_url(path):
        return getmtime(path)
    info = _info(path)
    v = info.get('mtime') or info.get('LastModified') or info.get('created')
    if isinstance(v, datetime):
        return v.timestamp()
    return float(v) if v is not None else 0.0


def read_schema(path: str):
    import pyarrow.parquet as pq
    if not is_url(path):
        return pq.read_schema(path)
    fs, p = fs_for(path)
    return pq.read_schema(p, filesystem=fs)


def read_table(path: str, columns: list[str] | None = None) -> pa.Table:
    import pyarrow.parquet as pq
    if not is_url(path):
        return pq.read_table(path, columns=columns)
    fs, p = fs_for(path)
    return pq.read_table(p, columns=columns, filesystem=fs)


def read_parquet(path: str, filters=None, columns: list[str] | None = None) -> pd.DataFrame:
    """`pd.read_parquet`, URL-aware. Predicate pushdown works the same remotely:
    row-group min/max stats are read from the footer and only overlapping groups
    are fetched (range GETs)."""
    import pandas as pd
    if not is_url(path):
        return pd.read_parquet(path, filters=filters, columns=columns)
    fs, p = fs_for(path)
    return pd.read_parquet(p, filesystem=fs, filters=filters, columns=columns)


def write_table(table: pa.Table, path: str, row_group_size: int | None = None) -> None:
    import pyarrow.parquet as pq
    kw = {'row_group_size': row_group_size} if row_group_size else {}
    if not is_url(path):
        pq.write_table(table, path, **kw)
        return
    fs, p = fs_for(path)
    _ensure_parent(fs, p)
    pq.write_table(table, p, filesystem=fs, **kw)
    _known.add(path)


def write_parquet(df: pd.DataFrame, path: str, row_group_size: int) -> None:
    """`df.to_parquet(path, index=False, row_group_size=…)`, URL-aware."""
    if not is_url(path):
        df.to_parquet(path, index=False, row_group_size=row_group_size)
        return
    import pyarrow as pa
    write_table(pa.Table.from_pandas(df, preserve_index=False), path, row_group_size)


def read_text(path: str) -> str:
    if not is_url(path):
        with open(path) as f:
            return f.read()
    fs, p = fs_for(path)
    return fs.cat(p).decode()


def write_text(path: str, text: str) -> None:
    if not is_url(path):
        with open(path, 'w') as f:
            f.write(text)
        return
    fs, p = fs_for(path)
    _ensure_parent(fs, p)
    fs.pipe(p, text.encode())
    _known.add(path)


def remove(path: str) -> None:
    if not is_url(path):
        os.remove(path)
        return
    fs, p = fs_for(path)
    fs.rm(p)
    _known.discard(path)


def put(local_path: str, path: str) -> None:
    """Upload a local file to a URL path (an `adopt_parquet` into a remote dir)."""
    fs, p = fs_for(path)
    _ensure_parent(fs, p)
    fs.put(local_path, p)
    _known.add(path)


def list_parquets(d: str) -> list[str]:
    """Basenames of the `*.parquet` blobs in a scans dir."""
    if not is_url(d):
        from glob import glob
        return sorted(os.path.basename(p) for p in glob(_local_join(d, '*.parquet')))
    fs, p = fs_for(d)
    return sorted(x.rsplit('/', 1)[-1] for x in fs.glob(f"{p.rstrip('/')}/*.parquet"))
