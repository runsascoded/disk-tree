"""Layer-2 listing format versions (spec `listing-slim.md`).

A layer-2 listing (the scan blob `import -e duckdb|stream` writes, and the
tiers cut from it) comes in two shapes:

- **v1** (no marker): every row carries `uri` (`<scan_root>/<path>`, the
  scan root itself at `.`), and `--pivot-sum` writes one `sum_<col>_<v>`
  column per value present. Snappy.
- **v2** (`disk_tree.listing_format = '2'` in the parquet key-value
  metadata): no `uri` column — the scan root is in the metadata and `uri` is
  derived at read time. A pivot column that equals `size` on every row (the
  pivot column held exactly one value and no NULLs, e.g. a single-storage-class
  bucket) is not written; the metadata's `implied` map names it and the column
  it equals, so readers restore it exactly. zstd (level `ZSTD_LEVEL`).

Readers see the v1 shape through :func:`restore` (`blobfs.read_parquet` does
this for every blob read), so nothing downstream needs to know which one it
got. `columns` records the v1 column order so the restored frame is
column-for-column the frame a v1 writer would have produced.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pandas as pd

FORMAT_KEY = 'disk_tree.listing_format'
SCAN_ROOT_KEY = 'disk_tree.scan_root'
IMPLIED_KEY = 'disk_tree.implied'
COLUMNS_KEY = 'disk_tree.columns'

LISTING_FORMAT = 2
COMPRESSION = 'zstd'
ZSTD_LEVEL = 3


@dataclass(frozen=True)
class ListingFormat:
    version: int = 1
    scan_root: str | None = None
    #: dropped column → the column it equals on every row
    implied: dict[str, str] = field(default_factory=dict)
    #: the v1 column order (None: unknown / v1 file)
    columns: tuple[str, ...] | None = None

    def kv(self) -> dict[str, str]:
        """This format's parquet key-value metadata."""
        if self.version < 2:
            return {}
        if self.scan_root is None:
            raise ValueError("a v2 listing needs its scan root")
        kv = {FORMAT_KEY: str(self.version), SCAN_ROOT_KEY: self.scan_root}
        if self.implied:
            kv[IMPLIED_KEY] = json.dumps(self.implied, sort_keys=True, separators=(',', ':'))
        if self.columns is not None:
            kv[COLUMNS_KEY] = json.dumps(list(self.columns), separators=(',', ':'))
        return kv


V1 = ListingFormat()


def slim(scan_root: str, v1_columns: list[str], implied: dict[str, str] | None = None) -> ListingFormat:
    """The v2 format for a listing whose v1 columns would be `v1_columns`."""
    return ListingFormat(LISTING_FORMAT, scan_root, dict(implied or {}), tuple(v1_columns))


def written_columns(fmt: ListingFormat) -> list[str]:
    """The columns a writer of `fmt` emits (v1 order minus `uri` and the implied ones)."""
    if fmt.columns is None:
        raise ValueError("format has no column list")
    if fmt.version < 2:
        return list(fmt.columns)
    return [c for c in fmt.columns if c != 'uri' and c not in fmt.implied]


def parse(metadata: dict | None) -> ListingFormat:
    """The format from a parquet/Arrow schema's key-value metadata (bytes or str keys)."""
    if not metadata:
        return V1
    md = {(k.decode() if isinstance(k, bytes) else k): (v.decode() if isinstance(v, bytes) else v) for k, v in metadata.items()}
    if FORMAT_KEY not in md:
        return V1
    version = int(md[FORMAT_KEY])
    if version > LISTING_FORMAT:
        raise ValueError(f"listing format {version} is newer than this reader ({LISTING_FORMAT})")
    return ListingFormat(
        version=version,
        scan_root=md.get(SCAN_ROOT_KEY),
        implied=json.loads(md[IMPLIED_KEY]) if IMPLIED_KEY in md else {},
        columns=tuple(json.loads(md[COLUMNS_KEY])) if COLUMNS_KEY in md else None,
    )


def format_of(path: str) -> ListingFormat:
    """The format of the parquet at `path` (local or URL) — a footer read."""
    from . import blobfs
    return parse(blobfs.read_schema(path).metadata)


def uri_sql(scan_root: str, col: str = 'path') -> str:
    """DuckDB expression deriving a row's `uri` from its path (`.` = the root)."""
    root = scan_root.replace("'", "''")
    return f"CASE WHEN {col} = '.' THEN '{root}' ELSE '{root}/' || {col} END"


def restore(df: "pd.DataFrame", fmt: ListingFormat) -> "pd.DataFrame":
    """The v1 view of a frame read from a `fmt` listing: add `uri` and the implied
    columns (only those missing — a projection that left them out stays out when
    their source column is absent too), in the recorded v1 order. v1 frames pass
    through untouched."""
    if fmt.version < 2:
        return df
    add = {}
    if 'uri' not in df.columns and 'path' in df.columns:
        root = fmt.scan_root
        path = df['path']
        add['uri'] = (f'{root}/' + path).where(path != '.', root)
    for col, src in fmt.implied.items():
        if col not in df.columns and src in df.columns:
            add[col] = df[src]
    if not add:
        return df
    df = df.assign(**add)
    if fmt.columns is not None:
        order = [c for c in fmt.columns if c in df.columns]
        rest = [c for c in df.columns if c not in order]
        df = df[order + rest]
    return df


def duckdb_copy_options(fmt: ListingFormat, row_group_size: int | None = None, extra_kv: dict | None = None) -> str:
    """`COPY … TO` options writing `fmt` (zstd + its key-value metadata for v2)."""
    opts = ['FORMAT PARQUET']
    if fmt.version >= 2:
        opts += [f'COMPRESSION {COMPRESSION}', f'COMPRESSION_LEVEL {ZSTD_LEVEL}']
    if row_group_size:
        opts.append(f'ROW_GROUP_SIZE {row_group_size}')
    kv = {**fmt.kv(), **(extra_kv or {})}
    if kv:
        esc = lambda s: str(s).replace("'", "''")
        opts.append('KV_METADATA {' + ', '.join(f"'{esc(k)}': '{esc(v)}'" for k, v in kv.items()) + '}')
    return f"({', '.join(opts)})"


def pyarrow_write_kwargs(fmt: ListingFormat) -> dict:
    """`pq.ParquetWriter` / `write_table` kwargs for `fmt`'s compression."""
    if fmt.version >= 2:
        return {'compression': COMPRESSION, 'compression_level': ZSTD_LEVEL}
    return {}


def with_kv(schema, fmt: ListingFormat):
    """`schema` carrying `fmt`'s key-value metadata (existing keys kept)."""
    kv = fmt.kv()
    if not kv:
        return schema
    return schema.with_metadata({**(schema.metadata or {}), **{k.encode(): v.encode() for k, v in kv.items()}})
